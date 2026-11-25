"""
Price Target Probability Engine
================================
Bookmap addon that:
1. Tracks sell limit orders in the range [driving_price, target_above]
2. Tracks buy limit orders in the range [target_below, driving_price]
3. Tracks cumulative buy/sell market order volume since start
4. Calculates directional probability for each target level
5. Serves a local web dashboard via HTTP + WebSocket
"""

import bookmap as bm
import threading
import json
import os
import datetime
import time
import math
import heapq
from collections import deque
from http.server import HTTPServer, SimpleHTTPRequestHandler
import asyncio

# ── Try importing websockets; if not installed, guide the user ──
try: 
    import websockets
except ImportError:
    print("ERROR: websockets library not found. Install with: pip install websockets", flush=True)
    raise

# ─────────────────────── CONFIG ───────────────────────
WS_PORT = 8768
HTTP_PORT = 8083
TREND_LOG_FILE = r"C:\Bookmap\Logs\trend_snapshots_time.log"
DASHBOARD_DIR = r"C:\Bookmap\Python\dashboard\static-time-phase"

# ─────────────────────── LOGGING ──────────────────────
def log_msg(msg):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    formatted = f"[{timestamp}] {msg}"
    print(formatted, flush=True)



# ══════════════════════════════════════════════════════
#   CORE ENGINE
# ══════════════════════════════════════════════════════
class PriceTargetEngine:
    def __init__(self):
        self.lock = threading.RLock()
        self.probability_lock = threading.RLock()
        self.phase_transition_lock = threading.RLock()
        self.market_lock = threading.RLock()
        self.snapshot_cache_lock = threading.Lock()
        self.active_alias = None

        # ── Configuration (controlled via dashboard) ──
        self.target_distance = 9           # Default 9 ticks
        self.num_levels = 1                # Default 1 level
        self.use_live_price = True         # Use live bid/ask midpoint by default
        self.user_driving_price = None     # Manual override price
        self.pips = 0.01                   # Tick size (updated on subscribe)

        # ── Live Data State ──
        self.best_bid = None
        self.best_ask = None
        self.last_trade_price = None

        # ── Market order volumes (cumulative) ──
        self.buy_market_volume = 0
        self.sell_market_volume = 0
        self.tracking_active = False
        self.tracking_start_time = None

        # ── Session Range Tracking ──
        self.session_anchor_price = None
        self.session_high_price = None
        self.session_low_price = None
        self.session_high_time = None
        self.session_low_time = None

        # ── Delta Liquidity Tracking ──
        self.delta_target_distance = 40  # Default 40 ticks
        self.initial_sell_limits = {}
        self.initial_buy_limits = {}

        # ── Order book depth snapshot (price -> size) ──
        self.bid_depth = {}   # price -> size
        self.ask_depth = {}   # price -> size
        self.bid_price_heap = []
        self.ask_price_heap = []
        self.bid_price_buckets = {}
        self.ask_price_buckets = {}
        self.memory_price_buckets = {}
        self.price_bucket_width = 8.0
        self.memory_cleanup_queue = deque()
        self.memory_cleanup_queued = set()
        self.memory_cleanup_budget = 512
        self.memory_cleanup_interval = 0.25
        self.last_memory_maintenance = 0.0
        self.last_decay_update = 0.0
        self.market_revision = 0
        self.depth_revision = 0
        self.book_revision = 0
        self.state_revision = 0
        self.snapshot_cache_key = None
        self.snapshot_levels_cache = []
        self.probability_cache_key = None

        self.lookahead_ticks = 10  # Look 10 ticks past the target
        self.last_up_weights = {}  # level_num -> total_weight
        self.last_down_weights = {}# level_num -> total_weight
        
        # ── Liquidity Decay (Anti-Spoofing) ──
        self.liquidity_memory = {}  # price -> {'size': last_max_size, 'time': last_seen_time, 'side': 'bid'/'ask'}
        self.dynamic_decay_period = 3.0  
        self.tick_history = deque()
        self.last_price_for_velocity = None
        
        # ── Trend Milestone Tracking ──
        self.recording_phase = 0     # 0: Standby, 1: Benchmark, 2: Sync1, 3: Sync2
        self.absolute_phase = 0      # Continuous phase counter
        self.milestone_target = 0    # Target volume from Phase 1
        self.current_benchmark_threshold = 100

        # ── Snapshot timer ──
        self.snapshot_interval = 0.25  # 250ms updates

        # ── WebSocket clients ──
        self.ws_clients = set()
        
        self.latest_probabilities = []
        threading.Thread(target=self.backbone_loop, daemon=True).start()

    def backbone_loop(self):
        """High-frequency internal loop (20Hz) to ensure all data is calculated independently of UI latency."""
        while True:
            try:
                with self.probability_lock:
                    if self.tracking_active:
                        with self.market_lock:
                            probability_key = (self.market_revision, self.state_revision)
                        if self.probability_cache_key != probability_key:
                            self.latest_probabilities = self.calculate_probabilities()
                            with self.market_lock:
                                if self.market_revision == probability_key[0]:
                                    self.probability_cache_key = probability_key
                self.check_auto_trend()
                self.maintain_liquidity_memory()
            except Exception as e:
                log_msg(f"Backbone loop error: {e}")
            time.sleep(0.05)

    # ── Settings update ──
    def update_settings(self, settings):
        with self.phase_transition_lock, self.lock, self.probability_lock:
            self.state_revision += 1
            if "driving_price" in settings:
                val = settings["driving_price"]
                if val is None or val == "live":
                    self.use_live_price = True
                    self.user_driving_price = None
                else:
                    self.use_live_price = False
                    self.user_driving_price = float(val)

            if "target_distance" in settings:
                self.target_distance = int(settings["target_distance"])
                self.delta_target_distance = self.target_distance

            if "num_levels" in settings:
                self.num_levels = max(1, int(settings["num_levels"]))
                
            if "volume_threshold" in settings:
                self.current_benchmark_threshold = max(5, int(settings["volume_threshold"]))

            if "action" in settings:
                action = settings["action"]
                if action == "start":
                    self.internal_start(is_auto=False)
                    log_msg("TRACKING STARTED — Phase 1 (Benchmark)")
                elif action == "stop":
                    with self.market_lock:
                        self.tracking_active = False
                    self.recording_phase = 0
                    log_msg("TRACKING STOPPED")
                elif action == "reset":
                    # Lock Milestone if Phase 1 is ending
                    if self.recording_phase == 1:
                        # Capture initial Time limit target (Phase 1)
                        self.milestone_target = self.current_benchmark_threshold
                        log_msg(f"MILESTONE LOCKED: {self.milestone_target}s (Time Limit)")
                    
                    # Advance Phase
                    if self.recording_phase < 3:
                        self.recording_phase += 1
                    
                    with self.market_lock:
                        self.buy_market_volume = 0
                        self.sell_market_volume = 0
                    self.tracking_start_time = time.time() if self.tracking_active else None
                    
                    # Reset High/Low for next wave, but KEEP the Anchor
                    if self.tracking_active:
                        dp = self.get_driving_price()
                        now_t = time.time()
                        self.session_high_price = dp
                        self.session_low_price = dp
                        self.session_high_time = now_t
                        self.session_low_time = now_t
                    
                    self.absolute_phase += 1
                    log_msg(f"RESET — Entering Phase {self.absolute_phase} (Internal {self.recording_phase})")

    # ── Get current driving price (in tick space) ──
    def get_driving_price(self):
        if not self.use_live_price and self.user_driving_price is not None:
            # User enters price in dollars (e.g. 97.48), convert to ticks
            if self.pips > 0:
                return round(self.user_driving_price / self.pips)
            return self.user_driving_price
        with self.market_lock:
            if self.best_bid is not None and self.best_ask is not None:
                return round((self.best_bid + self.best_ask) / 2)
            if self.last_trade_price is not None:
                return self.last_trade_price
        return None

    # ── Convert tick price to display price ──
    def tick_to_display(self, price):
        if price is None: return None
        if self.pips:
            return round(price * self.pips, 2)
        return price

    # ── Pure Delta Liquidity Range ──
    def get_raw_limits_in_range(self, start_price, end_price, is_ask):
        p_min = min(start_price, end_price)
        p_max = max(start_price, end_price)
        with self.market_lock:
            return self._sum_depth_range_locked(p_min, p_max, is_ask)

    def _bucket_id(self, price):
        return math.floor(price / self.price_bucket_width)

    def _add_indexed_price(self, buckets, price):
        bucket_id = self._bucket_id(price)
        bucket = buckets.get(bucket_id)
        if bucket is None:
            bucket = {}
            buckets[bucket_id] = bucket
        bucket[price] = None

    def _remove_indexed_price(self, buckets, price):
        bucket_id = self._bucket_id(price)
        bucket = buckets.get(bucket_id)
        if bucket is not None:
            bucket.pop(price, None)
            if not bucket:
                del buckets[bucket_id]

    def _sum_bucket_range_locked(self, buckets, depth, p_min, p_max):
        total = 0
        first_bucket = self._bucket_id(p_min)
        last_bucket = self._bucket_id(p_max)
        for bucket_id in range(first_bucket, last_bucket + 1):
            bucket = buckets.get(bucket_id)
            if bucket:
                for price in bucket:
                    if p_min <= price <= p_max:
                        total += depth[price]
        return total

    def _sum_depth_range_locked(self, p_min, p_max, is_ask):
        if is_ask:
            return self._sum_bucket_range_locked(
                self.ask_price_buckets, self.ask_depth, p_min, p_max
            )
        return self._sum_bucket_range_locked(
            self.bid_price_buckets, self.bid_depth, p_min, p_max
        )

    # ── Calculate visible liquidity and ghost flags for every target in one pass ──
    def get_target_liquidity(self, dp):
        count = self.num_levels
        up_visible = [0] * count
        down_visible = [0] * count
        up_ghosting = [False] * count
        down_ghosting = [False] * count
        now = time.time()
        with self.market_lock:
            decay_period = self.dynamic_decay_period
            up_high = dp + self.target_distance * count + self.lookahead_ticks
            down_low = dp - self.target_distance * count - self.lookahead_ticks

            first_bucket = self._bucket_id(dp)
            last_bucket = self._bucket_id(up_high)
            for bucket_id in range(first_bucket, last_bucket + 1):
                bucket = self.ask_price_buckets.get(bucket_id)
                if not bucket:
                    continue
                for price in bucket:
                    if not dp <= price <= up_high:
                        continue
                    actual_size = self.ask_depth[price]
                    memory = self.liquidity_memory.get(price)
                    decayed_size = actual_size
                    if memory:
                        elapsed = now - memory['time']
                        if elapsed < decay_period:
                            potential = memory['size'] * ((decay_period - elapsed) / decay_period)
                            decayed_size = max(actual_size, potential)
                    visible_size = max(actual_size, decayed_size)
                    ghosting = decayed_size > actual_size + 1
                    for index in range(count):
                        high = dp + self.target_distance * (index + 1) + self.lookahead_ticks
                        if price <= high:
                            up_visible[index] += visible_size
                            if ghosting:
                                up_ghosting[index] = True

            first_bucket = self._bucket_id(dp)
            last_bucket = self._bucket_id(up_high)
            for bucket_id in range(first_bucket, last_bucket + 1):
                bucket = self.memory_price_buckets.get(bucket_id)
                if not bucket:
                    continue
                for price in bucket:
                    if price in self.ask_depth or not dp <= price <= up_high:
                        continue
                    memory = self.liquidity_memory[price]
                    elapsed = now - memory['time']
                    if elapsed >= decay_period:
                        continue
                    visible_size = memory['size'] * ((decay_period - elapsed) / decay_period)
                    for index in range(count):
                        high = dp + self.target_distance * (index + 1) + self.lookahead_ticks
                        if price <= high:
                            up_visible[index] += visible_size
                            if visible_size > 1:
                                up_ghosting[index] = True

            first_bucket = self._bucket_id(down_low)
            last_bucket = self._bucket_id(dp)
            for bucket_id in range(first_bucket, last_bucket + 1):
                bucket = self.bid_price_buckets.get(bucket_id)
                if not bucket:
                    continue
                for price in bucket:
                    if not down_low <= price <= dp:
                        continue
                    actual_size = self.bid_depth[price]
                    memory = self.liquidity_memory.get(price)
                    decayed_size = actual_size
                    if memory:
                        elapsed = now - memory['time']
                        if elapsed < decay_period:
                            potential = memory['size'] * ((decay_period - elapsed) / decay_period)
                            decayed_size = max(actual_size, potential)
                    visible_size = max(actual_size, decayed_size)
                    ghosting = decayed_size > actual_size + 1
                    for index in range(count):
                        low = dp - self.target_distance * (index + 1) - self.lookahead_ticks
                        if price >= low:
                            down_visible[index] += visible_size
                            if ghosting:
                                down_ghosting[index] = True

            for bucket_id in range(first_bucket, last_bucket + 1):
                bucket = self.memory_price_buckets.get(bucket_id)
                if not bucket:
                    continue
                for price in bucket:
                    if price in self.bid_depth or not down_low <= price <= dp:
                        continue
                    memory = self.liquidity_memory[price]
                    elapsed = now - memory['time']
                    if elapsed >= decay_period:
                        continue
                    visible_size = memory['size'] * ((decay_period - elapsed) / decay_period)
                    for index in range(count):
                        low = dp - self.target_distance * (index + 1) - self.lookahead_ticks
                        if price >= low:
                            down_visible[index] += visible_size
                            if visible_size > 1:
                                down_ghosting[index] = True

        return up_visible, down_visible, up_ghosting, down_ghosting

    def maintain_liquidity_memory(self):
        now_monotonic = time.monotonic()
        update_decay = now_monotonic - self.last_decay_update >= 1.0
        cleanup_due = now_monotonic - self.last_memory_maintenance >= self.memory_cleanup_interval
        if not update_decay and not cleanup_due:
            return

        bid_prices = None
        ask_prices = None
        book_revision = None
        with self.market_lock:
            now = time.time()
            if update_decay:
                if self.update_decay_rate(now):
                    self.market_revision += 1
                self.last_decay_update = now_monotonic

            if cleanup_due:
                self.last_memory_maintenance = now_monotonic
                expiry = self.dynamic_decay_period * 2
                processed = min(self.memory_cleanup_budget, len(self.memory_cleanup_queue))
                for _ in range(processed):
                    price = self.memory_cleanup_queue.popleft()
                    self.memory_cleanup_queued.discard(price)
                    memory = self.liquidity_memory.get(price)
                    if memory is None:
                        continue
                    if now - memory['time'] > expiry:
                        del self.liquidity_memory[price]
                        self._remove_indexed_price(self.memory_price_buckets, price)
                        self.market_revision += 1
                    else:
                        self.memory_cleanup_queue.append(price)
                        self.memory_cleanup_queued.add(price)

                bid_limit = max(64, len(self.bid_depth) * 2)
                ask_limit = max(64, len(self.ask_depth) * 2)
                if len(self.bid_price_heap) > bid_limit or len(self.ask_price_heap) > ask_limit:
                    book_revision = self.book_revision
                    bid_prices = tuple(self.bid_depth)
                    ask_prices = tuple(self.ask_depth)

        if book_revision is not None:
            bid_heap = [-price for price in bid_prices]
            ask_heap = list(ask_prices)
            heapq.heapify(bid_heap)
            heapq.heapify(ask_heap)
            with self.market_lock:
                if self.book_revision == book_revision:
                    self.bid_price_heap = bid_heap
                    self.ask_price_heap = ask_heap

    def format_elapsed_static(self, seconds):
        if seconds is None: return "0s"
        try:
            sec = float(seconds)
            if sec < 60: return f"{int(sec)}s"
            m = int(sec // 60)
            s = int(sec % 60)
            return f"{m}m {s}s"
        except:
            return "0s"

    def log_milestone(self, snap):
        """Append one completed phase to the sole measurement log."""
        try:
            phase = snap.get("absolute_phase", 0)
            elapsed = snap.get("tracking_elapsed", 1) or 1
            if elapsed <= 0:
                elapsed = 1
            buy_volume = snap.get("buy_market_volume", 0)
            sell_volume = snap.get("sell_market_volume", 0)

            with open(TREND_LOG_FILE, "a", encoding="utf-8") as f:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                f.write(f"\n--- [TREND CAPTURED] {timestamp} ---\n")
                f.write(f"PHASE: {phase}\n")
                for level in snap.get("levels", []):
                    start_offset = level["start_offset"]
                    end_offset = level["end_offset"]
                    current_sell = level["current_sell_limits"]
                    current_buy = level["current_buy_limits"]
                    initial_sell = level["initial_sell_limits"]
                    initial_buy = level["initial_buy_limits"]
                    delta_sell = level["delta_sell"]
                    delta_buy = level["delta_buy"]
                    buy_reload = delta_sell + buy_volume
                    sell_reload = delta_buy + sell_volume
                    safe_buy_volume = buy_volume if buy_volume > 0 else 1
                    safe_sell_volume = sell_volume if sell_volume > 0 else 1
                    buy_efficiency = round((buy_reload / safe_buy_volume) * 100, 1)
                    sell_efficiency = round((sell_reload / safe_sell_volume) * 100, 1)
                    buy_delta_velocity = round(delta_sell / elapsed, 2)
                    sell_delta_velocity = round(delta_buy / elapsed, 2)
                    buy_market_velocity = round(buy_volume / elapsed, 2)
                    sell_market_velocity = round(sell_volume / elapsed, 2)

                    f.write(f"[LEVEL {level['level']}] - ({start_offset} to {end_offset} ticks offset)\n")
                    f.write(f"[BUY SIDE - {buy_volume}v / M Velocity= {buy_market_velocity}] reload power = {initial_sell} - {buy_volume} = {initial_sell - buy_volume} to positive {current_sell} = {buy_reload} load pressure\n")
                    f.write(f"Reload Efficiency: {buy_efficiency}%  |  Delta Velocity: {buy_delta_velocity} limits/sec  |  Mkt Velocity: {buy_market_velocity} vol/sec\n")
                    f.write(f"Initial Sell Limits (Resistance): {initial_sell}\n")
                    f.write(f"Current Sell Limits: {current_sell}\n")
                    f.write(f"Delta: {'+' if delta_sell > 0 else ''}{delta_sell}\n\n")

                    f.write(f"[SELL SIDE - {sell_volume}v / M Velocity= {sell_market_velocity}] reload power = {initial_buy} - {sell_volume} = {initial_buy - sell_volume} to positive {current_buy} = {sell_reload} load pressure\n")
                    f.write(f"Reload Efficiency: {sell_efficiency}%  |  Delta Velocity: {sell_delta_velocity} limits/sec  |  Mkt Velocity: {sell_market_velocity} vol/sec\n")
                    f.write(f"Initial Buy Limits (Support): {initial_buy}\n")
                    f.write(f"Current Buy Limits: {current_buy}\n")
                    f.write(f"Delta: {'+' if delta_buy > 0 else ''}{delta_buy}\n")

                open_price = snap.get("session_anchor", "—")
                close_price = snap.get("driving_price", "—")
                diff_text = ""
                close_diff_text = ""
                try:
                    open_display = float(open_price)
                    low_display = float(snap.get("session_low"))
                    high_display = float(snap.get("session_high"))
                    diff_low = round(open_display - low_display, 4)
                    diff_high = round(high_display - open_display, 4)
                    high_time = snap.get("session_high_time", 0)
                    low_time = snap.get("session_low_time", 0)

                    if high_time > 0 and low_time > 0 and high_display != open_display and low_display != open_display:
                        if high_time < low_time:
                            time_note = "High First, Low Last"
                        elif low_time < high_time:
                            time_note = "Low First, High Last"
                        else:
                            time_note = ""
                    elif high_display != open_display and low_display == open_display:
                        time_note = "High Hit Only"
                    elif low_display != open_display and high_display == open_display:
                        time_note = "Low Hit Only"
                    else:
                        time_note = "No Drawdown"
                    diff_text = f"  (Low: -{diff_low} | High: +{diff_high} | {time_note})"

                    close_display = float(close_price)
                    low_close = round(abs(close_display - low_display), 4)
                    open_close = round(abs(close_display - open_display), 4)
                    high_close = round(abs(close_display - high_display), 4)
                    close_note = ""
                    if low_close >= open_close and open_close < high_close:
                        close_note = " => Bullish"
                    elif low_close < open_close and open_close >= high_close:
                        close_note = " => Bearish"
                    close_diff_text = f"  (Low C: {low_close} | Open C: {open_close} | High C: {high_close}{close_note})"
                except (TypeError, ValueError):
                    pass

                f.write(f"Tracking Time: {self.format_elapsed_static(snap.get('tracking_elapsed', 0))}\n")
                f.write(f"Range: {snap.get('session_low', '—')} - {snap.get('session_high', '—')}{diff_text}\n")
                f.write(f"Open: {open_price}    Close: {close_price}{close_diff_text}\n")
                f.write("--------------------------------------\n")
        except Exception as e:
            log_msg(f"Text Logging Error: {e}")

    # ── Auto-Trend Logic ──
    def internal_reset(self):
        """Automatically triggers the reset transition."""
        with self.phase_transition_lock:
            with self.lock, self.market_lock:
                if not self.tracking_active or self.recording_phase == 0:
                    return

            snap = self.build_snapshot()
            self.log_milestone(snap)
            now = time.time()
            with self.lock, self.probability_lock:
                self.state_revision += 1
                if not self.tracking_active or self.recording_phase == 0:
                    return

                if self.recording_phase == 1:
                    self.milestone_target = self.current_benchmark_threshold
                    log_msg(f"AUTO-MILESTONE LOCKED: {self.milestone_target}s (Time Limit)")

                if self.recording_phase < 3:
                    self.recording_phase += 1
                    with self.market_lock:
                        self.buy_market_volume = 0
                        self.sell_market_volume = 0
                        self.tracking_start_time = now
                    dp = self.get_driving_price()
                    self.session_anchor_price = dp
                    self.session_high_price = dp
                    self.session_low_price = dp

                    if dp is not None:
                        self.initial_sell_limits = {}
                        self.initial_buy_limits = {}
                        for i in range(1, self.num_levels + 1):
                            start_offset = self.delta_target_distance * (i - 1)
                            end_offset = self.delta_target_distance * i
                            self.initial_sell_limits[i] = self.get_raw_limits_in_range(
                                dp + start_offset, dp + end_offset, True
                            )
                            self.initial_buy_limits[i] = self.get_raw_limits_in_range(
                                dp - end_offset, dp - start_offset, False
                            )

                    self.absolute_phase += 1
                    log_msg(f"AUTO-RESET: Entering Phase {self.absolute_phase} (Internal {self.recording_phase})")
                else:
                    log_msg("AUTO-TREND CYCLE COMPLETE — Continuing immediately without gaps.")
                    self.internal_start(is_auto=True)

    def internal_start(self, is_auto=False):
        """Standardized start logic for both manual and auto-pilot starts."""
        with self.phase_transition_lock, self.lock, self.probability_lock:
            self.state_revision += 1
            with self.market_lock:
                self.buy_market_volume = 0
                self.sell_market_volume = 0
                self.tracking_active = True
            self.milestone_target = self.current_benchmark_threshold
            self.recording_phase = 1
            if not is_auto:
                self.absolute_phase = 1
            else:
                self.absolute_phase += 1
                
            self.tracking_start_time = time.time()
            dp = self.get_driving_price()
            now_t = time.time()
            self.session_anchor_price = dp
            self.session_high_price = dp
            self.session_low_price = dp
            self.session_high_time = now_t
            self.session_low_time = now_t
            
            if dp is not None:
                self.initial_sell_limits = {}
                self.initial_buy_limits = {}
                for i in range(1, self.num_levels + 1):
                    start_offset = self.delta_target_distance * (i - 1)
                    end_offset = self.delta_target_distance * i
                    self.initial_sell_limits[i] = self.get_raw_limits_in_range(dp + start_offset, dp + end_offset, True)
                    self.initial_buy_limits[i] = self.get_raw_limits_in_range(dp - end_offset, dp - start_offset, False)
            
            with self.market_lock:
                self._clear_liquidity_memory_locked()
                self.market_revision += 1

    def check_auto_trend(self):
        with self.phase_transition_lock:
            with self.lock, self.market_lock:
                due = (
                    self.tracking_active
                    and self.recording_phase > 0
                    and self.tracking_start_time is not None
                    and time.time() - self.tracking_start_time >= self.milestone_target
                )
            if due:
                self.internal_reset()

    # ── Calculate target levels (in tick space) ──
    def get_target_levels(self):
        dp = self.get_driving_price()
        if dp is None:
            return [], []

        above = []
        below = []
        for i in range(1, self.num_levels + 1):
            dist = self.target_distance * i  # Distance in ticks
            above.append(dp + dist)
            below.append(dp - dist)
        return above, below

    # ── Calculate probability for each level ──
    def calculate_probabilities(self):
        dp = self.get_driving_price()
        if dp is None:
            return []

        with self.market_lock:
            buy_market_volume = self.buy_market_volume
            sell_market_volume = self.sell_market_volume

        above_targets, below_targets = self.get_target_levels()
        up_liquidity, down_liquidity, up_ghost_flags, down_ghost_flags = (
            self.get_target_liquidity(dp)
        )
        results = []

        for i in range(len(above_targets)):
            target_up = above_targets[i]
            target_down = below_targets[i]
            level_num = i + 1
            dist = self.target_distance * level_num

            visible_up = up_liquidity[i]
            visible_down = down_liquidity[i]
            is_ghosting = up_ghost_flags[i] or down_ghost_flags[i]
            up_delta = 0
            down_delta = 0
            if level_num in self.last_up_weights:
                last = self.last_up_weights[level_num]
                if last > 0:
                    up_delta = ((visible_up - last) / last) * 100
            if level_num in self.last_down_weights:
                last = self.last_down_weights[level_num]
                if last > 0:
                    down_delta = ((visible_down - last) / last) * 100

            self.last_up_weights[level_num] = visible_up
            self.last_down_weights[level_num] = visible_down

            bullish_denominator = buy_market_volume + visible_up
            bearish_denominator = sell_market_volume + visible_down
            up_probability = (
                (buy_market_volume / bullish_denominator) * 100
                if bullish_denominator > 0 else 0.0
            )
            down_probability = (
                (sell_market_volume / bearish_denominator) * 100
                if bearish_denominator > 0 else 0.0
            )

            results.append({
                "level": level_num,
                "distance": dist,
                "target_up": self.tick_to_display(target_up),
                "target_down": self.tick_to_display(target_down),
                "sell_limits": int(visible_up),
                "buy_limits": int(visible_down),
                "up_probability": round(up_probability, 1),
                "down_probability": round(down_probability, 1),
                "up_delta": round(up_delta, 1),
                "down_delta": round(down_delta, 1),
                "net_imbalance": int(visible_down - visible_up),
                "is_ghosting": is_ghosting,
            })

        return results

    # ── Build full snapshot for dashboard ──
    def build_snapshot(self):
        with self.lock:
            with self.market_lock:
                dp = self.get_driving_price()
                best_bid = self.best_bid
                best_ask = self.best_ask
                buy_market_volume = self.buy_market_volume
                sell_market_volume = self.sell_market_volume
                tracking_active = self.tracking_active
                depth_revision = self.depth_revision

            now_t = time.time()
            if tracking_active and dp is not None:
                if self.session_high_price is None or dp > self.session_high_price:
                    self.session_high_price = dp
                    self.session_high_time = now_t
                if self.session_low_price is None or dp < self.session_low_price:
                    self.session_low_price = dp
                    self.session_low_time = now_t

            tracking_start_time = self.tracking_start_time
            elapsed = round(now_t - tracking_start_time, 1) if tracking_start_time else 0
            anchor = self.session_anchor_price
            high_price = self.session_high_price
            low_price = self.session_low_price
            high_time = self.session_high_time
            low_time = self.session_low_time
            num_levels = self.num_levels
            target_distance = self.delta_target_distance
            initial_sell_limits = self.initial_sell_limits
            initial_buy_limits = self.initial_buy_limits
            recording_phase = self.recording_phase
            absolute_phase = self.absolute_phase
            milestone_target = self.milestone_target
            state_revision = self.state_revision

        cache_key = (depth_revision, state_revision)
        with self.snapshot_cache_lock:
            cached = self.snapshot_cache_key == cache_key
            if cached:
                levels_data = self.snapshot_levels_cache

        if not cached:
            with self.market_lock:
                depth_revision = self.depth_revision
                dp = self.get_driving_price()
                best_bid = self.best_bid
                best_ask = self.best_ask
                buy_market_volume = self.buy_market_volume
                sell_market_volume = self.sell_market_volume
                tracking_active = self.tracking_active
                cache_key = (depth_revision, state_revision)
                with self.snapshot_cache_lock:
                    cached = self.snapshot_cache_key == cache_key
                    if cached:
                        levels_data = self.snapshot_levels_cache

                if not cached:
                    levels_data = []
                    if anchor is not None:
                        for i in range(1, num_levels + 1):
                            start_offset = target_distance * (i - 1)
                            end_offset = target_distance * i
                            current_sell = self._sum_depth_range_locked(
                                anchor + start_offset, anchor + end_offset, True
                            )
                            current_buy = self._sum_depth_range_locked(
                                anchor - end_offset, anchor - start_offset, False
                            )
                            initial_sell = initial_sell_limits.get(i, 0)
                            initial_buy = initial_buy_limits.get(i, 0)
                            levels_data.append({
                                "level": i,
                                "start_offset": start_offset,
                                "end_offset": end_offset,
                                "initial_sell_limits": initial_sell,
                                "current_sell_limits": current_sell,
                                "delta_sell": current_sell - initial_sell,
                                "initial_buy_limits": initial_buy,
                                "current_buy_limits": current_buy,
                                "delta_buy": current_buy - initial_buy,
                            })

                    with self.snapshot_cache_lock:
                        self.snapshot_cache_key = cache_key
                        self.snapshot_levels_cache = levels_data

        return {
            "type": "snapshot",
            "timestamp": time.time(),
            "driving_price": self.tick_to_display(dp),
            "best_bid": self.tick_to_display(best_bid),
            "best_ask": self.tick_to_display(best_ask),
            "buy_market_volume": buy_market_volume,
            "sell_market_volume": sell_market_volume,
            "tracking_active": tracking_active,
            "tracking_elapsed": elapsed,
            "session_anchor": self.tick_to_display(anchor) if anchor else None,
            "session_high": self.tick_to_display(high_price) if high_price else None,
            "session_low": self.tick_to_display(low_price) if low_price else None,
            "session_high_time": high_time,
            "session_low_time": low_time,
            "recording_phase": recording_phase,
            "absolute_phase": absolute_phase,
            "milestone_target": milestone_target,
            "levels": levels_data,
        }

    # ── Bookmap callbacks ──
    def _reset_market_book_locked(self):
        self.bid_depth.clear()
        self.ask_depth.clear()
        self.bid_price_heap.clear()
        self.ask_price_heap.clear()
        self.bid_price_buckets.clear()
        self.ask_price_buckets.clear()
        self._clear_liquidity_memory_locked()
        self.tick_history.clear()
        self.best_bid = None
        self.best_ask = None
        self.last_trade_price = None
        self.last_price_for_velocity = None
        self.last_up_weights.clear()
        self.last_down_weights.clear()
        self.book_revision += 1
        self.depth_revision += 1
        self.market_revision += 1
        with self.snapshot_cache_lock:
            self.snapshot_cache_key = None

    def _clear_liquidity_memory_locked(self):
        self.memory_price_buckets.clear()
        self.liquidity_memory.clear()
        self.memory_cleanup_queue.clear()
        self.memory_cleanup_queued.clear()

    def on_subscribe(self, alias, pips, size_multiplier):
        with self.lock, self.market_lock:
            self._reset_market_book_locked()
            self.active_alias = alias
            self.pips = pips
            self.size_multiplier = size_multiplier
        log_msg(f"Subscribed to {alias}, pips={pips}, size_multiplier={size_multiplier}")

    def on_unsubscribe(self, alias):
        with self.market_lock:
            if alias == self.active_alias:
                self._reset_market_book_locked()
                self.active_alias = None
        log_msg(f"Unsubscribed from {alias}")

    def update_decay_rate(self, now=None):
        """Calculates market velocity and adjusts memory duration."""
        if now is None:
            now = time.time()
        cutoff = now - 5.0
        while self.tick_history and self.tick_history[0] <= cutoff:
            self.tick_history.popleft()
        tps = len(self.tick_history) / 5.0 # Ticks Per Second
        
        previous_period = self.dynamic_decay_period
        if tps > 20: self.dynamic_decay_period = 0.5   # High Vol
        elif tps < 5: self.dynamic_decay_period = 5.0  # Low Vol
        else: self.dynamic_decay_period = 3.0           # Normal
        return self.dynamic_decay_period != previous_period
        
    def kill_ghost_at_price(self, price):
        """Instantly deletes memory if price trades through it (The Kill Switch)."""
        if price in self.liquidity_memory:
            del self.liquidity_memory[price]
            self._remove_indexed_price(self.memory_price_buckets, price)
            return True
        return False

    def on_trade(self, alias, is_buy_aggressor, price, size, is_otc=None):
        # Skip zero-size trades
        if size <= 0:
            return
        with self.market_lock:
            if alias != self.active_alias:
                return
            # Convert Bookmap's fractional tick price to integer tick level
            tick_price = int(round(price))
            # Convert Bookmap's internal size to real contract volume
            real_size = size / self.size_multiplier if hasattr(self, 'size_multiplier') and self.size_multiplier else size

            # ── Kill Switch ──
            changed = self.kill_ghost_at_price(tick_price)
            
            if self.last_trade_price != tick_price:
                changed = True
            self.last_trade_price = tick_price

            # Velocity tracking
            if tick_price != self.last_price_for_velocity:
                self.tick_history.append(time.time())
                self.last_price_for_velocity = tick_price
                changed = True

            if self.tracking_active:
                if is_buy_aggressor:
                    self.buy_market_volume += real_size
                else:
                    self.sell_market_volume += real_size
                changed = True
            if changed:
                self.market_revision += 1

    def on_depth(self, alias, is_bid, price, size):
        with self.market_lock:
            if alias != self.active_alias:
                return
            # Convert internal size to real limit size
            real_size = size / self.size_multiplier if hasattr(self, 'size_multiplier') and self.size_multiplier else size
            
            # ── Kill Switch ──
            ghost_removed = self.kill_ghost_at_price(price)
            
            target_dict = self.bid_depth if is_bid else self.ask_depth
            price_heap = self.bid_price_heap if is_bid else self.ask_price_heap
            price_buckets = self.bid_price_buckets if is_bid else self.ask_price_buckets
            book_changed = False
            if real_size > 0:
                previous_size = target_dict.get(price)
                if price not in target_dict:
                    self._add_indexed_price(price_buckets, price)
                    heapq.heappush(price_heap, -price if is_bid else price)
                    self.book_revision += 1
                    book_changed = True
                target_dict[price] = real_size
                if previous_size != real_size:
                    self.depth_revision += 1
                # ── Liquidity Memory (High Water Mark) ──
                mem = self.liquidity_memory.get(price)
                if not mem or real_size > mem['size']:
                    self.liquidity_memory[price] = {
                        'size': real_size, 
                        'time': time.time(),
                        'side': 'bid' if is_bid else 'ask'
                    }
                    self._add_indexed_price(self.memory_price_buckets, price)
                    if price not in self.memory_cleanup_queued:
                        self.memory_cleanup_queue.append(price)
                        self.memory_cleanup_queued.add(price)
            else:
                if price in target_dict:
                    del target_dict[price]
                    self._remove_indexed_price(price_buckets, price)
                    self.book_revision += 1
                    self.depth_revision += 1
                    book_changed = True
                    ghost_removed = True

            while self.bid_price_heap and -self.bid_price_heap[0] not in self.bid_depth:
                heapq.heappop(self.bid_price_heap)
            self.best_bid = -self.bid_price_heap[0] if self.bid_price_heap else None
            while self.ask_price_heap and self.ask_price_heap[0] not in self.ask_depth:
                heapq.heappop(self.ask_price_heap)
            self.best_ask = self.ask_price_heap[0] if self.ask_price_heap else None

            if real_size > 0 or ghost_removed or book_changed:
                self.market_revision += 1


# ══════════════════════════════════════════════════════
#   GLOBAL ENGINE INSTANCE
# ══════════════════════════════════════════════════════
engine = PriceTargetEngine()


# ══════════════════════════════════════════════════════
#   PORT CLEANUP UTILITY
# ══════════════════════════════════════════════════════
def kill_port(port):
    """Kill any process holding the given port (Windows only)."""
    try:
        import subprocess
        result = subprocess.run(
            f'netstat -ano | findstr :{port}',
            shell=True, capture_output=True, text=True
        )
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                parts = line.strip().split()
                pid = parts[-1]
                if pid.isdigit() and pid != '0' and pid != str(os.getpid()):
                    subprocess.run(f'taskkill /PID {pid} /F', shell=True,
                                   capture_output=True)
                    log_msg(f"Killed stale process PID {pid} on port {port}")
        time.sleep(1)
    except Exception as e:
        log_msg(f"Port cleanup warning: {e}")


# ══════════════════════════════════════════════════════
#   WEBSOCKET SERVER (runs in a background thread)
# ══════════════════════════════════════════════════════
async def ws_handler(websocket):
    engine.ws_clients.add(websocket)
    log_msg(f"WebSocket client connected ({len(engine.ws_clients)} total)")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if data.get("type") == "settings":
                    engine.update_settings(data)
            except json.JSONDecodeError:
                pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        engine.ws_clients.discard(websocket)
        log_msg(f"WebSocket client disconnected ({len(engine.ws_clients)} total)")


async def broadcast_loop():
    """Send snapshot to all connected clients at high frequency."""
    while True:
        try:
            if engine.ws_clients:
                snapshot = engine.build_snapshot()
                msg = json.dumps(snapshot)
                dead = set()
                for ws in engine.ws_clients.copy():
                    try:
                        await ws.send(msg)
                    except Exception:
                        dead.add(ws)
                engine.ws_clients -= dead
        except Exception as e:
            log_msg(f"Error in broadcast loop: {e}")
            
        await asyncio.sleep(engine.snapshot_interval)


async def ws_main(port):
    async with websockets.serve(ws_handler, "0.0.0.0", port):
        log_msg(f"WebSocket server running on ws://localhost:{port}")
        await broadcast_loop()


def start_ws_server():
    global WS_PORT
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Try primary port, then kill and retry, then try fallback ports
    for port in [WS_PORT, WS_PORT, WS_PORT + 1, WS_PORT + 2]:
        try:
            WS_PORT = port
            loop.run_until_complete(ws_main(port))
            break
        except OSError as e:
            if e.errno == 10048:  # Port in use
                log_msg(f"Port {port} in use, attempting cleanup...")
                kill_port(port)
            else:
                log_msg(f"WebSocket server error: {e}")
                break


# ══════════════════════════════════════════════════════
#   HTTP SERVER (serves the dashboard HTML/CSS/JS)
# ══════════════════════════════════════════════════════
class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DASHBOARD_DIR, **kwargs)

    def log_message(self, format, *args):
        pass  # Suppress HTTP logs


def start_http_server():
    global HTTP_PORT
    kill_port(HTTP_PORT)  # Clean up before binding
    for port in [HTTP_PORT, HTTP_PORT + 1, HTTP_PORT + 2]:
        try:
            server = HTTPServer(("0.0.0.0", port), DashboardHandler)
            HTTP_PORT = port
            log_msg(f"Dashboard HTTP server running on http://localhost:{port}")
            server.serve_forever()
            break
        except OSError:
            log_msg(f"HTTP port {port} in use, trying next...")
            kill_port(port)


# ══════════════════════════════════════════════════════
#   BOOKMAP HANDLERS
# ══════════════════════════════════════════════════════
def handle_subscribe_instrument(addon, alias, full_name, is_crypto, pips, size_multiplier, instrument_multiplier, supported_features):
    engine.on_subscribe(alias, pips, size_multiplier)
    bm.subscribe_to_depth(addon, alias, 1)
    bm.subscribe_to_trades(addon, alias, 2)

def handle_unsubscribe_instrument(addon, alias):
    engine.on_unsubscribe(alias)

def handle_trade(addon, alias, price, size, *args):
    is_buy_aggressor = bool(args[1]) if len(args) > 1 else False
    engine.on_trade(alias, is_buy_aggressor, price, size)

def handle_depth_info(addon, alias, is_bid, price, size):
    engine.on_depth(alias, is_bid, price, size)


# ══════════════════════════════════════════════════════
#   MAIN
# ══════════════════════════════════════════════════════
if __name__ == "__main__":
    if not os.path.exists(r"C:\Bookmap\Logs"):
        os.makedirs(r"C:\Bookmap\Logs")

    log_msg("=" * 60)
    log_msg("  PRICE TARGET PROBABILITY ENGINE")
    log_msg("=" * 60)

    # Start HTTP server thread
    http_thread = threading.Thread(target=start_http_server, daemon=True)
    http_thread.start()

    # Start WebSocket server thread
    ws_thread = threading.Thread(target=start_ws_server, daemon=True)
    ws_thread.start()

    log_msg(f"Dashboard: http://localhost:{HTTP_PORT}")
    log_msg(f"WebSocket: ws://localhost:{WS_PORT}")

    # Start Bookmap addon
    addon = bm.create_addon()
    bm.add_trades_handler(addon, handle_trade)
    bm.add_depth_handler(addon, handle_depth_info)

    bm.start_addon(addon, handle_subscribe_instrument, handle_unsubscribe_instrument)
    bm.wait_until_addon_is_turned_off(addon)
