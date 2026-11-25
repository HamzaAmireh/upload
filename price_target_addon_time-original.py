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
from http.server import HTTPServer, SimpleHTTPRequestHandler
import asyncio
import random
import queue

# ─────────────────────── ASYNC LOGGING QUEUE ───────────────────────
log_queue = queue.Queue()

def async_file_writer():
    while True:
        try:
            target_file, msg = log_queue.get()
            with open(target_file, "a", encoding="utf-8") as f:
                f.write(msg)
        except Exception:
            pass
        finally:
            log_queue.task_done()

threading.Thread(target=async_file_writer, daemon=True).start()

class AsyncLogContext:
    def __init__(self, filepath, mode="a", encoding="utf-8"):
        self.filepath = filepath
        self.content = []
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.content:
            log_queue.put((self.filepath, "".join(self.content)))
    def write(self, text):
        self.content.append(text)

# ── Try importing websockets; if not installed, guide the user ──
try: 
    import websockets
    import websockets.sync.server as ws_sync
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
        self.alias_to_order_book = {}

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

        # ── Hidden Liquidity pools (Icebergs and Stops) ──
        self.iceberg_depth = {} # price -> hidden size
        self.stop_depth = {}    # price -> stop size

        # ── Trend Instance Tracking ──
        self.level_trend_stats = {} # level_num -> {bull_count, bear_count, bull_time, bear_time, bull_energy, bear_energy, bull_vol, bear_vol, current_side, current_start}
        
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

        # ── Institutional Power Signal Tracking ──
        self.rolling_history = []  # List of (timestamp, price, buy_vol, sell_vol)
        self.power_scores = {}     # level_num -> score (0-100)
        self.history_window = 10.0 # 10 second rolling window for ER calculation
        self.lookahead_ticks = 10  # Look 10 ticks past the target
        self.last_up_weights = {}  # level_num -> total_weight
        self.last_down_weights = {}# level_num -> total_weight
        
        # ── Liquidity Decay (Anti-Spoofing) ──
        self.liquidity_memory = {}  # price -> {'size': last_max_size, 'time': last_seen_time, 'side': 'bid'/'ask'}
        self.dynamic_decay_period = 3.0  
        self.tick_history = []      # Timestamps of price changes for velocity
        self.last_price_for_velocity = None
        
        # ── Trend Milestone Tracking ──
        self.recording_phase = 0     # 0: Standby, 1: Benchmark, 2: Sync1, 3: Sync2
        self.absolute_phase = 0      # Continuous phase counter
        self.milestone_target = 0    # Target volume from Phase 1
        self.current_benchmark_threshold = 100
        self.milestone_queue = []    # Thread-safe queue for auto-captured snapshots
        
        # Rolling 9-phase Macro trackers
        self.macro_phase_window = 9
        self.macro_buy_volumes = []
        self.macro_sell_volumes = []
        self.macro_buy_reloads = []
        self.macro_sell_reloads = []
        
        # Rolling 2-Phase trackers
        self.prev_buy_volume = 0
        self.prev_sell_volume = 0
        self.prev_buy_reload = 0
        self.prev_sell_reload = 0
        self.rolling_buy_eff = 0
        self.rolling_sell_eff = 0
        
        # Bingo Springboard memory
        self.prev_mkt_buy_vel = 0.0
        self.prev_mkt_sell_vel = 0.0
        self.prev_buy_delta_vel = 0.0
        self.prev_sell_delta_vel = 0.0
        
        # Rolling 2-Phase Price Footprint memory
        self.prev_low_c = 0
        self.prev_high_c = 0
        self.prev_low_price = 0
        self.prev_high_price = 0
        self.prev_open_price = 0
        
        # Velocity Intention Tracker
        self.vel_buy_count = 0        # Total BUY SIDE positive velocity events
        self.vel_sell_count = 0       # Total SELL SIDE positive velocity events
        self.vel_buy_cum = 0.0        # Cumulative BUY SIDE positive velocity sum
        self.vel_sell_cum = 0.0       # Cumulative SELL SIDE positive velocity sum
        self.vel_streak_side = None   # 'BUY' or 'SELL'
        self.vel_streak_count = 0     # Consecutive events on same side
        self.vel_threshold = 0.10    # Minimum velocity to log as intention event
        
        # ── Floating Imbalance Weight Tracking (mirrors dashboard) ──
        self.cumulative_weight = 0.0   # Running sum across all phases
        self.phase_weight = 0.0        # Resets each phase
        # Phase average delta tracking
        self.phase_delta_sum = 0.0     # Sum of all delta samples this phase
        self.phase_delta_count = 0     # Number of samples this phase

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
                with self.lock:
                    if self.tracking_active:
                        self.update_power_metrics()
                        self.latest_probabilities = self.calculate_probabilities()
                        self.check_auto_trend()
            except Exception as e:
                log_msg(f"Backbone loop error: {e}")
            time.sleep(0.05)

    def _accumulate_weight(self, dp):
        """Called once per build_snapshot (0.25s cadence = same as dashboard). Keeps weights in sync."""
        if dp is not None and self.tracking_active and self.recording_phase > 0:
            td = self.delta_target_distance
            buy_lim = self.get_raw_limits_in_range(dp - td, dp, False)
            sell_lim = self.get_raw_limits_in_range(dp, dp + td, True)
            net = buy_lim - sell_lim
            self.cumulative_weight += net
            self.phase_weight += net
            # Running average of the delta for this phase
            self.phase_delta_sum += net
            self.phase_delta_count += 1

    # ── Settings update ──
    def update_settings(self, settings):
        with self.lock:
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
                    self.tracking_active = False
                    self.recording_phase = 0
                    log_msg("TRACKING STOPPED")
                    
                    # Set Master Anchor on Start
                    dp = self.get_driving_price()
                    now_t = time.time()
                    self.session_anchor_price = dp
                    self.session_high_price = dp
                    self.session_low_price = dp
                    self.session_high_time = now_t
                    self.session_low_time = now_t
                    
                    log_msg(f"TRACKING STARTED — Phase 1 (Benchmark)")
                elif action == "stop":
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
                    
                    self.buy_market_volume = 0
                    self.sell_market_volume = 0
                    self.tracking_start_time = time.time() if self.tracking_active else None
                    self.iceberg_depth = {}
                    self.stop_depth = {}
                    self.level_trend_stats = {}
                    
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
        total = 0
        target_dict = self.ask_depth if is_ask else self.bid_depth
        p_min = min(start_price, end_price)
        p_max = max(start_price, end_price)
        for p, vol in target_dict.items():
            if p_min <= p <= p_max:
                total += vol
        return total

    # ── Sum all types of liquidity (Visible + Hidden + Stops) ──
    def get_total_liquidity_at_level(self, dp, target_price):
        """Calculates total current wall in the range [dp, target_price] plus lookahead."""
        is_up = target_price > dp
        now = time.time()
        
        if is_up:
            p_low = dp
            p_high = target_price + self.lookahead_ticks
            target_dict = self.ask_depth
        else:
            p_low = target_price - self.lookahead_ticks
            p_high = dp
            target_dict = self.bid_depth
        
        visible = 0
        hidden = 0
        is_ghosting = False
        
        # We iterate over the range to catch "Memory" of prices that might now be 0
        all_prices = set(target_dict.keys())
        # Also include prices in memory within this range
        for p_mem in self.liquidity_memory.keys():
            if p_low <= p_mem <= p_high:
                all_prices.add(p_mem)

        for p in all_prices:
            if p_low <= p <= p_high:
                # 1. Actual current size
                actual_size = target_dict.get(p, 0)
                
                # 2. Decayed Memory size
                mem = self.liquidity_memory.get(p)
                decayed_size = 0
                if mem:
                    elapsed = now - mem['time']
                    if elapsed < self.dynamic_decay_period:
                        # Linear decay from max_size to actual_size
                        remaining_ratio = (self.dynamic_decay_period - elapsed) / self.dynamic_decay_period
                        # Only decay down to the actual size, never below
                        mem_potential = mem['size'] * remaining_ratio
                        decayed_size = max(actual_size, mem_potential)
                        if decayed_size > actual_size + 1: # Significant ghosting
                            is_ghosting = True
                    else:
                        # Expired memory
                        pass
                
                # Use the larger of the two (The "Institutional Memory")
                visible += max(actual_size, decayed_size)
                
                # Add detected hidden pools
                hidden += self.iceberg_depth.get(p, 0)
                hidden += self.stop_depth.get(p, 0)
        
        return visible, hidden, is_ghosting

    # ── Update Institutional Power Metrics ──
    def update_power_metrics(self):
        now = time.time()
        dp = self.get_driving_price()
        if dp is None: return

        # 1. Update rolling history (10s window)
        self.rolling_history.append((now, dp, self.buy_market_volume, self.sell_market_volume))
        self.rolling_history = [h for h in self.rolling_history if now - h[0] <= self.history_window]

        if len(self.rolling_history) < 2: return

        # 2. Calculate Efficiency Ratio (ER)
        h_start = self.rolling_history[0]
        h_end = self.rolling_history[-1]
        
        price_delta = abs(h_end[1] - h_start[1])
        vol_delta = (h_end[2] - h_start[2]) + (h_end[3] - h_start[3])
        
        # ER = Price Move / Volume (higher is more efficient/momentum, lower is more absorption)
        er = (price_delta / vol_delta) if vol_delta > 0 else 0
        
        # 3. Support Migration & Strength (Check nearest 5 ticks)
        # We look for where the 'Weight' is relative to price
        bid_support_score = 0
        ask_resist_score = 0
        
        for i in range(1, 6):
            b_size = self.bid_depth.get(dp - i, 0)
            a_size = self.ask_depth.get(dp + i, 0)
            # High weight near price = Guarded Support
            bid_support_score += (b_size / i)
            ask_resist_score += (a_size / i)

        # 4. Composite Power Score for each Level
        for i in range(1, self.num_levels + 1):
            stats = self.level_trend_stats.get(i)
            if not stats: continue
            
            # Normalize components (Simplified)
            eff_factor = min(er * 10, 1.0) 
            
            # Support Factor
            if stats["current_side"] == "bull":
                sup_factor = min(bid_support_score / (ask_resist_score + 1), 1.0)
                vol_intensity = min((h_end[2] - h_start[2]) / 100, 1.0)
            else:
                sup_factor = min(ask_resist_score / (bid_support_score + 1), 1.0)
                vol_intensity = min((h_end[3] - h_start[3]) / 100, 1.0)

            # Composite Score (0-100)
            score = (vol_intensity * 40) + (sup_factor * 30) + (eff_factor * 30)
            self.power_scores[i] = round(score, 1)

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

    def format_weight(self, value):
        """Formats weight values the same way as the dashboard's formatNumber() function."""
        abs_val = abs(value)
        sign = '+' if value > 0 else ('-' if value < 0 else '')
        if abs_val >= 1_000_000:
            return f"{sign}{abs_val / 1_000_000:.1f}M"
        if abs_val >= 1_000:
            return f"{sign}{abs_val / 1_000:.1f}K"
        return f"{sign}{int(abs_val)}"

    def log_milestone(self, snap):
        """Writes captured trend data in a clean 'Simple Sniper' text format."""
        try:
            abs_phase = snap.get('absolute_phase', 0)
            bingo_signal = ""
            log_msg(f"DEBUG: log_milestone triggered for Phase {abs_phase}")
            with AsyncLogContext(TREND_LOG_FILE, "a", encoding="utf-8") as f:
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                
                f.write(f"\n--- [TREND CAPTURED] {ts} ---\n")
                f.write(f"PHASE: {abs_phase}\n")
                
                
                # Delta Liquidity Calculation for all levels
                if self.session_anchor_price is not None:
                    for i in range(1, self.num_levels + 1):
                        start_offset = self.delta_target_distance * (i - 1)
                        end_offset = self.delta_target_distance * i
                        
                        current_sell_limits = self.get_raw_limits_in_range(self.session_anchor_price + start_offset, self.session_anchor_price + end_offset, True)
                        current_buy_limits = self.get_raw_limits_in_range(self.session_anchor_price - end_offset, self.session_anchor_price - start_offset, False)
                        
                        init_sell = self.initial_sell_limits.get(i, 0)
                        init_buy = self.initial_buy_limits.get(i, 0)
                        
                        delta_sell = current_sell_limits - init_sell
                        delta_buy = current_buy_limits - init_buy
                        
                        buy_reload = delta_sell + self.buy_market_volume
                        sell_reload = delta_buy + self.sell_market_volume
                        
                        tracking_time = snap.get('tracking_elapsed', 1)
                        if tracking_time <= 0: tracking_time = 1
                        
                        safe_buy_vol = self.buy_market_volume if self.buy_market_volume > 0 else 1
                        safe_sell_vol = self.sell_market_volume if self.sell_market_volume > 0 else 1
                        
                        buy_eff = round((buy_reload / safe_buy_vol) * 100, 1)
                        sell_eff = round((sell_reload / safe_sell_vol) * 100, 1)
                        
                        buy_delta_vel = round(delta_sell / tracking_time, 2)
                        sell_delta_vel = round(delta_buy / tracking_time, 2)
                        
                        # Market Order Velocities (Aggression Speed)
                        mkt_buy_vel = round(self.buy_market_volume / tracking_time, 2)
                        mkt_sell_vel = round(self.sell_market_volume / tracking_time, 2)
                        
                        if i == 1:
                            self.macro_buy_volumes.append(safe_buy_vol)
                            self.macro_sell_volumes.append(safe_sell_vol)
                            self.macro_buy_reloads.append(buy_reload)
                            self.macro_sell_reloads.append(sell_reload)
                            if len(self.macro_buy_volumes) > self.macro_phase_window:
                                self.macro_buy_volumes.pop(0)
                                self.macro_sell_volumes.pop(0)
                                self.macro_buy_reloads.pop(0)
                                self.macro_sell_reloads.pop(0)
                            
                            # Bingo Springboard checks (Must run BEFORE prev_ variables are overwritten)
                            if self.absolute_phase > 1:
                                p1_sell_dominant = self.prev_mkt_sell_vel > self.prev_mkt_buy_vel
                                p1_floor_strong = self.prev_sell_reload > self.prev_buy_reload
                                p1_ceil_losing = self.prev_buy_delta_vel < 0
                                p1_floor_losing = self.prev_sell_delta_vel < 0
                                
                                p2_gap_closing = abs(mkt_sell_vel - mkt_buy_vel) <= 0.05
                                p2_floor_strong = sell_reload > buy_reload
                                p2_ceil_worse = buy_delta_vel < self.prev_buy_delta_vel
                                p2_floor_positive = sell_delta_vel > 0
                                
                                if (p1_sell_dominant and p1_floor_strong and p1_ceil_losing and p1_floor_losing and
                                    p2_gap_closing and p2_floor_strong and p2_ceil_worse and p2_floor_positive):
                                    bingo_signal = (
                                        "============================================================\n"
                                        "🎯 [BINGO SPRINGBOARD] PASSIVE ABSORPTION TRAP DETECTED!    \n"
                                        "   Setup: Floor Limits dominated neutralizing Sell Velocity.\n"
                                        "   Signal: High probability 10-20 tick BULLISH Squeeze.     \n"
                                        "============================================================"
                                    )

                                r_buy_vol = self.prev_buy_volume + safe_buy_vol
                                r_sell_vol = self.prev_sell_volume + safe_sell_vol
                                r_buy_reload = self.prev_buy_reload + buy_reload
                                r_sell_reload = self.prev_sell_reload + sell_reload
                                
                                self.rolling_buy_eff = round((r_buy_reload / r_buy_vol) * 100, 1) if r_buy_vol > 0 else 0
                                self.rolling_sell_eff = round((r_sell_reload / r_sell_vol) * 100, 1) if r_sell_vol > 0 else 0
                                
                            self.prev_buy_volume = safe_buy_vol
                            self.prev_sell_volume = safe_sell_vol
                            self.prev_buy_reload = buy_reload
                            self.prev_sell_reload = sell_reload
                            self.prev_mkt_buy_vel = mkt_buy_vel
                            self.prev_mkt_sell_vel = mkt_sell_vel
                            self.prev_buy_delta_vel = buy_delta_vel
                            self.prev_sell_delta_vel = sell_delta_vel
                        
                        f.write(f"[LEVEL {i}] - ({start_offset} to {end_offset} ticks offset)\n")
                        f.write(f"[BUY SIDE - {self.buy_market_volume}v / M Velocity= {mkt_buy_vel}] reload power = {init_sell} - {self.buy_market_volume} = {init_sell - self.buy_market_volume} to positive {current_sell_limits} = {buy_reload} load pressure\n")
                        f.write(f"Reload Efficiency: {buy_eff}%  |  Delta Velocity: {buy_delta_vel} limits/sec  |  Mkt Velocity: {mkt_buy_vel} vol/sec\n")
                        f.write(f"Initial Sell Limits (Resistance): {init_sell}\n")
                        f.write(f"Current Sell Limits: {current_sell_limits}\n")
                        sgn_sell = "+" if delta_sell > 0 else ""
                        f.write(f"Delta: {sgn_sell}{delta_sell}\n\n")
                        
                        f.write(f"[SELL SIDE - {self.sell_market_volume}v / M Velocity= {mkt_sell_vel}] reload power = {init_buy} - {self.sell_market_volume} = {init_buy - self.sell_market_volume} to positive {current_buy_limits} = {sell_reload} load pressure\n")
                        f.write(f"Reload Efficiency: {sell_eff}%  |  Delta Velocity: {sell_delta_vel} limits/sec  |  Mkt Velocity: {mkt_sell_vel} vol/sec\n")
                        f.write(f"Initial Buy Limits (Support): {init_buy}\n")
                        f.write(f"Current Buy Limits: {current_buy_limits}\n")
                        sgn_buy = "+" if delta_buy > 0 else ""
                        f.write(f"Delta: {sgn_buy}{delta_buy}\n")
                        
                        if buy_reload > sell_reload and current_sell_limits > current_buy_limits:
                            f.write("Sell Limit reload > Buy Limit reload && Current sell limits > current buy limits = Bearish\n\n")
                        elif sell_reload > buy_reload and current_buy_limits > current_sell_limits:
                            f.write("Sell Limit reload < Buy Limit reload && Current sell limits < current buy limits = Bullish\n\n")
                        elif buy_reload > sell_reload and current_sell_limits <= current_buy_limits:
                            f.write("Sell Limit reload > Buy Limit reload && Current sell limits < current buy limits = natural\n\n")
                        elif sell_reload > buy_reload and current_buy_limits <= current_sell_limits:
                            f.write("Sell Limit reload < Buy Limit reload && Current sell limits > current buy limits = natural\n\n")
                        else:
                            f.write("Sell Limit reload = Buy Limit reload => natural\n\n")
                            
                    if self.absolute_phase > 1 and self.macro_buy_volumes:
                        macro_buy_vol = sum(self.macro_buy_volumes)
                        macro_sell_vol = sum(self.macro_sell_volumes)
                        macro_buy_reload = sum(self.macro_buy_reloads)
                        macro_sell_reload = sum(self.macro_sell_reloads)
                        
                        m_buy_eff = round((macro_buy_reload / macro_buy_vol) * 100, 1) if macro_buy_vol > 0 else 0
                        m_sell_eff = round((macro_sell_reload / macro_sell_vol) * 100, 1) if macro_sell_vol > 0 else 0
                        m_note = "Bullish" if m_sell_eff > m_buy_eff else "Bearish"
                        
                        r_note = "Bullish" if self.rolling_sell_eff > self.rolling_buy_eff else "Bearish"
                        
                        f.write(f"[MACRO CUMULATIVE - {len(self.macro_buy_volumes)} PHASES]\n")
                        f.write(f"Total Resistance Efficiency: {m_buy_eff}% | Total Support Efficiency: {m_sell_eff}% => MACRO {m_note}\n\n")
                        
                        f.write(f"[ROLLING LAST 2 PHASES (Phase {self.absolute_phase-1} + {self.absolute_phase})]\n")
                        f.write(f"Rolling Resistance Efficiency: {self.rolling_buy_eff}% | Rolling Support Efficiency: {self.rolling_sell_eff}% => ROLLING {r_note}\n\n")
                else:
                    f.write("ERROR: No Anchor Price Recorded.\n\n")
                
                # Removed legacy state logging
                
                # Range & Anchors
                open_p = snap.get('session_anchor', '—')
                close_p = snap.get('driving_price', '—')
                
                # Calculate differences
                try:
                    open_disp = float(snap.get('session_anchor'))
                    low_disp = float(snap.get('session_low'))
                    high_disp = float(snap.get('session_high'))
                    diff_low = round(open_disp - low_disp, 4)
                    diff_high = round(high_disp - open_disp, 4)
                    
                    high_time = snap.get('session_high_time', 0)
                    low_time = snap.get('session_low_time', 0)
                    
                    if high_time > 0 and low_time > 0 and high_disp != open_disp and low_disp != open_disp:
                        if high_time < low_time:
                            time_note = "High First, Low Last"
                        elif low_time < high_time:
                            time_note = "Low First, High Last"
                        else:
                            time_note = ""
                    elif high_disp != open_disp and low_disp == open_disp:
                        time_note = "High Hit Only"
                    elif low_disp != open_disp and high_disp == open_disp:
                        time_note = "Low Hit Only"
                    else:
                        time_note = "No Drawdown"
                    
                    diff_str = f"  (Low: -{diff_low} | High: +{diff_high} | {time_note})"
                    
                    close_disp = float(snap.get('driving_price'))
                    low_c = round(abs(close_disp - low_disp), 4)
                    open_c = round(abs(close_disp - open_disp), 4)
                    high_c = round(abs(close_disp - high_disp), 4)
                    
                    c_note = ""
                    if low_c >= open_c and open_c < high_c:
                        c_note = " => Bullish"
                    elif low_c < open_c and open_c >= high_c:
                        c_note = " => Bearish"
                        
                    close_diff_str = f"  (Low C: {low_c} | Open C: {open_c} | High C: {high_c}{c_note})"
                    
                    footprint_pred = ""
                    if self.absolute_phase > 1:
                        # Filter 1: Reject if any LowC or HighC is exactly 0
                        has_zero_c = (self.prev_low_c == 0 or self.prev_high_c == 0 or low_c == 0 or high_c == 0)
                        
                        cond_bullish_close = (self.prev_low_c > self.prev_high_c) and (low_c > high_c)
                        cond_bearish_close = (self.prev_low_c < self.prev_high_c) and (low_c < high_c)
                        
                        if not has_zero_c and (cond_bullish_close or cond_bearish_close):
                            lowest_2p = min(self.prev_low_price, low_disp)
                            highest_2p = max(self.prev_high_price, high_disp)
                            p2_open = open_disp
                            
                            dist_to_low = round(p2_open - lowest_2p, 4)
                            dist_to_high = round(highest_2p - p2_open, 4)
                            
                            # Filter 2: Alignment of R values with C values
                            if cond_bullish_close and dist_to_low > dist_to_high:
                                footprint_pred = f"Condition Met: lowestR ({dist_to_low}) > HighestR ({dist_to_high}) => Bullish Next"
                            elif cond_bearish_close and dist_to_low < dist_to_high:
                                footprint_pred = f"Condition Met: lowestR ({dist_to_low}) < HighestR ({dist_to_high}) => Bearish Next"
                            else:
                                footprint_pred = "Next => Fake"
                        else:
                            footprint_pred = "Next => Fake"
                            
                    self.prev_low_c = low_c
                    self.prev_high_c = high_c
                    self.prev_low_price = low_disp
                    self.prev_high_price = high_disp
                    self.prev_open_price = open_disp
                    
                except Exception as e:
                    diff_str = f"  ({e})"
                    close_diff_str = ""
                    footprint_pred = ""

                f.write(f"Tracking Time: {self.format_elapsed_static(snap.get('tracking_elapsed', 0))}\n")
                f.write(f"Range: {snap.get('session_low', '—')} - {snap.get('session_high', '—')}{diff_str}\n")
                f.write(f"Open: {open_p}    Close: {close_p}{close_diff_str}\n")
                if footprint_pred:
                    f.write(f"2-Phase Footprint: {footprint_pred}\n")
                # Floating imbalance weights (formatted same as dashboard)
                cum_w = snap.get('cumulative_weight', 0)
                ph_w = snap.get('phase_weight', 0)
                avg_d = snap.get('phase_avg_delta', 0)
                f.write(f"Cumulative Weight: {self.format_weight(cum_w)}  |  Phase Weight: {self.format_weight(ph_w)}  |  Phase Avg Delta: {'+' if avg_d > 0 else ''}{avg_d}\n")
                if bingo_signal:
                    f.write(f"{bingo_signal}\n")
                
                f.write("--------------------------------------\n")
        except Exception as e:
            log_msg(f"Text Logging Error: {e}")

    # ── Auto-Trend Logic ──
    def internal_reset(self):
        """Automatically triggers the reset transition."""
        now = time.time()
        
        # Capture current state BEFORE resetting
        snap = self.build_snapshot()
        snap["type"] = "milestone_captured"
        self.milestone_queue.append(snap)
        self.log_milestone(snap)
        
        # Reset phase weight and delta average for next phase
        self.phase_weight = 0.0
        self.phase_delta_sum = 0.0
        self.phase_delta_count = 0

        if self.recording_phase == 1:
            # Lock the DECIDED level
            self.milestone_target = self.current_benchmark_threshold
            log_msg(f"AUTO-MILESTONE LOCKED: {self.milestone_target}s (Time Limit)")
        
        if self.recording_phase < 3:
            self.recording_phase += 1
            self.buy_market_volume = 0
            self.sell_market_volume = 0
            self.tracking_start_time = now
            self.iceberg_depth = {}
            self.stop_depth = {}
            self.level_trend_stats = {}
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
                    self.initial_sell_limits[i] = self.get_raw_limits_in_range(dp + start_offset, dp + end_offset, True)
                    self.initial_buy_limits[i] = self.get_raw_limits_in_range(dp - end_offset, dp - start_offset, False)
            
            self.absolute_phase += 1
            log_msg(f"AUTO-RESET: Entering Phase {self.absolute_phase} (Internal {self.recording_phase})")
        else:
            # Phase 3 Complete -> Auto Restart immediately for continuous tracking
            log_msg("AUTO-TREND CYCLE COMPLETE — Continuing immediately without gaps.")
            self.internal_start(is_auto=True)

    def internal_start(self, is_auto=False):
        """Standardized start logic for both manual and auto-pilot starts."""
        with self.lock:
            self.buy_market_volume = 0
            self.sell_market_volume = 0
            self.milestone_target = self.current_benchmark_threshold
            self.recording_phase = 1
            if not is_auto:
                self.absolute_phase = 1
                self.macro_buy_volumes = []
                self.macro_sell_volumes = []
                self.macro_buy_reloads = []
                self.macro_sell_reloads = []
                self.prev_buy_volume = 0
                self.prev_sell_volume = 0
                self.prev_buy_reload = 0
                self.prev_sell_reload = 0
                self.prev_mkt_buy_vel = 0.0
                self.prev_mkt_sell_vel = 0.0
                self.prev_buy_delta_vel = 0.0
                self.prev_sell_delta_vel = 0.0
                self.prev_low_c = 0
                self.prev_high_c = 0
                self.prev_low_price = 0
                self.prev_high_price = 0
                self.prev_open_price = 0
                self.phase_delta_sum = 0.0
                self.phase_delta_count = 0
                self.cumulative_weight = 0.0
                self.phase_weight = 0.0
            else:
                self.absolute_phase += 1
                
            self.tracking_active = True
            self.tracking_start_time = time.time()
            self.level_trend_stats = {}
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
            
            # Clean up logs
            self.power_scores = {}
            self.liquidity_memory = {}

    def check_auto_trend(self):
        if not self.tracking_active or self.recording_phase == 0 or not self.tracking_start_time:
            return

        elapsed = time.time() - self.tracking_start_time

        # Trigger phase reset when time elapsed hits the target
        if elapsed >= self.milestone_target:
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

        above_targets, below_targets = self.get_target_levels()
        results = []

        for i in range(len(above_targets)):
            target_up = above_targets[i]
            target_down = below_targets[i]
            level_num = i + 1
            dist = self.target_distance * level_num

            # Get current walls (Visible + Hidden) - Now with Anti-Spoofing Ghost Logic
            visible_up, hidden_up, ghost_up = self.get_total_liquidity_at_level(dp, target_up)
            visible_down, hidden_down, ghost_down = self.get_total_liquidity_at_level(dp, target_down)
            
            is_ghosting = ghost_up or ghost_down
            
            curr_up_total = visible_up + hidden_up
            curr_down_total = visible_down + hidden_down
            
            # Liquidity Delta (%)
            up_delta = 0
            down_delta = 0
            if level_num in self.last_up_weights:
                last = self.last_up_weights[level_num]
                if last > 0: up_delta = ((curr_up_total - last) / last) * 100
            if level_num in self.last_down_weights:
                last = self.last_down_weights[level_num]
                if last > 0: down_delta = ((curr_down_total - last) / last) * 100
            
            self.last_up_weights[level_num] = curr_up_total
            self.last_down_weights[level_num] = curr_down_total

            # Cumulative Market Volume
            buy_mkt = self.buy_market_volume
            sell_mkt = self.sell_market_volume

            # Dynamic Absorption Formula: 
            # Prob = Executed / (Executed + Pending)
            # This accounts for cancellations (Pending decreases -> Prob increases)
            # and replenishment (Hidden or Added Pending -> Prob decreases)
            
            total_bullish_denominator = buy_mkt + visible_up + hidden_up
            total_bearish_denominator = sell_mkt + visible_down + hidden_down

            if total_bullish_denominator > 0:
                up_prob = (buy_mkt / total_bullish_denominator) * 100
            else:
                up_prob = 0.0

            if total_bearish_denominator > 0:
                down_prob = (sell_mkt / total_bearish_denominator) * 100
            else:
                down_prob = 0.0

            # Round to 1 decimal place
            up_prob = round(up_prob, 1)
            down_prob = round(down_prob, 1)

            # ── Trend Instance Tracking ──
            if level_num not in self.level_trend_stats:
                self.level_trend_stats[level_num] = {
                    "bull_count": 0, "bear_count": 0,
                    "bull_time": 0, "bear_time": 0,
                    "bull_energy": 0, "bear_energy": 0,
                    "bull_vol": 0, "bear_vol": 0,
                    "bull_net_imb": 0, "bear_net_imb": 0,
                    "last_buy_vol": self.buy_market_volume,
                    "last_sell_vol": self.sell_market_volume,
                    "current_side": None, "current_start": None,
                    "last_energy_calc": None,
                    "session_open_price": dp
                }
            
            stats = self.level_trend_stats[level_num]
            new_side = None
            if up_prob > down_prob: new_side = "bull"
            elif down_prob > up_prob: new_side = "bear"
            
            now = time.time()
            if self.tracking_active:
                # Volume accumulation (Level-specific)
                dv_buy = self.buy_market_volume - stats["last_buy_vol"]
                dv_sell = self.sell_market_volume - stats["last_sell_vol"]
                stats["bull_vol"] += dv_buy
                stats["bear_vol"] += dv_sell
                stats["last_buy_vol"] = self.buy_market_volume
                stats["last_sell_vol"] = self.sell_market_volume

                # Energy and Imbalance accumulation
                if stats["current_side"] and stats["last_energy_calc"]:
                    slice_delta = now - stats["last_energy_calc"]
                    imb_val = (curr_down_total - curr_up_total) * slice_delta * 4.0 # Scale to original 250ms polling rate
                    if stats["current_side"] == "bull":
                        stats["bull_energy"] += (up_prob * slice_delta)
                        stats["bull_net_imb"] += imb_val
                    else:
                        stats["bear_energy"] += (down_prob * slice_delta)
                        stats["bear_net_imb"] += imb_val
                
                # Side transition logic
                if new_side != stats["current_side"]:
                    # End old side if it existed
                    if stats["current_side"] and stats["current_start"]:
                        side_dur = now - stats["current_start"]
                        if stats["current_side"] == "bull":
                            stats["bull_time"] += side_dur
                            stats["bull_count"] += 1
                        else:
                            stats["bear_time"] += side_dur
                            stats["bear_count"] += 1
                    
                        
                    stats["current_side"] = new_side
                    stats["current_start"] = now if new_side else None
                
                stats["last_energy_calc"] = now
            
            # ── Absorption/Exhaustion Logic ──
            price_state = "Neutral"
            absorption_intensity = 0
            cpt = 0
            
            if self.tracking_active and dp is not None and stats["session_open_price"] is not None:
                price_diff = (dp - stats["session_open_price"])
                abs_price_diff = abs(price_diff)
                
                # Contracts per Tick (CpT) - Rolling 10s
                if len(self.rolling_history) >= 2:
                    h_start = self.rolling_history[0]
                    h_end = self.rolling_history[-1]
                    v_delta = (h_end[2] - h_start[2]) + (h_end[3] - h_start[3])
                    p_delta = abs(h_end[1] - h_start[1])
                    cpt = round(v_delta / (p_delta + 0.1), 1)

                # Absorption Intensity (0-100)
                # High Volume + Low Price Change = High Intensity
                if abs_price_diff < 3: # Only for stalled price
                    vol_weight = min((stats["bull_vol"] + stats["bear_vol"]) / 200, 1.0)
                    absorption_intensity = round(vol_weight * (100 / (abs_price_diff + 1)), 1)

                # State Labeling
                if down_prob > 70 and price_diff >= -1:
                    price_state = "Bull Absorption"
                elif up_prob > 70 and price_diff <= 1:
                    price_state = "Bear Absorption"
                elif max(up_prob, down_prob) < 30 and abs(price_diff) > 5:
                    price_state = "High Momentum"

            live_bull_time = stats["bull_time"]
            live_bear_time = stats["bear_time"]
            if self.tracking_active and stats["current_side"] == "bull" and stats["current_start"]:
                live_bull_time += (now - stats["current_start"])
            elif self.tracking_active and stats["current_side"] == "bear" and stats["current_start"]:
                live_bear_time += (now - stats["current_start"])

            power_score = self.power_scores.get(level_num, 0)

            results.append({
                "level": level_num,
                "distance": dist,
                "target_up": self.tick_to_display(target_up),
                "target_down": self.tick_to_display(target_down),
                "sell_limits": int(visible_up + hidden_up),
                "buy_limits": int(visible_down + hidden_down),
                "up_probability": up_prob,
                "down_probability": down_prob,
                "bull_trends": stats["bull_count"] + (1 if stats["current_side"] == "bull" else 0),
                "bear_trends": stats["bear_count"] + (1 if stats["current_side"] == "bear" else 0),
                "bull_time_total": round(live_bull_time, 1),
                "bear_time_total": round(live_bear_time, 1),
                "bull_energy": round(stats["bull_energy"], 1),
                "bear_energy": round(stats["bear_energy"], 1),
                "bull_vol": stats["bull_vol"],
                "bear_vol": stats["bear_vol"],
                "bull_net_imb": int(stats["bull_net_imb"]),
                "bear_net_imb": int(stats["bear_net_imb"]),
                "market_state": price_state,
                "power_score": power_score,
                "absorption_intensity": absorption_intensity,
                "cpt": cpt,
                "up_delta": round(up_delta, 1),
                "down_delta": round(down_delta, 1),
                "net_imbalance": int(curr_down_total - curr_up_total),
                "is_ghosting": is_ghosting
            })

        return results

    # ── Build full snapshot for dashboard ──
    def build_snapshot(self):
        with self.lock:
            dp = self.get_driving_price()
            
            # Accumulate weight at the same 0.25s cadence as the dashboard
            self._accumulate_weight(dp)
            
            # Update Session Range
            if self.tracking_active and dp is not None:
                now_t = time.time()
                if self.session_high_price is None or dp > self.session_high_price:
                    self.session_high_price = dp
                    self.session_high_time = now_t
                if self.session_low_price is None or dp < self.session_low_price:
                    self.session_low_price = dp
                    self.session_low_time = now_t

            elapsed = 0
            if self.tracking_start_time:
                elapsed = round(time.time() - self.tracking_start_time, 1)

            # Delta Liquidity Status (Multi-Level)
            levels_data = []
            if self.session_anchor_price is not None:
                for i in range(1, self.num_levels + 1):
                    start_offset = self.delta_target_distance * (i - 1)
                    end_offset = self.delta_target_distance * i
                    
                    curr_sell = self.get_raw_limits_in_range(self.session_anchor_price + start_offset, self.session_anchor_price + end_offset, True)
                    curr_buy = self.get_raw_limits_in_range(self.session_anchor_price - end_offset, self.session_anchor_price - start_offset, False)
                    
                    init_sell = self.initial_sell_limits.get(i, 0)
                    init_buy = self.initial_buy_limits.get(i, 0)
                    
                    delta_sell = curr_sell - init_sell
                    delta_buy = curr_buy - init_buy
                    
                    levels_data.append({
                        "level": i,
                        "start_offset": start_offset,
                        "end_offset": end_offset,
                        "initial_sell_limits": init_sell,
                        "current_sell_limits": curr_sell,
                        "delta_sell": delta_sell,
                        "initial_buy_limits": init_buy,
                        "current_buy_limits": curr_buy,
                        "delta_buy": delta_buy,
                    })

            # Calculate floating live probabilities
            live_probs = []
            try:
                td = getattr(self, 'delta_target_distance', 60)
                if dp is not None:
                    visible_up = self.get_raw_limits_in_range(dp, dp + td, True)
                    visible_down = self.get_raw_limits_in_range(dp - td, dp, False)
                    log_msg(f"DEBUG: dp={dp}, td={td}, up={visible_up}, down={visible_down}")
                    live_probs.append({
                        "sell_limits": visible_up,
                        "buy_limits": visible_down,
                        "net_imbalance": visible_down - visible_up
                    })
                else:
                    pass
                    # log_msg("dp is None, skipping floating limits")
            except Exception as e:
                log_msg(f"ERROR calculating live_probs: {e}")

            return {
                "type": "snapshot",
                "timestamp": time.time(),
                "driving_price": self.tick_to_display(dp),
                "best_bid": self.tick_to_display(self.best_bid),
                "best_ask": self.tick_to_display(self.best_ask),
                "buy_market_volume": self.buy_market_volume,
                "sell_market_volume": self.sell_market_volume,
                "tracking_active": self.tracking_active,
                "tracking_elapsed": elapsed,
                
                # Session Range Info
                "session_anchor": self.tick_to_display(self.session_anchor_price) if self.session_anchor_price else None,
                "session_high": self.tick_to_display(self.session_high_price) if self.session_high_price else None,
                "session_low": self.tick_to_display(self.session_low_price) if self.session_low_price else None,
                "session_high_time": self.session_high_time,
                "session_low_time": self.session_low_time,
                
                "recording_phase": self.recording_phase,
                "absolute_phase": self.absolute_phase,
                "milestone_target": self.milestone_target,
                
                # Imbalance weight accumulators (mirrored from dashboard)
                "cumulative_weight": round(self.cumulative_weight),
                "phase_weight": round(self.phase_weight),
                "phase_avg_delta": round(self.phase_delta_sum / self.phase_delta_count, 1) if self.phase_delta_count > 0 else 0,
                
                "levels": levels_data,
                "live_probabilities": live_probs
            }

    # ── Bookmap callbacks ──
    def on_subscribe(self, alias, pips, size_multiplier, order_book):
        with self.lock:
            self.alias_to_order_book[alias] = order_book
            self.pips = pips
            self.size_multiplier = size_multiplier
            log_msg(f"Subscribed to {alias}, pips={pips}, size_multiplier={size_multiplier}")

    def update_decay_rate(self):
        """Calculates market velocity and adjusts memory duration."""
        now = time.time()
        self.tick_history = [t for t in self.tick_history if now - t < 5.0]
        tps = len(self.tick_history) / 5.0 # Ticks Per Second
        
        if tps > 20: self.dynamic_decay_period = 0.5   # High Vol
        elif tps < 5: self.dynamic_decay_period = 5.0  # Low Vol
        else: self.dynamic_decay_period = 3.0           # Normal
        
    def kill_ghost_at_price(self, price):
        """Instantly deletes memory if price trades through it (The Kill Switch)."""
        if price in self.liquidity_memory:
            del self.liquidity_memory[price]
            # log_msg(f"BREACH KILL SWITCH: Cleared ghost at {self.tick_to_display(price)}")

    def on_trade(self, alias, is_buy_aggressor, price, size, is_otc=None):
        # Skip zero-size trades
        if size <= 0:
            return
        with self.lock:
            # Convert Bookmap's fractional tick price to integer tick level
            tick_price = int(round(price))
            # Convert Bookmap's internal size to real contract volume
            real_size = size / self.size_multiplier if hasattr(self, 'size_multiplier') and self.size_multiplier else size

            # ── Kill Switch ──
            self.kill_ghost_at_price(tick_price)
            
            self.last_trade_price = tick_price
            if is_buy_aggressor: self.best_ask = tick_price
            else: self.best_bid = tick_price

            # Velocity tracking
            if tick_price != self.last_price_for_velocity:
                self.tick_history.append(time.time())
                self.last_price_for_velocity = tick_price

            if self.tracking_active:
                if is_buy_aggressor:
                    # Detect hidden ask replenishment (Iceberg) - market BUY hits ASK
                    visible = self.ask_depth.get(tick_price, 0)
                    if real_size > visible and visible > 0:
                        hidden_hit = real_size - visible
                        self.iceberg_depth[tick_price] = self.iceberg_depth.get(tick_price, 0) + hidden_hit
                        log_queue.put((r"C:\Bookmap\Logs\micro_icebergs.log", f"[{time.strftime('%H:%M:%S')}] UNDERFLOW ASK (Resistance Iceberg) at {self.tick_to_display(tick_price)} | Trade Size: {real_size} | Visible Book: {visible} | Hidden Size: {hidden_hit}\n"))
                    self.buy_market_volume += real_size
                else:
                    # Detect hidden bid replenishment (Iceberg) - market SELL hits BID
                    visible = self.bid_depth.get(tick_price, 0)
                    if real_size > visible and visible > 0:
                        hidden_hit = real_size - visible
                        self.iceberg_depth[tick_price] = self.iceberg_depth.get(tick_price, 0) + hidden_hit
                        log_queue.put((r"C:\Bookmap\Logs\micro_icebergs.log", f"[{time.strftime('%H:%M:%S')}] UNDERFLOW BID (Support Iceberg) at {self.tick_to_display(tick_price)} | Trade Size: {real_size} | Visible Book: {visible} | Hidden Size: {hidden_hit}\n"))
                    self.sell_market_volume += real_size

    def on_depth(self, alias, is_bid, price, size):
        with self.lock:
            # Convert internal size to real limit size
            real_size = size / self.size_multiplier if hasattr(self, 'size_multiplier') and self.size_multiplier else size
            
            # ── Kill Switch ──
            self.kill_ghost_at_price(price)
            
            target_dict = self.bid_depth if is_bid else self.ask_depth
            if real_size > 0:
                target_dict[price] = real_size
                # ── Liquidity Memory (High Water Mark) ──
                mem = self.liquidity_memory.get(price)
                if not mem or real_size > mem['size']:
                    self.liquidity_memory[price] = {
                        'size': real_size, 
                        'time': time.time(),
                        'side': 'bid' if is_bid else 'ask'
                    }
            else:
                target_dict.pop(price, None)
            
            # Update best bid/ask
            if self.bid_depth: self.best_bid = max(self.bid_depth.keys())
            if self.ask_depth: self.best_ask = min(self.ask_depth.keys())
        
        self.check_auto_trend()
        
        # Periodic cleanup of expired memory
        if random.random() < 0.01:
            self.update_decay_rate()
            now = time.time()
            expired = [p for p, m in self.liquidity_memory.items() if now - m['time'] > self.dynamic_decay_period * 2]
            for p in expired: del self.liquidity_memory[p]


    def on_iceberg(self, alias, is_bid, price, size):
        with self.lock:
            real_size = size / self.size_multiplier if hasattr(self, 'size_multiplier') and self.size_multiplier else size
            if self.tracking_active:
                # Add to the current hidden pool
                self.iceberg_depth[price] = self.iceberg_depth.get(price, 0) + real_size
                log_msg(f"ICEBERG DETECTED at {self.tick_to_display(price)}: {real_size}")

    def on_stop(self, alias, is_bid, price, size):
        with self.lock:
            real_size = size / self.size_multiplier if hasattr(self, 'size_multiplier') and self.size_multiplier else size
            if self.tracking_active:
                # Add to the hidden stop pool
                self.stop_depth[price] = self.stop_depth.get(price, 0) + real_size
                log_msg(f"STOP ORDER DETECTED at {self.tick_to_display(price)}: {real_size}")

    def on_mbo(self, alias, event_type, order_id, price, size):
        # MBO events help refine the order book, but for absorption logic, 
        # we mainly use it as a trigger to ensure we are seeing all updates.
        pass


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
                    log_msg(f"Settings updated: {data}")
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
                # ── 1. Check for Auto-Captured Milestones (High Priority) ──
                to_send = []
                with engine.lock:
                    if engine.milestone_queue:
                        to_send = list(engine.milestone_queue)
                        engine.milestone_queue.clear()
                
                for captured_snap in to_send:
                    msg = json.dumps(captured_snap)
                    for ws in engine.ws_clients.copy():
                        try: await ws.send(msg)
                        except: pass

                # ── 2. Send Regular Live Snapshot ──
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
    order_book = bm.create_order_book()
    engine.on_subscribe(alias, pips, size_multiplier, order_book)
    bm.subscribe_to_depth(addon, alias, 1)
    bm.subscribe_to_trades(addon, alias, 2)
    # Subscribe to MBO feed for hidden order tracking
    try:
        bm.subscribe_to_mbo(addon, alias, 3)
    except AttributeError:
        pass

def handle_unsubscribe_instrument(addon, alias):
    log_msg(f"Unsubscribed from {alias}")

def handle_trade(addon, alias, price, size, *args):
    if size <= 0:  # Skip zero-size events
        return
    is_otc = bool(args[0]) if len(args) > 0 else False
    is_buy_aggressor = bool(args[1]) if len(args) > 1 else False
    
    
    
    # Optional: Skip OTC trades
    # if is_otc:
    #     return
    
    engine.on_trade(alias, is_buy_aggressor, price, size)

def handle_depth_info(addon, alias, is_bid, price, size):
    engine.on_depth(alias, is_bid, price, size)

def handle_iceberg(addon, alias, is_bid, price, size):
    engine.on_iceberg(alias, is_bid, price, size)

def handle_stop(addon, alias, is_bid, price, size):
    engine.on_stop(alias, is_bid, price, size)

def handle_mbo(addon, alias, event_type, order_id, price, size):
    engine.on_mbo(alias, event_type, order_id, price, size)


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
    
    # Add hidden data handlers (using try-except for API compatibility)
    try:
        bm.add_iceberg_handler(addon, handle_iceberg)
    except AttributeError:
        log_msg("Iceberg handler not supported in this API version")
    
    try:
        bm.add_stop_handler(addon, handle_stop)
    except AttributeError:
        log_msg("Stop order handler not supported in this API version")
        
    try:
        bm.add_mbo_handler(addon, handle_mbo)
    except AttributeError:
        log_msg("MBO handler not supported in this API version")

    bm.start_addon(addon, handle_subscribe_instrument, handle_unsubscribe_instrument)
    bm.wait_until_addon_is_turned_off(addon)
