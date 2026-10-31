#!/usr/bin/env python3
"""Minimal Rithmic Protocol API login and market-data verification test."""

import argparse
import asyncio
import getpass
import os
import pathlib
import ssl
import sys

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


DEFAULT_URI = "wss://rituz00100.rithmic.com:443"
DEFAULT_SDK_ROOT = pathlib.Path(__file__).resolve().parent / "0.90.0.0"
ORDER_BOOK_TEMPLATE_ID = 156
TEMPLATE_NAMES = {
    11: "login response",
    13: "logout response",
    17: "system info response",
    19: "heartbeat response",
    101: "market-data subscription response",
    21: "system gateway info response",
    150: "last trade",
    151: "best bid/offer",
    ORDER_BOOK_TEMPLATE_ID: "order book",
}
UPDATE_TYPES = {
    1: "CLEAR_ORDER_BOOK",
    2: "NO_BOOK",
    3: "SNAPSHOT_IMAGE",
    4: "BEGIN",
    5: "MIDDLE",
    6: "END",
    7: "SOLO",
}


def load_sdk_modules(sdk_root):
    sample_dir = sdk_root / "samples" / "samples.py"
    if not sample_dir.is_dir():
        raise FileNotFoundError(f"Python SDK sample directory not found: {sample_dir}")
    sys.path.insert(0, str(sample_dir))

    import base_pb2
    import best_bid_offer_pb2
    import last_trade_pb2
    import request_heartbeat_pb2
    import request_login_pb2
    import request_logout_pb2
    import request_market_data_update_pb2
    import request_rithmic_system_info_pb2
    import response_login_pb2
    import response_market_data_update_pb2
    import response_rithmic_system_info_pb2

    return {
        "base": base_pb2,
        "bbo": best_bid_offer_pb2,
        "last_trade": last_trade_pb2,
        "heartbeat_request": request_heartbeat_pb2,
        "login_request": request_login_pb2,
        "logout_request": request_logout_pb2,
        "market_data_request": request_market_data_update_pb2,
        "system_info_request": request_rithmic_system_info_pb2,
        "login_response": response_login_pb2,
        "market_data_response": response_market_data_update_pb2,
        "system_info_response": response_rithmic_system_info_pb2,
        "order_book": create_order_book_message_class(),
    }


def create_order_book_message_class():
    """Build the message class from the SDK schema, which has no Python binding."""
    file_descriptor = descriptor_pb2.FileDescriptorProto()
    file_descriptor.name = "rithmic_test_order_book.proto"
    file_descriptor.package = "rti_test"
    message = file_descriptor.message_type.add()
    message.name = "OrderBook"

    update_type = message.enum_type.add()
    update_type.name = "UpdateType"
    for number, name in UPDATE_TYPES.items():
        value = update_type.value.add()
        value.name = name
        value.number = number

    optional = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    required = descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED
    repeated = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
    types = descriptor_pb2.FieldDescriptorProto

    def add_field(name, number, field_type, label=optional, type_name=None):
        field = message.field.add()
        field.name = name
        field.number = number
        field.type = field_type
        field.label = label
        if type_name:
            field.type_name = type_name

    add_field("template_id", 154467, types.TYPE_INT32, required)
    add_field("symbol", 110100, types.TYPE_STRING)
    add_field("exchange", 110101, types.TYPE_STRING)
    add_field("presence_bits", 149138, types.TYPE_UINT32)
    add_field(
        "update_type",
        157608,
        types.TYPE_ENUM,
        type_name=".rti_test.OrderBook.UpdateType",
    )
    for name, number in (
        ("bid_price", 154282),
        ("ask_price", 154284),
    ):
        add_field(name, number, types.TYPE_DOUBLE, repeated)
    for name, number in (
        ("bid_size", 154283),
        ("bid_orders", 154401),
        ("impl_bid_size", 154412),
        ("ask_size", 154285),
        ("ask_orders", 154402),
        ("impl_ask_size", 154415),
        ("ssboe", 150100),
        ("usecs", 150101),
    ):
        add_field(name, number, types.TYPE_INT32, repeated if name not in ("ssboe", "usecs") else optional)
    for name, number in (
        ("bid_size_64", 159004),
        ("impl_bid_size_64", 159005),
        ("ask_size_64", 159006),
        ("impl_ask_size_64", 159007),
    ):
        add_field(name, number, types.TYPE_UINT64, repeated)

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_descriptor)
    descriptor = pool.FindMessageTypeByName("rti_test.OrderBook")
    return message_factory.MessageFactory(pool).GetPrototype(descriptor)


def create_gateway_info_message_classes():
    file_descriptor = descriptor_pb2.FileDescriptorProto()
    file_descriptor.name = "rithmic_test_system_gateway_info.proto"
    file_descriptor.package = "rti_test"
    types = descriptor_pb2.FieldDescriptorProto

    def add_message(name, fields):
        message = file_descriptor.message_type.add()
        message.name = name
        for field_name, number, field_type, label in fields:
            field = message.field.add()
            field.name = field_name
            field.number = number
            field.type = field_type
            field.label = label

    optional = types.LABEL_OPTIONAL
    required = types.LABEL_REQUIRED
    repeated = types.LABEL_REPEATED
    add_message(
        "RequestRithmicSystemGatewayInfo",
        [
            ("template_id", 154467, types.TYPE_INT32, required),
            ("user_msg", 132760, types.TYPE_STRING, repeated),
            ("system_name", 153628, types.TYPE_STRING, optional),
        ],
    )
    add_message(
        "ResponseRithmicSystemGatewayInfo",
        [
            ("template_id", 154467, types.TYPE_INT32, required),
            ("user_msg", 132760, types.TYPE_STRING, repeated),
            ("rp_code", 132766, types.TYPE_STRING, repeated),
            ("system_name", 153628, types.TYPE_STRING, optional),
            ("gateway_name", 153640, types.TYPE_STRING, repeated),
            ("gateway_uri", 153641, types.TYPE_STRING, repeated),
        ],
    )

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_descriptor)
    factory = message_factory.MessageFactory(pool)
    return (
        factory.GetPrototype(
            pool.FindMessageTypeByName("rti_test.RequestRithmicSystemGatewayInfo")
        ),
        factory.GetPrototype(
            pool.FindMessageTypeByName("rti_test.ResponseRithmicSystemGatewayInfo")
        ),
    )


def make_ssl_context(sdk_root):
    ca_file = sdk_root / "etc" / "rithmic_ssl_cert_auth_params"
    if not ca_file.is_file():
        raise FileNotFoundError(f"SDK TLS trust bundle not found: {ca_file}")
    return ssl.create_default_context(cafile=str(ca_file))


async def connect(uri, sdk_root):
    return await websockets.connect(
        uri,
        ssl=make_ssl_context(sdk_root) if uri.lower().startswith("wss://") else None,
        ping_interval=None,
        open_timeout=15,
        close_timeout=5,
    )


def decode_message(modules, payload):
    base = modules["base"].Base()
    base.ParseFromString(payload)
    template_id = base.template_id
    name = TEMPLATE_NAMES.get(template_id, "unknown template")

    if template_id == 11:
        message = modules["login_response"].ResponseLogin()
    elif template_id == 17:
        message = modules["system_info_response"].ResponseRithmicSystemInfo()
    elif template_id == 101:
        message = modules["market_data_response"].ResponseMarketDataUpdate()
    elif template_id == 150:
        message = modules["last_trade"].LastTrade()
    elif template_id == 151:
        message = modules["bbo"].BestBidOffer()
    elif template_id == ORDER_BOOK_TEMPLATE_ID:
        message = modules["order_book"]()
    elif template_id in (13, 19):
        return template_id, name, None
    else:
        return template_id, name, None

    message.ParseFromString(payload)
    return template_id, name, message


async def discover_systems(uri, sdk_root, modules):
    print(f"WebSocket connection to {uri} ...", flush=True)
    async with await connect(uri, sdk_root) as ws:
        print("WebSocket connection: SUCCESS", flush=True)
        request = modules["system_info_request"].RequestRithmicSystemInfo()
        request.template_id = 16
        request.user_msg.append("connection-test")
        await ws.send(request.SerializeToString())
        payload = await asyncio.wait_for(ws.recv(), timeout=15)
        template_id, _, response = decode_message(modules, payload)
        if template_id != 17:
            raise RuntimeError(
                f"Expected system-info response template 17; received "
                f"{template_id} ({TEMPLATE_NAMES.get(template_id, 'unknown')})."
            )
        print(f"System discovery response code: {list(response.rp_code)}")
        print(f"Systems: {list(response.system_name)}")
        print(
            "This unauthenticated discovery response does not establish which "
            "system or gateway is enabled for your Paper Trading account."
        )


async def discover_gateways(uri, sdk_root, system_name):
    request_type, response_type = create_gateway_info_message_classes()
    print(f"WebSocket connection to {uri} ...", flush=True)
    async with await connect(uri, sdk_root) as ws:
        print("WebSocket connection: SUCCESS", flush=True)
        request = request_type()
        request.template_id = 20
        request.user_msg.append("connection-test")
        request.system_name = system_name
        await ws.send(request.SerializeToString())
        payload = await asyncio.wait_for(ws.recv(), timeout=15)
        base = load_sdk_modules_base_only()
        template_id = base.Base()
        template_id.ParseFromString(payload)
        if template_id.template_id != 21:
            raise RuntimeError(
                f"Expected system-gateway response template 21; received "
                f"{template_id.template_id} "
                f"({TEMPLATE_NAMES.get(template_id.template_id, 'unknown')})."
            )
        response = response_type()
        response.ParseFromString(payload)
        print(f"Gateway response code: {list(response.rp_code)}")
        print(f"System: {response.system_name}")
        print("Gateways:")
        for name, gateway_uri in zip(response.gateway_name, response.gateway_uri):
            print(f"  {name}: {gateway_uri}")
        if not response.gateway_uri:
            print("No gateway URI was returned for this system.")


def load_sdk_modules_base_only():
    import base_pb2

    return base_pb2


async def login(ws, modules, system_name, username, password):
    request = modules["login_request"].RequestLogin()
    request.template_id = 10
    request.template_version = "5.55"
    request.user_msg.append("hello")
    request.user = username
    request.password = password
    request.app_name = "SampleMD.py"
    request.app_version = "0.3.0.0"
    request.system_name = system_name
    request.infra_type = modules["login_request"].RequestLogin.SysInfraType.TICKER_PLANT
    await ws.send(request.SerializeToString())

    payload = await asyncio.wait_for(ws.recv(), timeout=20)
    template_id, _, response = decode_message(modules, payload)
    if template_id != 11:
        raise RuntimeError(
            f"Expected login response template 11; received "
            f"{template_id} ({TEMPLATE_NAMES.get(template_id, 'unknown')})."
        )

    codes = list(response.rp_code)
    print(f"Authentication response code: {codes}")
    if response.user_msg:
        print(f"Authentication response message: {list(response.user_msg)}")
    authenticated = codes == ["0"]
    print(f"Authentication: {'SUCCESS' if authenticated else 'FAILED'}")
    return authenticated, response.heartbeat_interval or 5.0


def market_data_update_bits(modules, depth):
    request_type = modules["market_data_request"].RequestMarketDataUpdate
    bits = request_type.UpdateBits.LAST_TRADE | request_type.UpdateBits.BBO
    if depth == "level2":
        bits |= request_type.UpdateBits.ORDER_BOOK
    return bits


async def subscribe(ws, modules, exchange, symbol, depth):
    request_type = modules["market_data_request"].RequestMarketDataUpdate
    request = request_type()
    request.template_id = 100
    correlation_id = f"connection-test:{symbol}"
    request.user_msg.append(correlation_id)
    request.symbol = symbol
    request.exchange = exchange
    request.request = request_type.Request.SUBSCRIBE
    request.update_bits = market_data_update_bits(modules, depth)
    await ws.send(request.SerializeToString())
    return correlation_id


def print_decoded(template_id, message):
    if message is None:
        return
    print(f"\n{TEMPLATE_NAMES.get(template_id, 'message')} ({template_id}):")
    print(message)


async def listen(
    ws, modules, duration, heartbeat_interval, exchange, symbols, depth, correlation_ids
):
    results = {
        symbol: {
            "ack": None,
            "bbo": 0,
            "last_trade": 0,
            "order_book": 0,
            "bid_prices": set(),
            "ask_prices": set(),
            "max_bid_levels": 0,
            "max_ask_levels": 0,
        }
        for symbol in symbols
    }
    token_to_symbol = {
        token: symbol for symbol, token in correlation_ids.items()
    }
    deadline = asyncio.get_running_loop().time() + duration
    connection_closed = None
    ended_early = None

    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            payload = await asyncio.wait_for(
                ws.recv(), timeout=min(remaining, max(1.0, heartbeat_interval))
            )
        except asyncio.TimeoutError:
            request = modules["heartbeat_request"].RequestHeartbeat()
            request.template_id = 18
            try:
                await ws.send(request.SerializeToString())
            except ConnectionClosed as error:
                connection_closed = error
                print(
                    f"\nWebSocket closed while sending heartbeat "
                    f"(code={error.code}, reason={error.reason!r})."
                )
                break
            continue
        except ConnectionClosed as error:
            connection_closed = error
            print(
                f"\nWebSocket closed during data collection "
                f"(code={error.code}, reason={error.reason!r})."
            )
            break

        template_id, _, message = decode_message(modules, payload)
        if template_id in (150, 151, ORDER_BOOK_TEMPLATE_ID):
            received_symbol = getattr(message, "symbol", "")
            received_exchange = getattr(message, "exchange", "")
            if received_symbol not in results:
                continue
            if received_exchange and received_exchange != exchange:
                continue
            result = results[received_symbol]
            result_key = {
                150: "last_trade",
                151: "bbo",
                ORDER_BOOK_TEMPLATE_ID: "order_book",
            }[template_id]
            result[result_key] += 1
            print_decoded(template_id, message)

            if template_id == ORDER_BOOK_TEMPLATE_ID:
                bid_prices = set(message.bid_price)
                ask_prices = set(message.ask_price)
                result["bid_prices"].update(bid_prices)
                result["ask_prices"].update(ask_prices)
                result["max_bid_levels"] = max(
                    result["max_bid_levels"], len(bid_prices)
                )
                result["max_ask_levels"] = max(
                    result["max_ask_levels"], len(ask_prices)
                )
        elif template_id == 101:
            codes = list(message.rp_code)
            echoed_tokens = list(message.user_msg)
            symbol = next(
                (token_to_symbol[token] for token in echoed_tokens if token in token_to_symbol),
                None,
            )
            print(f"\nMarket-data subscription response (101):")
            print(f"  correlation: {symbol or 'unmatched'}")
            print(f"  response code: {codes}")
            print(f"  message: {echoed_tokens}")
            print(f"  status: {'SUCCESS' if codes == ['0'] else 'FAILED'}")
            if symbol is not None:
                results[symbol]["ack"] = {
                    "codes": codes,
                    "messages": echoed_tokens,
                }
        elif template_id == 19:
            continue
        elif template_id == 13:
            print("\nReceived logout response.")
            ended_early = "server sent logout response"
            break
        else:
            print_decoded(template_id, message)

    print("\nPer-symbol results:")
    for symbol, result in results.items():
        ack = result["ack"]
        if ack is None:
            ack_status = "NOT RECEIVED"
        else:
            ack_status = "SUCCESS" if ack["codes"] == ["0"] else "FAILED"
        print(f"  {symbol}:")
        print(f"    subscription acknowledgement: {ack_status}")
        if ack is not None and ack_status == "FAILED":
            print(f"    subscription response: {ack['codes']} {ack['messages']}")
        print(f"    BBO updates: {result['bbo']}")
        print(f"    last-trade updates: {result['last_trade']}")
        if depth == "level2":
            print(f"    order-book updates: {result['order_book']}")
            print(
                "    distinct depth prices: "
                f"{len(result['bid_prices'])} bid / "
                f"{len(result['ask_prices'])} ask"
            )
            print(
                "    maximum levels in one message: "
                f"{result['max_bid_levels']} bid / "
                f"{result['max_ask_levels']} ask"
            )
    if connection_closed is None:
        if ended_early:
            print(f"Connection status: test ended early because the {ended_early}.")
        else:
            print(f"Connection status: remained open for the {duration:g}s test.")
    else:
        print(
            "Connection status: closed before test completion "
            f"(code={connection_closed.code}, reason={connection_closed.reason!r})."
        )


async def run_market_data(args, modules):
    username = os.environ.get("RITHMIC_USER") or input("Rithmic username: ")
    password = os.environ.get("RITHMIC_PASSWORD") or getpass.getpass(
        "Rithmic password: "
    )

    print(f"WebSocket connection to {args.uri} ...", flush=True)
    async with await connect(args.uri, args.sdk_root) as ws:
        print("WebSocket connection: SUCCESS", flush=True)
        authenticated = False
        subscribed = False
        try:
            authenticated, heartbeat_interval = await login(
                ws, modules, args.system, username, password
            )
            if not authenticated:
                print(
                    "No market-data request was sent because the Protocol API "
                    "login did not succeed."
                )
                return

            symbols = args.symbol
            print(
                f"Requested market-data tier: {args.depth.upper()} "
                f"for {len(symbols)} symbol(s): {', '.join(symbols)}"
            )
            correlation_ids = {}
            for symbol in symbols:
                try:
                    correlation_ids[symbol] = await subscribe(
                        ws, modules, args.exchange, symbol, args.depth
                    )
                except ConnectionClosed as error:
                    print(
                        f"WebSocket closed while subscribing to {symbol} "
                        f"(code={error.code}, reason={error.reason!r})."
                    )
                    break
                subscribed = True
                print(f"Subscription request sent: {symbol} / {args.exchange}")
            await listen(
                ws,
                modules,
                args.seconds,
                heartbeat_interval,
                args.exchange,
                symbols,
                args.depth,
                correlation_ids,
            )
        finally:
            if authenticated:
                try:
                    if subscribed:
                        request_type = modules["market_data_request"].RequestMarketDataUpdate
                        for symbol in args.symbol:
                            unsubscribe = request_type()
                            unsubscribe.template_id = 100
                            unsubscribe.symbol = symbol
                            unsubscribe.exchange = args.exchange
                            unsubscribe.request = request_type.Request.UNSUBSCRIBE
                            unsubscribe.update_bits = market_data_update_bits(
                                modules, args.depth
                            )
                            await ws.send(unsubscribe.SerializeToString())

                    logout = modules["logout_request"].RequestLogout()
                    logout.template_id = 12
                    await ws.send(logout.SerializeToString())
                except ConnectionClosed:
                    pass


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=os.environ.get("RITHMIC_URI", DEFAULT_URI))
    parser.add_argument("--sdk-root", type=pathlib.Path, default=DEFAULT_SDK_ROOT)
    parser.add_argument("--discover", action="store_true", help="List system names without logging in.")
    parser.add_argument(
        "--discover-gateways",
        action="store_true",
        help="List gateways for --system without logging in.",
    )
    parser.add_argument("--system", help="Exact Rithmic system name from system discovery.")
    parser.add_argument("--exchange", help="Rithmic exchange code, e.g. COMEX.")
    parser.add_argument(
        "--symbol",
        action="append",
        help="Exact Rithmic contract symbol; repeat to subscribe to multiple instruments.",
    )
    parser.add_argument(
        "--depth",
        choices=("level1", "level2"),
        default="level1",
        help="Market-data tier to request; Level 1 is the safe default.",
    )
    parser.add_argument("--seconds", type=float, default=30, help="Market-data listen duration.")
    args = parser.parse_args()
    if args.discover and args.discover_gateways:
        parser.error("--discover and --discover-gateways cannot be combined")
    if args.discover_gateways and not args.system:
        parser.error("--discover-gateways requires --system")
    if not args.discover and not args.discover_gateways:
        missing = [
            option
            for option in ("system", "exchange")
            if not getattr(args, option)
        ]
        if not args.symbol:
            missing.append("symbol")
        if missing:
            parser.error("market-data mode requires --system, --exchange, and --symbol")
        if len(set(args.symbol)) != len(args.symbol):
            parser.error("--symbol values must be unique")
    if args.seconds <= 0:
        parser.error("--seconds must be greater than zero")
    return args


async def async_main(args):
    if args.discover:
        modules = load_sdk_modules(args.sdk_root)
        await discover_systems(args.uri, args.sdk_root, modules)
    elif args.discover_gateways:
        sample_dir = args.sdk_root / "samples" / "samples.py"
        if not sample_dir.is_dir():
            raise FileNotFoundError(f"Python SDK sample directory not found: {sample_dir}")
        sys.path.insert(0, str(sample_dir))
        await discover_gateways(args.uri, args.sdk_root, args.system)
    else:
        modules = load_sdk_modules(args.sdk_root)
        await run_market_data(args, modules)


def main():
    args = parse_args()
    try:
        asyncio.run(async_main(args))
    except (OSError, ValueError, RuntimeError, WebSocketException) as error:
        print(f"Connection test failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nConnection test interrupted.")
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
