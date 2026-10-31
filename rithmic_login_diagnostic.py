#!/usr/bin/env python3
"""Test Rithmic Protocol API WebSocket and Ticker Plant login only."""

import argparse
import asyncio
import getpass
import os
import pathlib
import ssl
import sys

import websockets
from websockets.exceptions import WebSocketException


SDK_ROOT = pathlib.Path(__file__).resolve().parent / "0.90.0.0"
DEFAULT_URI = "wss://rprotocol-mobile.rithmic.com:443"
DEFAULT_SYSTEM = "Rithmic Paper Trading"


def load_sdk(sdk_root):
    sample_dir = sdk_root / "samples" / "samples.py"
    if not sample_dir.is_dir():
        raise FileNotFoundError(f"Rithmic Python SDK not found: {sample_dir}")
    sys.path.insert(0, str(sample_dir))

    import base_pb2
    import request_login_pb2
    import response_login_pb2

    return base_pb2, request_login_pb2, response_login_pb2


def tls_context(sdk_root):
    ca_file = sdk_root / "etc" / "rithmic_ssl_cert_auth_params"
    if not ca_file.is_file():
        raise FileNotFoundError(f"Rithmic SDK trust bundle not found: {ca_file}")
    return ssl.create_default_context(cafile=str(ca_file))


async def run(args):
    base_pb2, request_login_pb2, response_login_pb2 = load_sdk(args.sdk_root)
    username = os.environ.get("RITHMIC_USER") or input("Rithmic username: ")
    password = os.environ.get("RITHMIC_PASSWORD") or getpass.getpass(
        "Rithmic password: "
    )

    print(f"Connecting to {args.uri} ...", flush=True)
    async with websockets.connect(
        args.uri,
        ssl=tls_context(args.sdk_root),
        ping_interval=None,
        open_timeout=15,
        close_timeout=5,
    ) as ws:
        print("WebSocket/TLS connection: SUCCESS")

        request = request_login_pb2.RequestLogin()
        request.template_id = 10
        request.template_version = "5.55"
        request.user_msg.append("hello")
        request.user = username
        request.password = password
        request.app_name = "SampleMD.py"
        request.app_version = "0.3.0.0"
        request.system_name = args.system
        request.infra_type = request_login_pb2.RequestLogin.SysInfraType.TICKER_PLANT

        print("Sending login request:")
        print("  system: Rithmic Paper Trading" if args.system == DEFAULT_SYSTEM else f"  system: {args.system}")
        print("  infrastructure: TICKER_PLANT (market-data login)")
        print("  client metadata: SampleMD.py 0.3.0.0 (matches SDK sample)")
        print("  protocol template version: 5.55 (matches SDK SampleMD.py)")
        print("  market-data subscription: NOT SENT")
        await ws.send(request.SerializeToString())

        try:
            payload = await asyncio.wait_for(ws.recv(), timeout=args.timeout)
        except asyncio.TimeoutError as error:
            raise TimeoutError(
                f"No login response received within {args.timeout:g} seconds."
            ) from error

        base = base_pb2.Base()
        base.ParseFromString(payload)
        print(f"Response template ID: {base.template_id}")
        if base.template_id != 11:
            print(
                "Expected login response template 11; received a different "
                "template. No market-data request was sent."
            )
            return 2

        response = response_login_pb2.ResponseLogin()
        response.ParseFromString(payload)
        codes = list(response.rp_code)
        print(f"Login response codes: {codes}")
        print(f"Login response messages: {list(response.user_msg)}")
        if codes == ["0"]:
            print("R|Protocol API login: SUCCESS")
            return 0

        print("R|Protocol API login: FAILED")
        print(
            "The WebSocket endpoint was reached, but Ticker Plant login was "
            "rejected. No Level 1 or Level 2 subscription was requested."
        )
        print(
            "This test uses the SDK SampleMD.py login fields. The response "
            "does not identify whether the cause is credentials, system access, "
            "or Protocol API entitlement."
        )
        return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=os.environ.get("RITHMIC_URI", DEFAULT_URI))
    parser.add_argument(
        "--system",
        default=os.environ.get("RITHMIC_SYSTEM", DEFAULT_SYSTEM),
        help="Exact system name advertised by Rithmic.",
    )
    parser.add_argument("--sdk-root", type=pathlib.Path, default=SDK_ROOT)
    parser.add_argument("--timeout", type=float, default=20)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")

    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nLogin diagnostic interrupted.")
        return 130
    except (OSError, ValueError, TimeoutError, WebSocketException) as error:
        print(f"Diagnostic failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
