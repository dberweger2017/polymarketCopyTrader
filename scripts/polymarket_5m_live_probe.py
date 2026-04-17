#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable

import requests
import websockets
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams, MarketOrderArgs, OrderType


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"
USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
DISCOVERY_TAGS = frozenset({"up-or-down", "5M", "recurring"})


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def json_get(url: str, *, params: dict[str, Any] | None = None) -> Any:
    response = requests.get(
        url,
        params=params,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def parse_json_list(value: str | list[Any] | None) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return json.loads(value)


def decimal_str(value: Decimal) -> str:
    return format(value.normalize() if value != 0 else Decimal("0"), "f")


def make_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_str(value)
    if isinstance(value, dict):
        return {str(key): make_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [make_jsonable(item) for item in value]
    return value


def round_price(value: Decimal, tick_size: Decimal, *, mode: str) -> Decimal:
    if tick_size <= 0:
        return value
    steps = value / tick_size
    if mode == "up":
        rounded_steps = steps.quantize(Decimal("1"), rounding=ROUND_CEILING)
    else:
        rounded_steps = steps.quantize(Decimal("1"), rounding=ROUND_FLOOR)
    return rounded_steps * tick_size


@dataclass(frozen=True)
class ActiveMarket:
    question: str
    slug: str
    condition_id: str
    token_ids: list[str]
    outcomes: list[str]
    end_date: datetime
    start_date: datetime


def discover_active_5m_market(asset: str) -> ActiveMarket:
    events = json_get(
        f"{GAMMA_API_BASE}/events",
        params={
            "tag_slug": asset,
            "closed": "false",
            "order": "start_date",
            "ascending": "false",
            "limit": 600,
        },
    )
    now = utc_now()
    candidates: list[ActiveMarket] = []
    for event in events:
        tags = {str(tag.get("slug")) for tag in event.get("tags", []) if tag.get("slug")}
        if not DISCOVERY_TAGS.issubset(tags):
            continue
        for market in event.get("markets", []):
            if not market.get("enableOrderBook"):
                continue
            start = iso_to_dt(market.get("eventStartTime")) or iso_to_dt(event.get("startTime"))
            end = iso_to_dt(market.get("endDate"))
            if start is None or end is None or end <= now:
                continue
            token_ids = [str(token) for token in parse_json_list(market.get("clobTokenIds"))]
            outcomes = [str(label) for label in parse_json_list(market.get("outcomes"))]
            if len(token_ids) != 2 or len(outcomes) != 2:
                continue
            candidates.append(
                ActiveMarket(
                    question=str(market.get("question") or event.get("title") or ""),
                    slug=str(market.get("slug") or event.get("slug") or ""),
                    condition_id=str(market.get("conditionId") or ""),
                    token_ids=token_ids,
                    outcomes=outcomes,
                    start_date=start,
                    end_date=end,
                )
            )
    active = [market for market in candidates if market.start_date <= now < market.end_date]
    if active:
        active.sort(key=lambda market: market.end_date)
        return active[0]
    if not candidates:
        raise RuntimeError(f"No future recurring 5-minute {asset} market found.")
    candidates.sort(key=lambda market: market.start_date)
    raise RuntimeError(
        f"No currently active recurring 5-minute {asset} market found. "
        f"Next one starts at {candidates[0].start_date.isoformat()}."
    )


def build_auth_client(env: dict[str, str]) -> ClobClient:
    creds = ApiCreds(
        api_key=env["POLYMARKET_API_KEY"],
        api_secret=env["POLYMARKET_API_SECRET"],
        api_passphrase=env["POLYMARKET_API_PASSPHRASE"],
    )
    return ClobClient(
        host=env["POLYMARKET_HOST"],
        chain_id=int(env["POLYMARKET_CHAIN_ID"]),
        key=env["POLYMARKET_PRIVATE_KEY"],
        creds=creds,
        signature_type=int(env["POLYMARKET_SIGNATURE_TYPE"]),
        funder=env["POLYMARKET_FUNDER_ADDRESS"],
    )


def compute_open_order_cash_reserve(open_orders: list[dict[str, Any]]) -> Decimal:
    reserved = Decimal("0")
    for order in open_orders:
        if str(order.get("side")).upper() != "BUY":
            continue
        original_size = Decimal(str(order.get("original_size", "0")))
        matched_size = Decimal(str(order.get("size_matched", "0")))
        price = Decimal(str(order.get("price", "0")))
        reserved += max(original_size - matched_size, Decimal("0")) * price
    return reserved


def fetch_market_positions(proxy_address: str, condition_id: str) -> list[dict[str, Any]]:
    payload = json_get(
        f"{DATA_API_BASE}/positions",
        params={
            "user": proxy_address,
            "market": condition_id,
            "sizeThreshold": 0,
            "limit": 500,
            "offset": 0,
        },
    )
    return payload if isinstance(payload, list) else []


def fetch_total_position_value(proxy_address: str) -> Decimal:
    payload = json_get(f"{DATA_API_BASE}/value", params={"user": proxy_address})
    if isinstance(payload, list) and payload:
        return Decimal(str(payload[0].get("value", 0)))
    return Decimal("0")


def fetch_snapshot(client: ClobClient, proxy_address: str, condition_id: str) -> dict[str, Any]:
    balance = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    open_orders = client.get_orders()
    positions = fetch_market_positions(proxy_address, condition_id)
    cash_balance = Decimal(str(balance["balance"])) / Decimal("1000000")
    reserved_cash = compute_open_order_cash_reserve(open_orders)
    total_position_value = fetch_total_position_value(proxy_address)
    return {
        "captured_at_utc": utc_now().isoformat(),
        "cash_balance_usdc": decimal_str(cash_balance),
        "cash_balance_micro_usdc": str(balance["balance"]),
        "open_orders_count": len(open_orders),
        "open_order_reserved_cash_usdc": decimal_str(reserved_cash),
        "total_position_value_usdc": decimal_str(total_position_value),
        "free_cash_usdc": decimal_str(cash_balance - reserved_cash),
        "market_positions_count": len(positions),
        "market_positions": positions,
    }


class UserChannel:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, condition_id: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.condition_id = condition_id
        self.ws: websockets.ClientConnection | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._events: list[dict[str, Any]] = []
        self._condition = asyncio.Condition()

    @property
    def events(self) -> list[dict[str, Any]]:
        return list(self._events)

    async def __aenter__(self) -> "UserChannel":
        self.ws = await websockets.connect(USER_WS_URL, ping_interval=20, ping_timeout=20)
        await self.ws.send(
            json.dumps(
                {
                    "auth": {
                        "apiKey": self.api_key,
                        "secret": self.api_secret,
                        "passphrase": self.api_passphrase,
                    },
                    "markets": [self.condition_id],
                    "type": "user",
                }
            )
        )
        self._reader_task = asyncio.create_task(self._reader())
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        if self.ws is not None:
            await self.ws.close()

    async def _reader(self) -> None:
        assert self.ws is not None
        async for raw in self.ws:
            received_monotonic_ns = time.monotonic_ns()
            received_at_utc = utc_now().isoformat()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            entries = payload if isinstance(payload, list) else [payload]
            async with self._condition:
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    event = {
                        "received_monotonic_ns": received_monotonic_ns,
                        "received_at_utc": received_at_utc,
                        **entry,
                    }
                    self._events.append(event)
                self._condition.notify_all()

    async def wait_for(self, predicate: Callable[[dict[str, Any]], bool], timeout: float) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        index = 0
        async with self._condition:
            while True:
                for event in self._events[index:]:
                    if predicate(event):
                        return event
                index = len(self._events)
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for user-channel event.")
                await asyncio.wait_for(self._condition.wait(), timeout=remaining)


async def wait_for_position(
    client: ClobClient,
    proxy_address: str,
    condition_id: str,
    token_id: str,
    *,
    timeout: float,
) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        positions = await asyncio.to_thread(fetch_market_positions, proxy_address, condition_id)
        for position in positions:
            if str(position.get("asset")) == token_id and Decimal(str(position.get("size", 0))) > 0:
                return position
        await asyncio.sleep(1.0)
    return None


def build_market_order_args(
    *,
    token_id: str,
    side: str,
    amount: Decimal,
    price: Decimal,
    order_type: OrderType,
) -> MarketOrderArgs:
    return MarketOrderArgs(
        token_id=token_id,
        amount=float(amount),
        side=side,
        price=float(price),
        order_type=order_type,
    )


def choose_market_side(market_meta: dict[str, Any]) -> dict[str, Any]:
    tokens = market_meta.get("tokens") or []
    if not isinstance(tokens, list) or not tokens:
        raise RuntimeError("Market metadata did not include token pricing.")
    chosen = max(tokens, key=lambda token: Decimal(str(token.get("price", 0))))
    return {
        "token_id": str(chosen["token_id"]),
        "outcome": str(chosen["outcome"]),
        "price": Decimal(str(chosen["price"])),
    }


def build_worst_price(current_price: Decimal, tick_size: Decimal, *, side: str) -> Decimal:
    buffer = Decimal("0.20")
    if side == "BUY":
        value = min(Decimal("0.99"), current_price + buffer)
        return max(tick_size, round_price(value, tick_size, mode="up"))
    value = max(tick_size, current_price - buffer)
    return min(Decimal("0.99"), round_price(value, tick_size, mode="down"))


async def execute_market_order(
    *,
    client: ClobClient,
    stream: UserChannel,
    token_id: str,
    side: str,
    amount: Decimal,
    order_type: OrderType,
    current_price: Decimal,
    tick_size: Decimal,
) -> dict[str, Any]:
    decision_at_utc = utc_now().isoformat()
    decision_monotonic_ns = time.monotonic_ns()
    worst_price = build_worst_price(current_price, tick_size, side=side)
    order_args = build_market_order_args(
        token_id=token_id,
        side=side,
        amount=amount,
        price=worst_price,
        order_type=order_type,
    )
    signed_order = await asyncio.to_thread(client.create_market_order, order_args)
    submit_started_ns = time.monotonic_ns()
    response = await asyncio.to_thread(client.post_order, signed_order, order_type)
    submit_completed_ns = time.monotonic_ns()

    order_id = str(response.get("orderID") or "")
    matched_event = await stream.wait_for(
        lambda event: event.get("event_type") == "trade"
        and str(event.get("taker_order_id")) == order_id
        and str(event.get("status")).upper() == "MATCHED",
        timeout=20,
    )
    confirmed_event = await stream.wait_for(
        lambda event: event.get("event_type") == "trade"
        and str(event.get("taker_order_id")) == order_id
        and str(event.get("status")).upper() == "CONFIRMED",
        timeout=60,
    )
    order_event = None
    try:
        order_event = await stream.wait_for(
            lambda event: event.get("event_type") == "order" and str(event.get("id")) == order_id,
            timeout=5,
        )
    except TimeoutError:
        order_event = None

    def ms_since_decision(monotonic_ns: int) -> float:
        return round((monotonic_ns - decision_monotonic_ns) / 1_000_000, 3)

    return {
        "decision_at_utc": decision_at_utc,
        "token_id": token_id,
        "side": side,
        "requested_amount": decimal_str(amount),
        "worst_price_limit": decimal_str(worst_price),
        "order_type": str(order_type),
        "post_order_response": response,
        "order_id": order_id,
        "http_submit_latency_ms": round((submit_completed_ns - submit_started_ns) / 1_000_000, 3),
        "decision_to_http_response_ms": ms_since_decision(submit_completed_ns),
        "decision_to_order_event_ms": ms_since_decision(order_event["received_monotonic_ns"]) if order_event else None,
        "decision_to_trade_matched_ms": ms_since_decision(matched_event["received_monotonic_ns"]),
        "decision_to_trade_confirmed_ms": ms_since_decision(confirmed_event["received_monotonic_ns"]),
        "matched_event": matched_event,
        "confirmed_event": confirmed_event,
        "order_event": order_event,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a tiny live trade on the active Polymarket 5-minute ETH market and measure latency."
    )
    parser.add_argument("--env-file", default=".env", help="Path to the env file containing Polymarket credentials.")
    parser.add_argument("--asset", default="ethereum", choices=["ethereum"], help="5-minute market family to probe.")
    parser.add_argument("--amount-usd", type=Decimal, default=Decimal("1"), help="USD notional for the opening buy.")
    parser.add_argument("--hold-seconds", type=float, default=20.0, help="Seconds to wait before closing the position.")
    parser.add_argument(
        "--output",
        default="docs/polymarket-5m-live-probe.json",
        help="Where to write the JSON report.",
    )
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Required to place live orders. Without this flag the script exits after discovery.",
    )
    return parser


async def async_main(args: argparse.Namespace) -> int:
    env = load_env(Path(args.env_file))
    required_keys = [
        "POLYMARKET_HOST",
        "POLYMARKET_CHAIN_ID",
        "POLYMARKET_SIGNATURE_TYPE",
        "POLYMARKET_FUNDER_ADDRESS",
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_API_PASSPHRASE",
        "POLYMARKET_PROXY_ADDRESS",
    ]
    missing = [key for key in required_keys if not env.get(key)]
    if missing:
        raise RuntimeError(f"Missing required env keys: {', '.join(missing)}")

    market = discover_active_5m_market(args.asset)
    client = build_auth_client(env)
    market_meta = await asyncio.to_thread(client.get_market, market.condition_id)
    chosen = choose_market_side(market_meta)
    tick_size = Decimal(str(market_meta["minimum_tick_size"]))

    pre_snapshot = await asyncio.to_thread(
        fetch_snapshot, client, env["POLYMARKET_PROXY_ADDRESS"], market.condition_id
    )
    result: dict[str, Any] = {
        "generated_at_utc": utc_now().isoformat(),
        "market": {
            "question": market.question,
            "slug": market.slug,
            "condition_id": market.condition_id,
            "start_date_utc": market.start_date.isoformat(),
            "end_date_utc": market.end_date.isoformat(),
            "minimum_order_size": market_meta.get("minimum_order_size"),
            "minimum_tick_size": market_meta.get("minimum_tick_size"),
            "tokens": market_meta.get("tokens"),
        },
        "decision_rule": "Buy the currently higher-priced outcome on the active 5-minute market.",
        "chosen_outcome": chosen,
        "pre_trade_snapshot": pre_snapshot,
        "docs_notes": {
            "balance_allowance_endpoint_tracks_cash_collateral": True,
            "positions_should_be_queried_via_data_api_positions_or_value": True,
            "user_channel_exists_for_faster_trade_lifecycle_updates": True,
            "official_refs": [
                "https://docs.polymarket.com/api-reference/authentication",
                "https://docs.polymarket.com/market-data/websocket/user-channel",
                "https://docs.polymarket.com/concepts/order-lifecycle",
                "https://docs.polymarket.com/trading/orderbook",
                "https://docs.polymarket.com/trading/orders/create",
            ],
        },
    }

    if not args.execute_live:
        result["live_execution"] = "skipped"
        rendered = json.dumps(make_jsonable(result), indent=2)
        Path(args.output).write_text(rendered, encoding="utf-8")
        print(rendered)
        return 0

    exit_code = 0
    try:
        async with UserChannel(
            env["POLYMARKET_API_KEY"],
            env["POLYMARKET_API_SECRET"],
            env["POLYMARKET_API_PASSPHRASE"],
            market.condition_id,
        ) as stream:
            buy_result = await execute_market_order(
                client=client,
                stream=stream,
                token_id=chosen["token_id"],
                side="BUY",
                amount=args.amount_usd,
                order_type=OrderType.FAK,
                current_price=chosen["price"],
                tick_size=tick_size,
            )
            result["buy_order"] = buy_result

            visible_position = await wait_for_position(
                client,
                env["POLYMARKET_PROXY_ADDRESS"],
                market.condition_id,
                chosen["token_id"],
                timeout=30,
            )
            result["position_after_buy"] = visible_position

            await asyncio.sleep(args.hold_seconds)

            refreshed_market = await asyncio.to_thread(client.get_market, market.condition_id)
            refreshed_tokens = refreshed_market.get("tokens") or []
            refreshed_price = next(
                Decimal(str(token["price"]))
                for token in refreshed_tokens
                if str(token["token_id"]) == chosen["token_id"]
            )

            if visible_position is None:
                raise RuntimeError(
                    "Buy confirmed, but no position appeared in the Data API for the chosen token."
                )
            position_size = Decimal(str(visible_position["size"]))
            sell_result = await execute_market_order(
                client=client,
                stream=stream,
                token_id=chosen["token_id"],
                side="SELL",
                amount=position_size,
                order_type=OrderType.FAK,
                current_price=refreshed_price,
                tick_size=tick_size,
            )
            result["sell_order"] = sell_result
    except Exception as exc:  # noqa: BLE001
        exit_code = 1
        result["live_execution"] = "failed"
        result["error"] = str(exc)
    finally:
        result["post_trade_snapshot"] = await asyncio.to_thread(
            fetch_snapshot, client, env["POLYMARKET_PROXY_ADDRESS"], market.condition_id
        )

    rendered = json.dumps(make_jsonable(result), indent=2)
    Path(args.output).write_text(rendered, encoding="utf-8")
    print(rendered)
    return exit_code


def main() -> int:
    args = build_parser().parse_args()
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
