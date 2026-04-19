#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Mapping

try:
    import requests
except ImportError:  # pragma: no cover - optional dependency at runtime
    requests = None

try:
    import websockets
except ImportError:  # pragma: no cover - optional dependency at runtime
    websockets = None

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        ApiCreds,
        AssetType,
        BalanceAllowanceParams,
        MarketOrderArgs,
        OrderType,
        TradeParams,
    )
except ImportError:  # pragma: no cover - optional dependency at runtime
    ClobClient = None
    ApiCreds = None
    AssetType = None
    BalanceAllowanceParams = None
    MarketOrderArgs = None
    OrderType = None
    TradeParams = None


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)
DATA_API_BASE = "https://data-api.polymarket.com"
USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


class LiveDependencyError(RuntimeError):
    pass


def ensure_live_dependencies() -> None:
    if requests is None:
        raise LiveDependencyError("Missing dependency: requests")
    if websockets is None:
        raise LiveDependencyError("Missing dependency: websockets")
    if ClobClient is None or ApiCreds is None or BalanceAllowanceParams is None or OrderType is None:
        raise LiveDependencyError("Missing dependency: py-clob-client")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def decimal_str(value: Decimal) -> str:
    return format(value.normalize() if value != 0 else Decimal("0"), "f")


def round_price(value: Decimal, tick_size: Decimal, *, mode: str) -> Decimal:
    if tick_size <= 0:
        return value
    steps = value / tick_size
    if mode == "up":
        rounded_steps = steps.quantize(Decimal("1"), rounding=ROUND_CEILING)
    else:
        rounded_steps = steps.quantize(Decimal("1"), rounding=ROUND_FLOOR)
    return rounded_steps * tick_size


def calculate_live_buy_shares(notional_usd: Decimal, buy_quote_price: Decimal) -> Decimal:
    if notional_usd <= 0 or buy_quote_price <= 0:
        return Decimal("0")
    return notional_usd / buy_quote_price


def calculate_live_sell_shares(
    target_notional_usd: Decimal,
    sell_quote_price: Decimal,
    available_shares: Decimal,
) -> Decimal:
    if target_notional_usd <= 0 or sell_quote_price <= 0 or available_shares <= 0:
        return Decimal("0")
    requested_shares = target_notional_usd / sell_quote_price
    return min(requested_shares, available_shares)


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


def build_live_account_state(
    *,
    cash_balance_micro_usdc: str | int,
    open_orders: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    marked_position_value_usdc: Decimal | None = None,
    executable_sell_prices: Mapping[str, Decimal | float | None] | None = None,
) -> dict[str, Any]:
    cash_balance = Decimal(str(cash_balance_micro_usdc)) / Decimal("1000000")
    reserved_cash = compute_open_order_cash_reserve(open_orders)
    normalized_positions: list[dict[str, Any]] = []
    positions_by_asset: dict[str, dict[str, Any]] = {}
    open_cost_basis = Decimal("0")
    executable_position_value = Decimal("0")

    for position in positions:
        shares = Decimal(str(position.get("size", "0")))
        if shares <= 0:
            continue
        asset_id = str(position.get("asset") or "")
        cost_basis = Decimal(str(position.get("initialValue", "0")))
        marked_value = Decimal(str(position.get("currentValue", "0")))
        marked_price = Decimal(str(position.get("curPrice", "0")))
        executable_sell_price: Decimal | None = None
        if executable_sell_prices is not None and asset_id in executable_sell_prices:
            raw_price = executable_sell_prices[asset_id]
            if raw_price is not None:
                executable_sell_price = Decimal(str(raw_price))
        executable_value = marked_value
        if executable_sell_price is not None and executable_sell_price > 0:
            executable_value = shares * executable_sell_price
        open_cost_basis += cost_basis
        executable_position_value += executable_value
        normalized = {
            "asset_id": asset_id,
            "condition_id": str(position.get("conditionId") or ""),
            "title": str(position.get("title") or ""),
            "label": str(position.get("outcome") or ""),
            "shares": decimal_str(shares),
            "avg_cost": decimal_str(Decimal(str(position.get("avgPrice", "0")))),
            "cost_basis": decimal_str(cost_basis),
            "marked_price": decimal_str(marked_price),
            "marked_value": decimal_str(marked_value),
            "executable_sell_price": decimal_str(executable_sell_price) if executable_sell_price is not None else "",
            "executable_value": decimal_str(executable_value),
            "raw": position,
        }
        normalized_positions.append(normalized)
        positions_by_asset[asset_id] = normalized

    effective_marked_value = marked_position_value_usdc
    if effective_marked_value is None:
        effective_marked_value = sum(Decimal(str(row["marked_value"])) for row in normalized_positions)

    return {
        "captured_at_utc": utc_now().isoformat(),
        "cash_balance_usdc": decimal_str(cash_balance),
        "cash_balance_micro_usdc": str(cash_balance_micro_usdc),
        "open_orders_count": len(open_orders),
        "open_order_reserved_cash_usdc": decimal_str(reserved_cash),
        "free_cash_usdc": decimal_str(cash_balance - reserved_cash),
        "open_positions_count": len(normalized_positions),
        "open_cost_basis_usdc": decimal_str(open_cost_basis),
        "marked_position_value_usdc": decimal_str(effective_marked_value),
        "executable_position_value_usdc": decimal_str(executable_position_value),
        "marked_total_account_value_usdc": decimal_str(cash_balance + effective_marked_value),
        "executable_total_account_value_usdc": decimal_str(cash_balance + executable_position_value),
        "positions": normalized_positions,
        "positions_by_asset": positions_by_asset,
        "raw_open_orders": open_orders,
    }


def _json_get(url: str, *, params: dict[str, Any] | None = None) -> Any:
    ensure_live_dependencies()
    response = requests.get(
        url,
        params=params,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def fetch_positions(proxy_address: str, *, condition_id: str | None = None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "user": proxy_address,
        "sizeThreshold": 0,
        "limit": 500,
        "offset": 0,
    }
    if condition_id is not None:
        params["market"] = condition_id
    payload = _json_get(f"{DATA_API_BASE}/positions", params=params)
    return payload if isinstance(payload, list) else []


def fetch_total_position_value(proxy_address: str) -> Decimal:
    payload = _json_get(f"{DATA_API_BASE}/value", params={"user": proxy_address})
    if isinstance(payload, list) and payload:
        return Decimal(str(payload[0].get("value", 0)))
    return Decimal("0")


def build_auth_client(env: Mapping[str, str]) -> Any:
    ensure_live_dependencies()
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


def fetch_live_account_state(
    client: Any,
    proxy_address: str,
    *,
    executable_sell_prices: Mapping[str, Decimal | float | None] | None = None,
) -> dict[str, Any]:
    ensure_live_dependencies()
    balance = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    open_orders = client.get_orders()
    positions = fetch_positions(proxy_address)
    if executable_sell_prices is None:
        executable_sell_prices = {}
        for position in positions:
            asset_id = str(position.get("asset") or "")
            if not asset_id:
                continue
            try:
                executable_sell_prices[asset_id] = Decimal(str(client.get_price(asset_id, "SELL")["price"]))
            except Exception:  # noqa: BLE001
                executable_sell_prices[asset_id] = None
    marked_value = fetch_total_position_value(proxy_address)
    return build_live_account_state(
        cash_balance_micro_usdc=balance["balance"],
        open_orders=open_orders,
        positions=positions,
        marked_position_value_usdc=marked_value,
        executable_sell_prices=executable_sell_prices,
    )


def fetch_token_quotes(client: Any, market_meta: dict[str, Any]) -> list[dict[str, Any]]:
    tokens = market_meta.get("tokens") or []
    quotes: list[dict[str, Any]] = []
    for token in tokens:
        token_id = str(token["token_id"])
        buy_quote: Decimal | None = None
        sell_quote: Decimal | None = None
        try:
            buy_quote = Decimal(str(client.get_price(token_id, "BUY")["price"]))
        except Exception:  # noqa: BLE001
            buy_quote = None
        try:
            sell_quote = Decimal(str(client.get_price(token_id, "SELL")["price"]))
        except Exception:  # noqa: BLE001
            sell_quote = None
        quotes.append(
            {
                "token_id": token_id,
                "outcome": str(token["outcome"]),
                "market_meta_price": Decimal(str(token.get("price", 0))),
                "buy_quote_price": buy_quote,
                "sell_quote_price": sell_quote,
            }
        )
    return quotes


def build_worst_price(current_price: Decimal, tick_size: Decimal, *, side: str) -> Decimal:
    buffer = Decimal("0.20")
    if side == "BUY":
        value = min(Decimal("0.99"), current_price + buffer)
        return max(tick_size, round_price(value, tick_size, mode="up"))
    value = max(tick_size, current_price - buffer)
    return min(Decimal("0.99"), round_price(value, tick_size, mode="down"))


def build_market_order_args(
    *,
    token_id: str,
    side: str,
    amount: Decimal,
    price: Decimal,
    order_type: Any,
) -> Any:
    ensure_live_dependencies()
    return MarketOrderArgs(
        token_id=token_id,
        amount=float(amount),
        side=side,
        price=float(price),
        order_type=order_type,
    )


@dataclass
class TradeTimingResult:
    status: str
    event: dict[str, Any]


class UserChannel:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, condition_id: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.condition_id = condition_id
        self.ws: Any = None
        self._reader_task: asyncio.Task[None] | None = None
        self._events: list[dict[str, Any]] = []
        self._condition = asyncio.Condition()

    @property
    def events(self) -> list[dict[str, Any]]:
        return list(self._events)

    async def __aenter__(self) -> "UserChannel":
        ensure_live_dependencies()
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
                    self._events.append(
                        {
                            "received_monotonic_ns": received_monotonic_ns,
                            "received_at_utc": received_at_utc,
                            **entry,
                        }
                    )
                self._condition.notify_all()

    async def wait_for(self, predicate: Any, timeout: float) -> dict[str, Any]:
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


async def poll_for_trade_status(
    client: Any,
    *,
    condition_id: str,
    order_id: str,
    status: str,
    timeout: float,
    poll_interval: float = 0.5,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    status_upper = status.upper()
    while time.monotonic() < deadline:
        trades = await asyncio.to_thread(client.get_trades, TradeParams(market=condition_id))
        for trade in trades:
            if str(trade.get("taker_order_id")) != order_id:
                continue
            if str(trade.get("status", "")).upper() != status_upper:
                continue
            return {
                "source": "poll",
                "received_monotonic_ns": time.monotonic_ns(),
                "received_at_utc": utc_now().isoformat(),
                "event_type": "trade",
                **trade,
            }
        await asyncio.sleep(poll_interval)
    raise TimeoutError(f"Timed out polling for trade status {status_upper}.")


async def wait_for_trade_status(
    *,
    stream: UserChannel | None,
    client: Any,
    condition_id: str,
    order_id: str,
    status: str,
    timeout: float,
) -> dict[str, Any]:
    status_upper = status.upper()
    websocket_timeout = min(timeout, 4.0)
    if stream is not None:
        try:
            event = await stream.wait_for(
                lambda item: item.get("event_type") == "trade"
                and str(item.get("taker_order_id")) == order_id
                and str(item.get("status", "")).upper() == status_upper,
                timeout=websocket_timeout,
            )
            return {"source": "user_ws", **event}
        except TimeoutError:
            pass
    remaining = max(timeout - websocket_timeout, 0.25)
    return await poll_for_trade_status(
        client,
        condition_id=condition_id,
        order_id=order_id,
        status=status_upper,
        timeout=remaining,
    )


async def execute_market_order(
    *,
    client: Any,
    stream: UserChannel | None,
    condition_id: str,
    token_id: str,
    side: str,
    amount: Decimal,
    order_type: Any,
    current_price: Decimal,
    tick_size: Decimal,
    matched_timeout: float = 6.0,
) -> dict[str, Any]:
    ensure_live_dependencies()
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
    matched_event = await wait_for_trade_status(
        stream=stream,
        client=client,
        condition_id=condition_id,
        order_id=order_id,
        status="MATCHED",
        timeout=matched_timeout,
    )

    def ms_since_decision(monotonic_ns: int) -> float:
        return round((monotonic_ns - decision_monotonic_ns) / 1_000_000, 3)

    return {
        "decision_at_utc": decision_at_utc,
        "decision_monotonic_ns": decision_monotonic_ns,
        "token_id": token_id,
        "side": side,
        "requested_amount": decimal_str(amount),
        "worst_price_limit": decimal_str(worst_price),
        "order_type": str(order_type),
        "post_order_response": response,
        "response_status": str(response.get("status") or ""),
        "order_id": order_id,
        "http_submit_latency_ms": round((submit_completed_ns - submit_started_ns) / 1_000_000, 3),
        "decision_to_http_response_ms": ms_since_decision(submit_completed_ns),
        "decision_to_trade_matched_ms": ms_since_decision(matched_event["received_monotonic_ns"]),
        "matched_event": matched_event,
    }


async def wait_for_order_confirmation(
    *,
    stream: UserChannel | None,
    client: Any,
    condition_id: str,
    order_id: str,
    decision_monotonic_ns: int,
    timeout: float = 60.0,
) -> dict[str, Any]:
    confirmed_event = await wait_for_trade_status(
        stream=stream,
        client=client,
        condition_id=condition_id,
        order_id=order_id,
        status="CONFIRMED",
        timeout=timeout,
    )
    return {
        "decision_to_trade_confirmed_ms": round(
            (confirmed_event["received_monotonic_ns"] - decision_monotonic_ns) / 1_000_000,
            3,
        ),
        "confirmed_event": confirmed_event,
    }
