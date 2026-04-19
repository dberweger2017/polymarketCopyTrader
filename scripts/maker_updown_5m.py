#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
import websockets
import plotext as plt
from py_clob_client.clob_types import (
    AssetType,
    BalanceAllowanceParams,
    OpenOrderParams,
    OrderArgs,
    OrderType,
)
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from websockets.exceptions import ConnectionClosed

from polymarket_live import (
    build_auth_client,
    compute_open_order_cash_reserve,
    fetch_positions,
    load_env,
)


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CHAINLINK_API_BASE = "https://data.chain.link/api"
CHAINLINK_LIVE_QUERY = "LIVE_STREAM_REPORTS_QUERY"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_RTDS_URL = "wss://ws-live-data.polymarket.com"
DISCOVERY_TAGS = frozenset({"up-or-down", "5M", "recurring"})
WINDOW_DURATION = timedelta(minutes=5)
RECURRING_MARKET_STEP_SECONDS = int(WINDOW_DURATION.total_seconds())
CONSOLE = Console()
CSV_FIELDNAMES = [
    "event_type",
    "logged_at_utc",
    "source_timestamp_utc",
    "market_slug",
    "market_id",
    "condition_id",
    "market_title",
    "asset",
    "token_id",
    "token_label",
    "price_to_beat",
    "beat_price_source",
    "current_price",
    "seconds_to_expiry",
    "sigma_per_sqrt_second",
    "fair_up",
    "fair_down",
    "fair_token",
    "target_alpha",
    "alpha_band_low",
    "alpha_band_high",
    "up_best_bid",
    "up_best_bid_size",
    "up_best_ask",
    "up_best_ask_size",
    "up_mid",
    "down_best_bid",
    "down_best_bid_size",
    "down_best_ask",
    "down_best_ask_size",
    "down_mid",
    "desired_bid_price",
    "desired_alpha",
    "active_order_id",
    "active_order_price",
    "active_order_size",
    "active_order_alpha",
    "token_position_shares",
    "token_position_cost_basis",
    "up_position_shares",
    "up_position_cost_basis",
    "down_position_shares",
    "down_position_cost_basis",
    "cash_balance_usdc",
    "total_reserved_cash_usdc",
    "market_reserved_cash_usdc",
    "market_committed_usdc",
    "combined_all_in_if_both_fill",
    "total_open_orders_count",
    "market_open_orders_count",
    "session_trade_count",
    "market_trade_count",
    "order_action",
    "order_id",
    "order_price",
    "order_size",
    "order_status",
    "fill_delta_shares",
    "note",
    "raw_payload_json",
]


@dataclass
class MarketWindow:
    asset: str
    event_id: str
    market_id: str
    condition_id: str
    title: str
    slug: str
    token_ids: tuple[str, str]
    outcome_labels: tuple[str, str]
    window_start: datetime
    window_end: datetime


ASSET_TAG_SLUGS: dict[str, str] = {
    "ethereum": "ethereum",
    "solana": "solana",
    "bitcoin": "bitcoin",
}

ASSET_RTDS_SYMBOLS: dict[str, str] = {
    "ethereum": "eth/usd",
    "solana": "sol/usd",
    "bitcoin": "btc/usd",
}

ASSET_CHAINLINK_FEED_IDS: dict[str, str] = {
    "ethereum": "0x000362205e10b3a147d02792eccee483dca6c7b44ecce7012cb8c6e0b68b3ae9",
    "solana": "0x0003b778d3f6b2ac4991302b89cb313f99a42467d6c9c5f96f57c29c0d2bc24f",
    "bitcoin": "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8",
}


@dataclass
class BookState:
    asset_id: str
    token_id: str
    label: str
    tick_size: float
    best_bid: float | None = None
    best_ask: float | None = None
    best_bid_size: float | None = None
    best_ask_size: float | None = None
    updated_at: datetime | None = None

    @property
    def mid(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0


@dataclass
class PositionState:
    asset_id: str
    label: str
    shares: float = 0.0
    cost_basis: float = 0.0


@dataclass
class ActiveOrder:
    order_id: str
    asset_id: str
    price: float
    size: float
    created_at: str
    raw: dict[str, Any]


@dataclass
class DesiredQuote:
    price: float
    alpha: float
    fair_value: float


@dataclass
class SessionStats:
    trade_count: int = 0


@dataclass(frozen=True)
class ChainlinkQuote:
    timestamp: datetime
    price: float


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def emit(message: str) -> None:
    CONSOLE.print(message)


def log(message: str, *, label: str = "INFO", tone: str = "white") -> None:
    emit(f"[{tone}][{label}][/{tone}] {message}")


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def http_json(path: str, *, params: dict[str, Any] | None = None, timeout: int = 20) -> Any:
    url = path if not params else f"{path}?{urlencode(params)}"
    response = requests.get(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def decimal_from_integer_string(value: str | None, *, scale: int = 18) -> float | None:
    if not value:
        return None
    try:
        decimal_value = Decimal(value) / (Decimal(10) ** scale)
    except (InvalidOperation, ValueError):
        return None
    return float(decimal_value)


def parse_token_ids(raw_token_ids: str | list[str] | None) -> tuple[str, str] | None:
    if raw_token_ids is None:
        return None
    values = json.loads(raw_token_ids) if isinstance(raw_token_ids, str) else raw_token_ids
    if not isinstance(values, list) or len(values) != 2:
        return None
    return str(values[0]), str(values[1])


def parse_outcome_labels(raw_outcomes: str | list[str] | None) -> tuple[str, str] | None:
    if raw_outcomes is None:
        return None
    values = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
    if not isinstance(values, list) or len(values) != 2:
        return None
    return str(values[0]), str(values[1])


def recurring_previous_slug(slug: str) -> str | None:
    prefix, separator, suffix = slug.rpartition("-")
    if not prefix or separator != "-":
        return None
    try:
        start_stamp = int(suffix)
    except ValueError:
        return None
    return f"{prefix}-{start_stamp - RECURRING_MARKET_STEP_SECONDS}"


def fetch_event_by_slug(slug: str) -> dict[str, Any] | None:
    payload = http_json(f"{GAMMA_API_BASE}/events", params={"slug": slug})
    if isinstance(payload, list) and payload:
        first = payload[0]
        return first if isinstance(first, dict) else None
    return payload if isinstance(payload, dict) else None


def fetch_previous_market_reference(market: MarketWindow) -> tuple[float, str] | None:
    previous_slug = recurring_previous_slug(market.slug)
    if previous_slug is None:
        return None
    event_payload = fetch_event_by_slug(previous_slug)
    if event_payload is None:
        return None
    metadata = event_payload.get("eventMetadata")
    if not isinstance(metadata, dict):
        return None
    final_price = metadata.get("finalPrice")
    try:
        return float(final_price), previous_slug
    except (TypeError, ValueError):
        return None


def select_boundary_quote(quotes: list[ChainlinkQuote], boundary: datetime) -> ChainlinkQuote | None:
    exact_match: ChainlinkQuote | None = None
    last_before_or_equal: ChainlinkQuote | None = None
    first_after_or_equal: ChainlinkQuote | None = None
    for quote in quotes:
        if quote.timestamp == boundary:
            exact_match = quote
            break
        if quote.timestamp <= boundary:
            last_before_or_equal = quote
        elif first_after_or_equal is None:
            first_after_or_equal = quote
    return exact_match or last_before_or_equal or first_after_or_equal


def fetch_chainlink_quotes(feed_id: str) -> list[ChainlinkQuote]:
    payload = http_json(
        f"{CHAINLINK_API_BASE}/query-timescale",
        params={
            "query": CHAINLINK_LIVE_QUERY,
            "variables": json.dumps({"feedId": feed_id}, separators=(",", ":")),
        },
        timeout=20,
    )
    nodes = payload.get("data", {}).get("liveStreamReports", {}).get("nodes", [])
    quotes: list[ChainlinkQuote] = []
    for node in reversed(nodes):
        if not isinstance(node, dict):
            continue
        timestamp = parse_iso_datetime(str(node.get("validFromTimestamp") or ""))
        if timestamp is None:
            continue
        price = decimal_from_integer_string(str(node.get("price") or ""))
        if price is None:
            continue
        quotes.append(ChainlinkQuote(timestamp=timestamp, price=price))
    return quotes


def find_market_start_quote(*, asset: str, market: MarketWindow) -> ChainlinkQuote | None:
    feed_id = ASSET_CHAINLINK_FEED_IDS[asset]
    quotes = fetch_chainlink_quotes(feed_id)
    return select_boundary_quote(quotes, market.window_start)


def discover_next_market(asset: str, processed_market_ids: set[str], *, require_future_start: bool) -> MarketWindow | None:
    events = http_json(
        f"{GAMMA_API_BASE}/events",
        params={
            "tag_slug": ASSET_TAG_SLUGS[asset],
            "closed": "false",
            "order": "start_date",
            "ascending": "false",
            "limit": 200,
        },
    )
    now = utc_now()
    candidates: list[MarketWindow] = []

    for event in events:
        if not isinstance(event, dict):
            continue
        event_tags = {str(tag.get("slug")) for tag in event.get("tags", []) if isinstance(tag, dict) and tag.get("slug")}
        if not DISCOVERY_TAGS.issubset(event_tags):
            continue

        for market in event.get("markets", []):
            if not isinstance(market, dict):
                continue
            market_id = str(market.get("id") or "")
            if not market_id or market_id in processed_market_ids:
                continue
            if not market.get("enableOrderBook", False):
                continue

            window_end = parse_iso_datetime(str(market.get("endDate") or ""))
            if window_end is None or window_end <= now:
                continue
            window_start = (
                parse_iso_datetime(str(market.get("eventStartTime") or ""))
                or parse_iso_datetime(str(event.get("startTime") or ""))
                or (window_end - WINDOW_DURATION)
            )
            if require_future_start and window_start <= now:
                continue

            token_ids = parse_token_ids(market.get("clobTokenIds"))
            outcome_labels = parse_outcome_labels(market.get("outcomes"))
            condition_id = str(market.get("conditionId") or "")
            if token_ids is None or outcome_labels is None or not condition_id:
                continue

            candidates.append(
                MarketWindow(
                    asset=asset,
                    event_id=str(event.get("id") or ""),
                    market_id=market_id,
                    condition_id=condition_id,
                    title=str(event.get("title") or market.get("question") or market_id),
                    slug=str(market.get("slug") or market_id),
                    token_ids=token_ids,
                    outcome_labels=outcome_labels,
                    window_start=window_start,
                    window_end=window_end,
                )
            )

    candidates.sort(key=lambda item: (item.window_start, item.window_end, item.market_id))
    return candidates[0] if candidates else None


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def approx_fee_per_share(price: float) -> float:
    bounded = max(0.0, min(price, 1.0))
    return 0.0156 * 4.0 * bounded * (1.0 - bounded)


def alpha_for_buy(*, fair_value: float | None, price: float | None) -> float | None:
    if fair_value is None or price is None:
        return None
    return fair_value - price - approx_fee_per_share(price)


def round_down_to_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return value
    steps = math.floor((value + 1e-12) / tick)
    return round(steps * tick, 10)


def short_order_id(value: str) -> str:
    return value[:8] if value else "-"


def format_duration(total_seconds: float) -> str:
    total = max(int(total_seconds), 0)
    minutes, seconds = divmod(total, 60)
    return f"{minutes}m {seconds:02d}s"


def format_price(value: float | None) -> str:
    return "-" if value is None else f"{value:.4f}"


def format_signed(value: float | None) -> str:
    return "-" if value is None else f"{value:+.4f}"


def format_money(value: float | None) -> str:
    return "-" if value is None else f"${value:,.2f}"


def json_compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def build_plotext_chart(
    *,
    x_values: list[float],
    series: list[tuple[str, list[float], str]],
    width: int,
    height: int,
    title: str,
    x_min: float | None = None,
    x_max: float | None = None,
    y_min: float | None = None,
    y_max: float | None = None,
) -> str:
    if not x_values or not any(values for _, values, _ in series):
        return "Waiting for enough live data to draw chart."

    plt.clear_figure()
    plt.theme("clear")
    plt.plotsize(width, height)
    plt.title(title)
    plt.xlabel("sec from start")
    plt.grid(True, True)

    if x_min is not None and x_max is not None and x_max > x_min and hasattr(plt, "xlim"):
        plt.xlim(x_min, x_max)

    if y_min is not None and y_max is not None and not math.isclose(y_min, y_max):
        plt.ylim(y_min, y_max)

    for label, values, color_name in series:
        if not values:
            continue
        points = x_values[-len(values):]
        plt.plot(points, values, color=color_name, label=label)

    return plt.build()


def solve_target_bid_price(*, fair_value: float | None, target_alpha: float, tick_size: float) -> float | None:
    if fair_value is None:
        return None
    hi = min(fair_value, 1.0 - tick_size)
    lo = tick_size
    if hi < lo:
        return None
    if alpha_for_buy(fair_value=fair_value, price=lo) is None:
        return None
    if alpha_for_buy(fair_value=fair_value, price=lo) < target_alpha:
        return None

    for _ in range(80):
        mid = (lo + hi) / 2.0
        alpha = alpha_for_buy(fair_value=fair_value, price=mid)
        if alpha is None:
            return None
        if alpha > target_alpha:
            lo = mid
        else:
            hi = mid

    return round_down_to_tick((lo + hi) / 2.0, tick_size)


def next_fill_within_side_exposure(
    *,
    side_shares: float,
    opposite_side_shares: float,
    order_size_shares: float,
    max_side_exposure_shares: float,
) -> bool:
    return side_shares + order_size_shares <= opposite_side_shares + max_side_exposure_shares + 1e-9


class SessionCsvRecorder:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self.path.exists()
        self._handle = self.path.open("a", newline="", encoding="utf-8", buffering=1)
        self._writer = csv.DictWriter(self._handle, fieldnames=CSV_FIELDNAMES)
        if not file_exists or self.path.stat().st_size == 0:
            self._writer.writeheader()

    def write_row(self, row: dict[str, Any]) -> None:
        normalized = {field: row.get(field, "") for field in CSV_FIELDNAMES}
        self._writer.writerow(normalized)

    def close(self) -> None:
        self._handle.close()


class MakerQuoter:
    def __init__(
        self,
        *,
        market: MarketWindow,
        client: Any,
        proxy_address: str,
        session_stats: SessionStats | None = None,
        csv_recorder: SessionCsvRecorder | None = None,
        target_alpha: float,
        alpha_band_half_width: float,
        order_size_shares: float,
        max_market_committed_usd: float,
        max_side_exposure_shares: float,
        no_trade_first_seconds: float,
        no_trade_last_seconds: float,
    ) -> None:
        self.market = market
        self.client = client
        self.proxy_address = proxy_address
        self.session_stats = session_stats or SessionStats()
        self.csv_recorder = csv_recorder
        self.target_alpha = target_alpha
        self.band_low = target_alpha - alpha_band_half_width
        self.band_high = target_alpha + alpha_band_half_width
        self.order_size_shares = order_size_shares
        self.max_market_committed_usd = max_market_committed_usd
        self.max_side_exposure_shares = max_side_exposure_shares
        self.no_trade_first_seconds = max(no_trade_first_seconds, 0.0)
        self.no_trade_last_seconds = max(no_trade_last_seconds, 0.0)

        self.start_underlying_price: float | None = None
        self.start_underlying_price_source: str = "-"
        self.latest_underlying_price: float | None = None
        self.latest_underlying_timestamp: datetime | None = None
        self._raw_underlying_ticks: deque[tuple[datetime, float]] = deque(maxlen=1200)
        self._underlying_by_second: deque[tuple[datetime, float]] = deque(maxlen=900)
        self._history_seconds: deque[datetime] = deque(maxlen=360)
        self._history_fair_up: deque[float] = deque(maxlen=360)
        self._history_up_mid: deque[float] = deque(maxlen=360)
        self._recent_events: deque[str] = deque(maxlen=12)
        self._sync_event: asyncio.Event | None = None

        self.books = {
            market.token_ids[0]: BookState(
                asset_id=market.token_ids[0],
                token_id=market.token_ids[0],
                label=market.outcome_labels[0],
                tick_size=0.01,
            ),
            market.token_ids[1]: BookState(
                asset_id=market.token_ids[1],
                token_id=market.token_ids[1],
                label=market.outcome_labels[1],
                tick_size=0.01,
            ),
        }
        self.positions: dict[str, PositionState] = {
            token_id: PositionState(asset_id=token_id, label=book.label) for token_id, book in self.books.items()
        }
        self.active_orders: dict[str, ActiveOrder | None] = {token_id: None for token_id in self.books}
        self.desired_quotes: dict[str, DesiredQuote | None] = {token_id: None for token_id in self.books}

        self.cash_balance_usdc: float = 0.0
        self.total_reserved_cash_usdc: float = 0.0
        self.market_reserved_cash_usdc: float = 0.0
        self.market_committed_usdc: float = 0.0
        self.combined_all_in_if_both_fill: float | None = None
        self.total_open_orders_count: int = 0
        self.market_open_orders_count: int = 0
        self.market_trade_count: int = 0

    def add_event(self, message: str) -> None:
        self._recent_events.appendleft(message)

    def current_timestamp(self) -> datetime:
        return self.latest_underlying_timestamp or utc_now()

    def attach_sync_event(self, sync_event: asyncio.Event) -> None:
        self._sync_event = sync_event

    def request_sync(self) -> None:
        if self._sync_event is not None:
            self._sync_event.set()

    @property
    def up_asset_id(self) -> str:
        for asset_id, label in zip(self.market.token_ids, self.market.outcome_labels, strict=True):
            if label.lower() == "up":
                return asset_id
        return self.market.token_ids[0]

    @property
    def down_asset_id(self) -> str:
        for asset_id, label in zip(self.market.token_ids, self.market.outcome_labels, strict=True):
            if label.lower() == "down":
                return asset_id
        return self.market.token_ids[1]

    def market_seconds_to_expiry(self, timestamp: datetime) -> float:
        return max((self.market.window_end - timestamp).total_seconds(), 0.0)

    def _context_row(
        self,
        *,
        event_type: str,
        timestamp: datetime | None = None,
        source_timestamp: datetime | None = None,
        asset_id: str | None = None,
        order_action: str = "",
        order_id: str = "",
        order_price: float | None = None,
        order_size: float | None = None,
        order_status: str = "",
        fill_delta_shares: float | None = None,
        note: str = "",
        raw_payload: Any | None = None,
    ) -> dict[str, Any]:
        logged_at = utc_now()
        context_time = timestamp or self.current_timestamp()
        fair_up = self.fair_up_probability(context_time)
        fair_down = None if fair_up is None else 1.0 - fair_up
        target_asset = asset_id or ""
        target_book = self.books.get(target_asset) if target_asset else None
        target_position = self.positions.get(target_asset) if target_asset else None
        target_desired = self.desired_quotes.get(target_asset) if target_asset else None
        target_active = self.active_orders.get(target_asset) if target_asset else None
        target_fair = self.fair_value_for(target_asset, timestamp=context_time) if target_asset else None
        active_alpha = (
            alpha_for_buy(fair_value=target_fair, price=target_active.price)
            if target_active is not None and target_asset
            else None
        )
        up_book = self.books[self.up_asset_id]
        down_book = self.books[self.down_asset_id]
        up_position = self.positions.get(self.up_asset_id, PositionState(asset_id=self.up_asset_id, label=up_book.label))
        down_position = self.positions.get(
            self.down_asset_id,
            PositionState(asset_id=self.down_asset_id, label=down_book.label),
        )
        row: dict[str, Any] = {
            "event_type": event_type,
            "logged_at_utc": logged_at.isoformat(),
            "source_timestamp_utc": source_timestamp.isoformat() if source_timestamp is not None else "",
            "market_slug": self.market.slug,
            "market_id": self.market.market_id,
            "condition_id": self.market.condition_id,
            "market_title": self.market.title,
            "asset": self.market.asset,
            "token_id": target_asset,
            "token_label": target_book.label if target_book is not None else "",
            "price_to_beat": self.start_underlying_price,
            "beat_price_source": self.start_underlying_price_source,
            "current_price": self.latest_underlying_price,
            "seconds_to_expiry": self.market_seconds_to_expiry(context_time),
            "sigma_per_sqrt_second": self.sigma_per_sqrt_second(),
            "fair_up": fair_up,
            "fair_down": fair_down,
            "fair_token": target_fair,
            "target_alpha": self.target_alpha,
            "alpha_band_low": self.band_low,
            "alpha_band_high": self.band_high,
            "up_best_bid": up_book.best_bid,
            "up_best_bid_size": up_book.best_bid_size,
            "up_best_ask": up_book.best_ask,
            "up_best_ask_size": up_book.best_ask_size,
            "up_mid": up_book.mid,
            "down_best_bid": down_book.best_bid,
            "down_best_bid_size": down_book.best_bid_size,
            "down_best_ask": down_book.best_ask,
            "down_best_ask_size": down_book.best_ask_size,
            "down_mid": down_book.mid,
            "desired_bid_price": target_desired.price if target_desired is not None else None,
            "desired_alpha": target_desired.alpha if target_desired is not None else None,
            "active_order_id": target_active.order_id if target_active is not None else "",
            "active_order_price": target_active.price if target_active is not None else None,
            "active_order_size": target_active.size if target_active is not None else None,
            "active_order_alpha": active_alpha,
            "token_position_shares": target_position.shares if target_position is not None else None,
            "token_position_cost_basis": target_position.cost_basis if target_position is not None else None,
            "up_position_shares": up_position.shares,
            "up_position_cost_basis": up_position.cost_basis,
            "down_position_shares": down_position.shares,
            "down_position_cost_basis": down_position.cost_basis,
            "cash_balance_usdc": self.cash_balance_usdc,
            "total_reserved_cash_usdc": self.total_reserved_cash_usdc,
            "market_reserved_cash_usdc": self.market_reserved_cash_usdc,
            "market_committed_usdc": self.market_committed_usdc,
            "combined_all_in_if_both_fill": self.combined_all_in_if_both_fill,
            "total_open_orders_count": self.total_open_orders_count,
            "market_open_orders_count": self.market_open_orders_count,
            "session_trade_count": self.session_stats.trade_count,
            "market_trade_count": self.market_trade_count,
            "order_action": order_action,
            "order_id": order_id,
            "order_price": order_price,
            "order_size": order_size,
            "order_status": order_status,
            "fill_delta_shares": fill_delta_shares,
            "note": note,
            "raw_payload_json": json_compact(raw_payload) if raw_payload is not None else "",
        }
        return row

    def log_csv(
        self,
        *,
        event_type: str,
        timestamp: datetime | None = None,
        source_timestamp: datetime | None = None,
        asset_id: str | None = None,
        order_action: str = "",
        order_id: str = "",
        order_price: float | None = None,
        order_size: float | None = None,
        order_status: str = "",
        fill_delta_shares: float | None = None,
        note: str = "",
        raw_payload: Any | None = None,
    ) -> None:
        if self.csv_recorder is None:
            return
        self.csv_recorder.write_row(
            self._context_row(
                event_type=event_type,
                timestamp=timestamp,
                source_timestamp=source_timestamp,
                asset_id=asset_id,
                order_action=order_action,
                order_id=order_id,
                order_price=order_price,
                order_size=order_size,
                order_status=order_status,
                fill_delta_shares=fill_delta_shares,
                note=note,
                raw_payload=raw_payload,
            )
        )

    def seed_reference_price(self, price: float, *, note: str) -> None:
        self.start_underlying_price = price
        self.start_underlying_price_source = note
        self.add_event(f"Reference seeded {price:.2f} from {note}")
        self.log_csv(
            event_type="reference_seed",
            timestamp=self.current_timestamp(),
            note=note,
            raw_payload={"price": price, "note": note},
        )

    def try_seed_reference_from_buffer(self) -> bool:
        if self.start_underlying_price is not None:
            return True
        if self.latest_underlying_timestamp is None or self.latest_underlying_timestamp < self.market.window_start:
            return False
        quotes = [ChainlinkQuote(timestamp=timestamp, price=price) for timestamp, price in self._raw_underlying_ticks]
        quote = select_boundary_quote(quotes, self.market.window_start)
        if quote is None:
            return False
        if quote.timestamp == self.market.window_start:
            note = f"RTDS boundary exact @ {quote.timestamp.isoformat()}"
        elif quote.timestamp < self.market.window_start:
            note = f"RTDS last <= open @ {quote.timestamp.isoformat()}"
        else:
            note = f"RTDS first >= open @ {quote.timestamp.isoformat()}"
        self.seed_reference_price(quote.price, note=note)
        return True

    def on_underlying_price(self, *, timestamp: datetime, price: float) -> None:
        self.latest_underlying_price = price
        self.latest_underlying_timestamp = timestamp
        self._raw_underlying_ticks.append((timestamp, price))
        self.try_seed_reference_from_buffer()
        second_timestamp = timestamp.replace(microsecond=0)
        if self._underlying_by_second and self._underlying_by_second[-1][0] == second_timestamp:
            self._underlying_by_second[-1] = (second_timestamp, price)
        else:
            self._underlying_by_second.append((second_timestamp, price))
        self.log_csv(
            event_type="underlying_tick",
            timestamp=timestamp,
            source_timestamp=timestamp,
            note="rtds_chainlink",
            raw_payload={"price": price, "symbol": ASSET_RTDS_SYMBOLS[self.market.asset]},
        )
        self.request_sync()

    def on_book_message(self, payload: dict[str, Any]) -> None:
        if str(payload.get("event_type") or payload.get("type") or "") != "book":
            return
        asset_id = str(payload.get("asset_id") or "")
        book = self.books.get(asset_id)
        if book is None:
            return
        bids = payload.get("bids") if isinstance(payload.get("bids"), list) else []
        asks = payload.get("asks") if isinstance(payload.get("asks"), list) else []
        book.best_bid, book.best_bid_size = self._best_level(bids, reverse=True)
        book.best_ask, book.best_ask_size = self._best_level(asks, reverse=False)
        book.updated_at = parse_iso_datetime(str(payload.get("timestamp") or "")) or utc_now()
        self.log_csv(
            event_type="market_book",
            timestamp=book.updated_at,
            source_timestamp=book.updated_at,
            asset_id=asset_id,
            note="top_of_book_update",
            raw_payload=payload,
        )
        self.request_sync()

    def _best_level(self, levels: list[dict[str, Any]], *, reverse: bool) -> tuple[float | None, float | None]:
        parsed: list[tuple[float, float]] = []
        for level in levels:
            try:
                parsed.append((float(level["price"]), float(level["size"])))
            except (KeyError, TypeError, ValueError):
                continue
        if not parsed:
            return None, None
        parsed.sort(key=lambda item: item[0], reverse=reverse)
        return parsed[0]

    def sigma_per_sqrt_second(self) -> float | None:
        if len(self._underlying_by_second) < 3:
            return None
        diffs = [
            current - previous
            for (_, previous), (_, current) in zip(self._underlying_by_second, list(self._underlying_by_second)[1:])
        ]
        if len(diffs) < 2:
            return None
        mean = sum(diffs) / len(diffs)
        variance = sum((diff - mean) ** 2 for diff in diffs) / (len(diffs) - 1)
        sigma = math.sqrt(max(variance, 0.0))
        return sigma if sigma > 1e-9 else None

    def fair_up_probability(self, timestamp: datetime) -> float | None:
        if self.start_underlying_price is None or self.latest_underlying_price is None:
            return None
        sigma = self.sigma_per_sqrt_second()
        if sigma is None:
            return 0.5
        remaining_seconds = max((self.market.window_end - timestamp).total_seconds(), 1e-9)
        z_score = (self.latest_underlying_price - self.start_underlying_price) / (sigma * math.sqrt(remaining_seconds))
        return max(0.0, min(1.0, normal_cdf(z_score)))

    def fair_value_for(self, asset_id: str, *, timestamp: datetime) -> float | None:
        fair_up = self.fair_up_probability(timestamp)
        if fair_up is None:
            return None
        first_token = self.market.token_ids[0]
        return fair_up if asset_id == first_token else 1.0 - fair_up

    def trading_active(self, timestamp: datetime) -> bool:
        return (
            timestamp >= self.market.window_start + timedelta(seconds=self.no_trade_first_seconds)
            and timestamp <= self.market.window_end - timedelta(seconds=self.no_trade_last_seconds)
        )

    def desired_quote_for(self, asset_id: str, *, timestamp: datetime) -> DesiredQuote | None:
        book = self.books[asset_id]
        fair_value = self.fair_value_for(asset_id, timestamp=timestamp)
        target_price = solve_target_bid_price(
            fair_value=fair_value,
            target_alpha=self.target_alpha,
            tick_size=book.tick_size,
        )
        if fair_value is None or target_price is None:
            return None
        if book.best_ask is not None:
            max_maker_price = round_down_to_tick(book.best_ask - book.tick_size, book.tick_size)
            if max_maker_price < book.tick_size:
                return None
            target_price = min(target_price, max_maker_price)
            target_price = round_down_to_tick(target_price, book.tick_size)
        alpha = alpha_for_buy(fair_value=fair_value, price=target_price)
        if alpha is None or alpha < self.band_low or alpha > self.band_high:
            return None
        return DesiredQuote(price=target_price, alpha=alpha, fair_value=fair_value)

    def can_add_next_chunk(self, asset_id: str) -> bool:
        side_position = self.positions.get(asset_id, PositionState(asset_id=asset_id, label=self.books[asset_id].label))
        opposite_asset_id = self.market.token_ids[1] if asset_id == self.market.token_ids[0] else self.market.token_ids[0]
        opposite_position = self.positions.get(
            opposite_asset_id,
            PositionState(asset_id=opposite_asset_id, label=self.books[opposite_asset_id].label),
        )
        return next_fill_within_side_exposure(
            side_shares=side_position.shares,
            opposite_side_shares=opposite_position.shares,
            order_size_shares=self.order_size_shares,
            max_side_exposure_shares=self.max_side_exposure_shares,
        )

    def record_history(self, timestamp: datetime) -> None:
        fair_up = self.fair_up_probability(timestamp)
        up_mid = self.books[self.up_asset_id].mid
        if fair_up is None or up_mid is None:
            return
        second_timestamp = timestamp.replace(microsecond=0)
        if self._history_seconds and self._history_seconds[-1] == second_timestamp:
            self._history_fair_up[-1] = fair_up
            self._history_up_mid[-1] = up_mid
            return
        self._history_seconds.append(second_timestamp)
        self._history_fair_up.append(fair_up)
        self._history_up_mid.append(up_mid)

    async def initialize_books(self) -> None:
        for token_id, book in self.books.items():
            summary = await asyncio.to_thread(self.client.get_order_book, token_id)
            try:
                book.tick_size = float(summary.tick_size or "0.01")
            except (TypeError, ValueError):
                book.tick_size = 0.01
            book.best_bid, book.best_bid_size = self._summary_best(summary.bids, reverse=True)
            book.best_ask, book.best_ask_size = self._summary_best(summary.asks, reverse=False)

    def _summary_best(self, levels: list[Any] | None, *, reverse: bool) -> tuple[float | None, float | None]:
        parsed: list[tuple[float, float]] = []
        for level in levels or []:
            try:
                parsed.append((float(level.price), float(level.size)))
            except (AttributeError, TypeError, ValueError):
                continue
        if not parsed:
            return None, None
        parsed.sort(key=lambda item: item[0], reverse=reverse)
        return parsed[0]

    async def refresh_account_state(self) -> None:
        previous_positions = {asset_id: position.shares for asset_id, position in self.positions.items()}
        balance = await asyncio.to_thread(
            self.client.get_balance_allowance,
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
        )
        all_open_orders = await asyncio.to_thread(self.client.get_orders)
        market_open_orders = [
            order for order in all_open_orders if str(order.get("asset_id") or order.get("assetId") or "") in self.books
        ]
        positions_raw = await asyncio.to_thread(fetch_positions, self.proxy_address, condition_id=self.market.condition_id)

        self.cash_balance_usdc = float(balance["balance"]) / 1_000_000.0
        self.total_reserved_cash_usdc = float(compute_open_order_cash_reserve(all_open_orders))
        self.market_reserved_cash_usdc = float(compute_open_order_cash_reserve(market_open_orders))
        self.total_open_orders_count = len(all_open_orders)
        self.market_open_orders_count = len(market_open_orders)

        self.positions = {
            token_id: PositionState(asset_id=token_id, label=book.label) for token_id, book in self.books.items()
        }
        for raw in positions_raw:
            asset_id = str(raw.get("asset") or "")
            if asset_id not in self.positions:
                continue
            self.positions[asset_id] = PositionState(
                asset_id=asset_id,
                label=str(raw.get("outcome") or self.books[asset_id].label),
                shares=float(raw.get("size") or 0.0),
                cost_basis=float(raw.get("initialValue") or 0.0),
            )

        for asset_id, position in self.positions.items():
            previous = previous_positions.get(asset_id, 0.0)
            if position.shares > previous + 1e-6:
                fill_delta = position.shares - previous
                self.session_stats.trade_count += 1
                self.market_trade_count += 1
                self.add_event(
                    f"Filled {self.books[asset_id].label} +{fill_delta:.4f} sh | total {position.shares:.4f}"
                )
                self.log_csv(
                    event_type="fill",
                    timestamp=self.current_timestamp(),
                    source_timestamp=self.current_timestamp(),
                    asset_id=asset_id,
                    order_action="fill",
                    fill_delta_shares=fill_delta,
                    note="position increase detected on refresh",
                    raw_payload={"positions": positions_raw},
                )

        grouped_orders: dict[str, list[dict[str, Any]]] = {token_id: [] for token_id in self.books}
        for order in market_open_orders:
            asset_id = str(order.get("asset_id") or order.get("assetId") or "")
            if asset_id not in grouped_orders:
                continue
            if str(order.get("side") or "").upper() != "BUY":
                continue
            grouped_orders[asset_id].append(order)

        new_active_orders: dict[str, ActiveOrder | None] = {token_id: None for token_id in self.books}
        for asset_id, orders in grouped_orders.items():
            if not orders:
                continue
            orders.sort(key=lambda item: str(item.get("created_at") or item.get("createdAt") or item.get("id") or ""))
            newest = orders[-1]
            new_active_orders[asset_id] = ActiveOrder(
                order_id=str(newest.get("id") or newest.get("orderID") or ""),
                asset_id=asset_id,
                price=float(newest.get("price") or 0.0),
                size=float((newest.get("original_size") or newest.get("size") or 0.0)),
                created_at=str(newest.get("created_at") or newest.get("createdAt") or ""),
                raw=newest,
            )

        self.active_orders = new_active_orders
        self.market_committed_usdc = self.market_reserved_cash_usdc + sum(position.cost_basis for position in self.positions.values())
        self.log_csv(
            event_type="account_snapshot",
            timestamp=self.current_timestamp(),
            source_timestamp=self.current_timestamp(),
            note="post_refresh",
            raw_payload={
                "balance": balance,
                "market_open_orders": market_open_orders,
                "positions": positions_raw,
            },
        )

    async def cancel_duplicate_orders(self) -> None:
        market_open_orders = await asyncio.to_thread(
            self.client.get_orders,
            OpenOrderParams(market=self.market.condition_id),
        )
        grouped: dict[str, list[dict[str, Any]]] = {token_id: [] for token_id in self.books}
        for order in market_open_orders:
            asset_id = str(order.get("asset_id") or order.get("assetId") or "")
            if asset_id in grouped and str(order.get("side") or "").upper() == "BUY":
                grouped[asset_id].append(order)
        for asset_id, orders in grouped.items():
            if len(orders) <= 1:
                continue
            orders.sort(key=lambda item: str(item.get("created_at") or item.get("createdAt") or item.get("id") or ""))
            for stale in orders[:-1]:
                stale_id = str(stale.get("id") or stale.get("orderID") or "")
                if stale_id:
                    await asyncio.to_thread(self.client.cancel, stale_id)
                    self.add_event(f"Canceled duplicate {self.books[asset_id].label} {short_order_id(stale_id)}")
                    self.log_csv(
                        event_type="order_cancel",
                        timestamp=self.current_timestamp(),
                        source_timestamp=self.current_timestamp(),
                        asset_id=asset_id,
                        order_action="cancel_duplicate",
                        order_id=stale_id,
                        order_price=float(stale.get("price") or 0.0),
                        order_size=float(stale.get("size") or stale.get("original_size") or 0.0),
                        order_status="canceled",
                        note="duplicate_market_order",
                        raw_payload=stale,
                    )

    async def sync_quotes(self) -> None:
        await self.refresh_account_state()
        await self.cancel_duplicate_orders()

        now = self.latest_underlying_timestamp or utc_now()
        if now < self.market.window_start:
            return

        self.desired_quotes = {
            asset_id: self.desired_quote_for(asset_id, timestamp=now) for asset_id in self.books
        }
        self.record_history(now)

        if not self.trading_active(now):
            for asset_id, active in list(self.active_orders.items()):
                if active is None:
                    continue
                await asyncio.to_thread(self.client.cancel, active.order_id)
                self.add_event(f"Cancel {self.books[asset_id].label} {format_price(active.price)} | warmup window")
                self.log_csv(
                    event_type="order_cancel",
                    timestamp=now,
                    source_timestamp=now,
                    asset_id=asset_id,
                    order_action="cancel_no_trade_window",
                    order_id=active.order_id,
                    order_price=active.price,
                    order_size=active.size,
                    order_status="canceled",
                    note="outside trading window",
                    raw_payload=active.raw,
                )
                self.active_orders[asset_id] = None
            self.combined_all_in_if_both_fill = None
            return

        desired_quotes = [quote for quote in self.desired_quotes.values() if quote is not None]
        if desired_quotes:
            self.combined_all_in_if_both_fill = self.order_size_shares * sum(
                quote.price + approx_fee_per_share(quote.price) for quote in desired_quotes
            )
        else:
            self.combined_all_in_if_both_fill = None

        remaining_budget = max(self.max_market_committed_usd - self.market_committed_usdc, 0.0)

        for asset_id, book in self.books.items():
            desired = self.desired_quotes[asset_id]
            active = self.active_orders.get(asset_id)
            fair_value = self.fair_value_for(asset_id, timestamp=now)
            active_alpha = alpha_for_buy(fair_value=fair_value, price=active.price) if active is not None else None
            can_add_chunk = self.can_add_next_chunk(asset_id)

            if active is not None and (
                desired is None
                or active_alpha is None
                or active_alpha < self.band_low
                or active_alpha > self.band_high
                or not can_add_chunk
            ):
                await asyncio.to_thread(self.client.cancel, active.order_id)
                self.add_event(
                    f"Cancel {book.label} {format_price(active.price)} | alpha {format_signed(active_alpha)}"
                )
                self.log_csv(
                    event_type="order_cancel",
                    timestamp=now,
                    source_timestamp=now,
                    asset_id=asset_id,
                    order_action="cancel_reprice",
                    order_id=active.order_id,
                    order_price=active.price,
                    order_size=active.size,
                    order_status="canceled",
                    note=f"active alpha {format_signed(active_alpha)}",
                    raw_payload=active.raw,
                )
                self.active_orders[asset_id] = None
                continue

            if active is not None:
                continue
            if desired is None:
                continue
            if not can_add_chunk:
                continue

            order_cost = self.order_size_shares * (desired.price + approx_fee_per_share(desired.price))
            if order_cost > remaining_budget + 1e-9:
                continue

            signed_order = await asyncio.to_thread(
                self.client.create_order,
                OrderArgs(
                    token_id=asset_id,
                    price=desired.price,
                    size=self.order_size_shares,
                    side="BUY",
                ),
            )
            response = await asyncio.to_thread(self.client.post_order, signed_order, OrderType.GTC)
            order_id = str(response.get("orderID") or response.get("id") or "")
            self.add_event(
                f"Place {book.label} {self.order_size_shares:.2f} @ {desired.price:.4f} | alpha {desired.alpha:+.4f} | {short_order_id(order_id)}"
            )
            self.log_csv(
                event_type="order_place",
                timestamp=now,
                source_timestamp=now,
                asset_id=asset_id,
                order_action="place_buy",
                order_id=order_id,
                order_price=desired.price,
                order_size=self.order_size_shares,
                order_status=str(response.get("status") or response.get("success") or ""),
                note=f"desired alpha {desired.alpha:+.4f}",
                raw_payload=response,
            )
            remaining_budget = max(remaining_budget - order_cost, 0.0)
            self.active_orders[asset_id] = ActiveOrder(
                order_id=order_id,
                asset_id=asset_id,
                price=desired.price,
                size=self.order_size_shares,
                created_at=utc_now().isoformat(),
                raw=response,
            )

    async def cancel_market_orders(self) -> None:
        try:
            await asyncio.to_thread(self.client.cancel_market_orders, self.market.condition_id, "")
            self.log_csv(
                event_type="order_cancel",
                timestamp=self.current_timestamp(),
                source_timestamp=self.current_timestamp(),
                order_action="cancel_market_orders",
                order_status="canceled",
                note="market shutdown",
                raw_payload={"condition_id": self.market.condition_id},
            )
        except Exception:  # noqa: BLE001
            for order in list(self.active_orders.values()):
                if order is None or not order.order_id:
                    continue
                try:
                    await asyncio.to_thread(self.client.cancel, order.order_id)
                    self.log_csv(
                        event_type="order_cancel",
                        timestamp=self.current_timestamp(),
                        source_timestamp=self.current_timestamp(),
                        asset_id=order.asset_id,
                        order_action="cancel_market_orders_fallback",
                        order_id=order.order_id,
                        order_price=order.price,
                        order_size=order.size,
                        order_status="canceled",
                        note="market shutdown fallback",
                        raw_payload=order.raw,
                    )
                except Exception:  # noqa: BLE001
                    continue

    def render(self) -> Panel:
        timestamp = self.latest_underlying_timestamp or utc_now()
        fair_up = self.fair_up_probability(timestamp)
        fair_down = None if fair_up is None else 1.0 - fair_up
        sigma = self.sigma_per_sqrt_second()
        remaining = max((self.market.window_end - timestamp).total_seconds(), 0.0)

        summary = Table.grid(expand=True)
        summary.add_column()
        summary.add_column()
        summary.add_row("Market", self.market.title)
        summary.add_row("Time Left", format_duration(remaining))
        summary.add_row("Price To Beat", "-" if self.start_underlying_price is None else f"{self.start_underlying_price:.2f}")
        summary.add_row("Beat Price Source", self.start_underlying_price_source)
        summary.add_row("Current Price", "-" if self.latest_underlying_price is None else f"{self.latest_underlying_price:.2f}")
        summary.add_row("Sigma/sqrt(s)", "-" if sigma is None else f"{sigma:.6f}")
        summary.add_row("Fair Up / Down", f"{format_price(fair_up)} / {format_price(fair_down)}")
        summary.add_row("Target Alpha Band", f"{self.band_low:.3f} to {self.band_high:.3f}")
        summary.add_row("No-Trade Warmup", format_duration(self.no_trade_first_seconds))
        summary.add_row("No-Trade Final", format_duration(self.no_trade_last_seconds))
        summary.add_row("Max Side Exposure", f"{self.max_side_exposure_shares:.2f} sh")
        summary.add_row("Cash / Reserved", f"{format_money(self.cash_balance_usdc)} / {format_money(self.total_reserved_cash_usdc)}")
        summary.add_row("Market Committed", format_money(self.market_committed_usdc))
        summary.add_row("Both Fills All-In", format_money(self.combined_all_in_if_both_fill))
        summary.add_row("Trades This Market", str(self.market_trade_count))
        summary.add_row("Trades This Session", str(self.session_stats.trade_count))

        quotes = Table(title="Maker Quotes", expand=True)
        quotes.add_column("Token")
        quotes.add_column("Best Bid")
        quotes.add_column("Best Ask")
        quotes.add_column("Fair")
        quotes.add_column("Desired Bid")
        quotes.add_column("Desired Alpha")
        quotes.add_column("Active Bid")
        quotes.add_column("Active Alpha")
        quotes.add_column("Order")
        quotes.add_column("Pos")
        quotes.add_column("Cost")
        quotes.add_column("Can Add")

        for asset_id in self.market.token_ids:
            book = self.books[asset_id]
            desired = self.desired_quotes.get(asset_id)
            active = self.active_orders.get(asset_id)
            fair_value = self.fair_value_for(asset_id, timestamp=timestamp)
            active_alpha = alpha_for_buy(fair_value=fair_value, price=active.price) if active is not None else None
            position = self.positions.get(asset_id, PositionState(asset_id=asset_id, label=book.label))
            quotes.add_row(
                book.label,
                format_price(book.best_bid),
                format_price(book.best_ask),
                format_price(fair_value),
                format_price(desired.price if desired is not None else None),
                format_signed(desired.alpha if desired is not None else None),
                format_price(active.price if active is not None else None),
                format_signed(active_alpha),
                short_order_id(active.order_id if active is not None else ""),
                f"{position.shares:.4f}",
                format_money(position.cost_basis),
                "yes" if self.can_add_next_chunk(asset_id) else "no",
            )

        events = Text("\n".join(self._recent_events) if self._recent_events else "No events yet.")
        x_values = [
            max((point - self.market.window_start).total_seconds(), 0.0)
            for point in self._history_seconds
        ]
        price_chart = build_plotext_chart(
            x_values=x_values,
            series=[
                ("fair mid", list(self._history_fair_up), "cyan"),
                ("market mid", list(self._history_up_mid), "yellow"),
            ],
            width=88,
            height=14,
            title="Up Mid vs Model Mid",
            x_min=0.0,
            x_max=max((self.market.window_end - self.market.window_start).total_seconds(), 1.0),
            y_min=0.0,
            y_max=1.0,
        )
        return Panel(
            Group(summary, quotes, Panel(price_chart, title="Price Chart"), Panel(events, title="Recent Events")),
            title="5m Maker Engine",
            border_style="cyan",
        )


async def rtds_ping(websocket: websockets.ClientConnection, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(5.0)
        await websocket.send("PING")


async def stream_chainlink_prices(*, symbol: str, strategy: MakerQuoter, stop_event: asyncio.Event) -> None:
    subscribe_message = {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": "",
            }
        ],
    }
    reconnect_backoff = 1.0
    while not stop_event.is_set():
        try:
            async with websockets.connect(
                POLYMARKET_RTDS_URL,
                ping_interval=None,
                close_timeout=5,
                open_timeout=20,
                max_size=None,
            ) as websocket:
                await websocket.send(json.dumps(subscribe_message))
                ping_task = asyncio.create_task(rtds_ping(websocket, stop_event))
                reconnect_backoff = 1.0
                try:
                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        except TimeoutError:
                            continue
                        if raw == "PONG" or raw == "":
                            continue
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if str(payload.get("topic") or "") != "crypto_prices_chainlink":
                            continue
                        inner = payload.get("payload")
                        if not isinstance(inner, dict):
                            continue
                        if str(inner.get("symbol") or "").lower() != symbol:
                            continue
                        try:
                            price = float(inner["value"])
                            timestamp_ms = int(inner["timestamp"])
                        except (KeyError, TypeError, ValueError):
                            continue
                        strategy.on_underlying_price(
                            timestamp=datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc),
                            price=price,
                        )
                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
        except ConnectionClosed as exc:
            log(f"RTDS closed: {exc}. Reconnecting in {reconnect_backoff:.1f}s.", label="CHAINLINK", tone="yellow")
        except Exception as exc:  # noqa: BLE001
            log(f"RTDS error: {exc}. Reconnecting in {reconnect_backoff:.1f}s.", label="CHAINLINK", tone="yellow")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=reconnect_backoff)
        except TimeoutError:
            reconnect_backoff = min(reconnect_backoff * 2, 15.0)
            continue
        return


async def stream_market_books(*, market: MarketWindow, strategy: MakerQuoter, stop_event: asyncio.Event) -> None:
    subscribe_message = {"type": "market", "assets_ids": list(market.token_ids)}
    reconnect_backoff = 1.0
    while not stop_event.is_set():
        try:
            async with websockets.connect(
                MARKET_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                open_timeout=20,
                max_size=None,
            ) as websocket:
                await websocket.send(json.dumps(subscribe_message))
                reconnect_backoff = 1.0
                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    except TimeoutError:
                        continue
                    payload = json.loads(raw)
                    for entry in payload if isinstance(payload, list) else [payload]:
                        if isinstance(entry, dict):
                            strategy.on_book_message(entry)
        except ConnectionClosed as exc:
            log(f"Market WS closed: {exc}. Reconnecting in {reconnect_backoff:.1f}s.", label="MARKET", tone="yellow")
        except Exception as exc:  # noqa: BLE001
            log(f"Market WS error: {exc}. Reconnecting in {reconnect_backoff:.1f}s.", label="MARKET", tone="yellow")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=reconnect_backoff)
        except TimeoutError:
            reconnect_backoff = min(reconnect_backoff * 2, 15.0)
            continue
        return


async def reconcile_orders(
    *,
    strategy: MakerQuoter,
    poll_seconds: float,
    stop_event: asyncio.Event,
    sync_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(sync_event.wait(), timeout=poll_seconds)
            sync_event.clear()
        except TimeoutError:
            pass
        if stop_event.is_set():
            return
        try:
            await strategy.sync_quotes()
        except Exception as exc:  # noqa: BLE001
            log(f"Reconcile error: {exc}", label="RECON", tone="yellow")


async def live_dashboard(*, strategy: MakerQuoter, stop_event: asyncio.Event) -> None:
    with Live(strategy.render(), console=CONSOLE, refresh_per_second=4, screen=False) as live:
        while not stop_event.is_set():
            live.update(strategy.render(), refresh=True)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.5)
            except TimeoutError:
                continue


async def run_market(*, args: argparse.Namespace, market: MarketWindow) -> None:
    env = load_env(Path(args.env_file))
    client = build_auth_client(env)
    strategy = MakerQuoter(
        market=market,
        client=client,
        proxy_address=env["POLYMARKET_PROXY_ADDRESS"],
        session_stats=args._session_stats,
        csv_recorder=args._csv_recorder,
        target_alpha=args.target_alpha,
        alpha_band_half_width=args.alpha_band_half_width,
        order_size_shares=args.order_size_shares,
        max_market_committed_usd=args.max_market_committed_usd,
        max_side_exposure_shares=args.max_side_exposure_shares,
        no_trade_first_seconds=args.no_trade_first_seconds,
        no_trade_last_seconds=args.no_trade_last_seconds,
    )
    await strategy.initialize_books()
    strategy.log_csv(
        event_type="market_start",
        timestamp=utc_now(),
        source_timestamp=utc_now(),
        note="market session attached",
        raw_payload={
            "window_start": market.window_start.isoformat(),
            "window_end": market.window_end.isoformat(),
        },
    )

    if utc_now() >= market.window_start:
        try:
            start_quote = await asyncio.to_thread(find_market_start_quote, asset=args.asset, market=market)
        except Exception as exc:  # noqa: BLE001
            log(f"Start-price backfill failed: {exc}", label="CHAINLINK", tone="yellow")
            start_quote = None
        if start_quote is not None:
            strategy.seed_reference_price(
                start_quote.price,
                note=f"Chainlink backfill @ {start_quote.timestamp.isoformat()}",
            )
        else:
            log(
                f"No Chainlink start tick found for {market.slug}; trading will wait for a reliable reference.",
                label="CHAINLINK",
                tone="yellow",
            )
            strategy.log_csv(
                event_type="reference_seed_miss",
                timestamp=utc_now(),
                source_timestamp=utc_now(),
                note="no Chainlink start tick found on late join",
            )

    chainlink_symbol = ASSET_RTDS_SYMBOLS[args.asset]
    stop_event = asyncio.Event()
    sync_event = asyncio.Event()
    strategy.attach_sync_event(sync_event)
    sync_event.set()
    tasks = [
        asyncio.create_task(stream_chainlink_prices(symbol=chainlink_symbol, strategy=strategy, stop_event=stop_event)),
        asyncio.create_task(stream_market_books(market=market, strategy=strategy, stop_event=stop_event)),
        asyncio.create_task(
            reconcile_orders(
                strategy=strategy,
                poll_seconds=args.reconcile_seconds,
                stop_event=stop_event,
                sync_event=sync_event,
            )
        ),
        asyncio.create_task(live_dashboard(strategy=strategy, stop_event=stop_event)),
    ]
    try:
        await asyncio.sleep(max((market.window_end - utc_now()).total_seconds(), 0.0) + args.post_close_grace_seconds)
    finally:
        stop_event.set()
        await strategy.cancel_market_orders()
        await asyncio.gather(*tasks, return_exceptions=True)
        await strategy.refresh_account_state()
        strategy.log_csv(
            event_type="market_close",
            timestamp=utc_now(),
            source_timestamp=utc_now(),
            note="market loop closed",
            raw_payload={
                "positions": {
                    book.label: strategy.positions[token_id].shares for token_id, book in strategy.books.items()
                }
            },
        )
        log(
            f"Closed {market.slug} | market committed {strategy.market_committed_usdc:.2f} | "
            f"positions: {', '.join(f'{book.label} {strategy.positions[token_id].shares:.4f}' for token_id, book in strategy.books.items())}",
            label="CLOSE",
            tone="cyan",
        )


async def run(args: argparse.Namespace) -> None:
    processed_market_ids: set[str] = set()
    completed_markets = 0
    session_started_at = utc_now()
    csv_dir = Path(args.csv_dir)
    csv_path = csv_dir / f"{session_started_at.strftime('%Y%m%dT%H%M%SZ')}__{args.asset}__maker_session.csv"
    session_stats = SessionStats()
    csv_recorder = SessionCsvRecorder(csv_path)
    setattr(args, "_session_stats", session_stats)
    setattr(args, "_csv_recorder", csv_recorder)
    log(f"Writing session CSV to {csv_path}", label="CSV", tone="cyan")
    csv_recorder.write_row(
        {
            "event_type": "session_start",
            "logged_at_utc": session_started_at.isoformat(),
            "source_timestamp_utc": session_started_at.isoformat(),
            "asset": args.asset,
            "target_alpha": args.target_alpha,
            "alpha_band_low": args.target_alpha - args.alpha_band_half_width,
            "alpha_band_high": args.target_alpha + args.alpha_band_half_width,
            "order_size": args.order_size_shares,
            "note": "maker session started",
            "raw_payload_json": json_compact(
                {
                    "env_file": args.env_file,
                    "asset": args.asset,
                    "target_alpha": args.target_alpha,
                    "alpha_band_half_width": args.alpha_band_half_width,
                    "order_size_shares": args.order_size_shares,
                    "max_market_committed_usd": args.max_market_committed_usd,
                    "max_side_exposure_shares": args.max_side_exposure_shares,
                    "no_trade_first_seconds": args.no_trade_first_seconds,
                    "no_trade_last_seconds": args.no_trade_last_seconds,
                    "reconcile_seconds": args.reconcile_seconds,
                    "discovery_poll_seconds": args.discovery_poll_seconds,
                    "post_close_grace_seconds": args.post_close_grace_seconds,
                    "csv_dir": args.csv_dir,
                    "max_markets": args.max_markets,
                    "dry_run": args.dry_run,
                }
            ),
        }
    )

    try:
        while True:
            if args.max_markets and completed_markets >= args.max_markets:
                return

            require_future_start = completed_markets == 0
            market = discover_next_market(args.asset, processed_market_ids, require_future_start=require_future_start)
            if market is None:
                if require_future_start:
                    log("No future recurring 5-minute market found yet. Waiting...", label="DISCOVER", tone="yellow")
                else:
                    log("No active or future recurring 5-minute market found yet. Waiting...", label="DISCOVER", tone="yellow")
                await asyncio.sleep(args.discovery_poll_seconds)
                continue

            processed_market_ids.add(market.market_id)
            log(
                f"Next market {market.slug} | {market.title} | starts {market.window_start.isoformat()} | "
                f"ends {market.window_end.isoformat()}",
                label="DISCOVER",
                tone="cyan",
            )
            csv_recorder.write_row(
                {
                    "event_type": "market_discovered",
                    "logged_at_utc": utc_now().isoformat(),
                    "source_timestamp_utc": utc_now().isoformat(),
                    "market_slug": market.slug,
                    "market_id": market.market_id,
                    "condition_id": market.condition_id,
                    "market_title": market.title,
                    "asset": market.asset,
                    "note": "market discovery",
                    "raw_payload_json": json_compact(
                        {
                            "window_start": market.window_start.isoformat(),
                            "window_end": market.window_end.isoformat(),
                            "token_ids": market.token_ids,
                            "outcome_labels": market.outcome_labels,
                        }
                    ),
                }
            )
            if args.dry_run:
                return

            connect_early_seconds = max((market.window_start - utc_now()).total_seconds(), 0.0)
            if connect_early_seconds > 0:
                log(
                    f"Connecting early and buffering Chainlink ticks for {format_duration(connect_early_seconds)} before market start.",
                    label="WAIT",
                    tone="cyan",
                )

            await run_market(args=args, market=market)
            completed_markets += 1
    finally:
        csv_recorder.write_row(
            {
                "event_type": "session_end",
                "logged_at_utc": utc_now().isoformat(),
                "source_timestamp_utc": utc_now().isoformat(),
                "asset": args.asset,
                "session_trade_count": session_stats.trade_count,
                "note": "maker session ended",
            }
        )
        csv_recorder.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a live-only maker engine for recurring 5-minute Polymarket Up/Down markets."
    )
    parser.add_argument("--asset", choices=["ethereum", "solana", "bitcoin"], default="ethereum")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--target-alpha", type=float, default=0.30)
    parser.add_argument("--alpha-band-half-width", type=float, default=0.05)
    parser.add_argument("--order-size-shares", type=float, default=5.0)
    parser.add_argument("--max-market-committed-usd", type=float, default=10.0)
    parser.add_argument("--max-side-exposure-shares", type=float, default=5.0)
    parser.add_argument("--no-trade-first-seconds", type=float, default=30.0)
    parser.add_argument("--no-trade-last-seconds", type=float, default=20.0)
    parser.add_argument("--reconcile-seconds", type=float, default=1.0)
    parser.add_argument("--discovery-poll-seconds", type=int, default=15)
    parser.add_argument("--post-close-grace-seconds", type=int, default=5)
    parser.add_argument("--csv-dir", default="scripts/data/maker_logs")
    parser.add_argument("--max-markets", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.order_size_shares < 5.0:
        log("Limit orders must be at least 5 shares on Polymarket.", label="ERROR", tone="red")
        return 2
    if args.max_side_exposure_shares <= 0:
        log("max-side-exposure-shares must be positive.", label="ERROR", tone="red")
        return 2
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        log("Stopped by user.", label="STOP", tone="yellow")
        return 130
    except Exception as exc:  # noqa: BLE001
        log(f"Fatal error: {exc}", label="ERROR", tone="red")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
