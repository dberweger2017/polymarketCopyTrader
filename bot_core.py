from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

getcontext().prec = 28

DEFAULT_TRACKED_WALLETS = [
    "0x2005d16a84ceefa912d4e380cd32e7ff827875ea",
    "0x507e52ef684ca2dd91f90a9d26d149dd3288beae",
    "0xee613b3fc183ee44f9da9c05f53e2da107e3debf",
    "0x941e9756c3588cdc806d9e1005528a5cdcfa1fbf",
    "0x204f72f35326db932158cba6adff0b9a1da95e14",
]
DATA_API_BASE = "https://data-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"
PUBLIC_URL_FILE = Path(__file__).resolve().parent / "public_url.txt"

ZERO = Decimal("0")
EPSILON = Decimal("0.00000001")
LIMIT_MIN_SHARES = Decimal("5")
CHART_POINT_LIMIT = 240
RECENT_CHART_WINDOW = timedelta(hours=6)
CHART_WINDOWS: dict[str, timedelta | None] = {
    "5m": timedelta(minutes=5),
    "10m": timedelta(minutes=10),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "3h": timedelta(hours=3),
    "6h": timedelta(hours=6),
    "1d": timedelta(days=1),
    "7d": timedelta(days=7),
    "since_start": None,
}


@dataclass(frozen=True)
class BotConfig:
    tracked_wallets: list[str]
    starting_cash: Decimal
    copy_notional_usd: Decimal
    max_slippage: Decimal
    relative_slippage_rate: Decimal
    poll_interval_seconds: float
    heartbeat_seconds: float
    recent_trade_limit: int
    equity_history_length: int = 240
    event_history_length: int = 40

    @classmethod
    def from_env(cls) -> "BotConfig":
        wallets = [
            wallet.strip().lower()
            for wallet in os.getenv("TRACKED_WALLETS", ",".join(DEFAULT_TRACKED_WALLETS)).split(",")
            if wallet.strip()
        ]
        return cls(
            tracked_wallets=wallets,
            starting_cash=Decimal(os.getenv("STARTING_CASH", "50")),
            copy_notional_usd=Decimal(os.getenv("COPY_NOTIONAL_USD", "1")),
            max_slippage=Decimal(os.getenv("MAX_SLIPPAGE", "0.01")),
            relative_slippage_rate=Decimal(os.getenv("RELATIVE_SLIPPAGE_RATE", "0.02")),
            poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "3")),
            heartbeat_seconds=float(os.getenv("HEARTBEAT_SECONDS", "30")),
            recent_trade_limit=int(os.getenv("RECENT_TRADE_LIMIT", "30")),
        )


@dataclass
class Position:
    asset_id: str
    title: str
    outcome: str
    shares: Decimal = ZERO
    last_mark: Decimal = ZERO
    total_cost: Decimal = ZERO


@dataclass
class Event:
    timestamp: str
    kind: str
    message: str


@dataclass(frozen=True)
class EquitySample:
    recorded_at: datetime
    equity: Decimal


class PaperPortfolio:
    def __init__(self, starting_cash: Decimal) -> None:
        self.cash = starting_cash
        self.positions: dict[str, Position] = {}
        self.realized_pnl = ZERO

    def buy(self, trade: dict[str, Any], shares: Decimal, spent: Decimal, mark: Decimal) -> None:
        position = self.positions.get(trade["asset_id"])
        if position is None:
            position = Position(
                asset_id=trade["asset_id"],
                title=trade["title"],
                outcome=trade["outcome"],
            )
            self.positions[trade["asset_id"]] = position
        position.title = trade["title"]
        position.outcome = trade["outcome"]
        position.shares += shares
        position.last_mark = mark
        position.total_cost += spent
        self.cash -= spent

    def sell(self, asset_id: str, shares: Decimal, proceeds: Decimal, mark: Decimal) -> Decimal:
        position = self.positions[asset_id]
        average_entry = position.total_cost / position.shares if position.shares > EPSILON else ZERO
        cost_removed = average_entry * shares
        position.shares -= shares
        position.last_mark = mark
        position.total_cost -= cost_removed
        self.cash += proceeds
        realized = proceeds - cost_removed
        self.realized_pnl += realized
        if position.shares <= EPSILON:
            del self.positions[asset_id]
        return realized

    def equity(self, marks: dict[str, Decimal]) -> Decimal:
        total = self.cash
        for asset_id, position in self.positions.items():
            mark = marks.get(asset_id, position.last_mark)
            position.last_mark = mark
            total += position.shares * mark
        return total


def decimalize(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def format_money(amount: Decimal) -> str:
    return f"${amount.quantize(Decimal('0.01'))}"


def format_signed_money(amount: Decimal) -> str:
    prefix = "+" if amount >= ZERO else "-"
    return f"{prefix}{format_money(abs(amount))}"


def format_decimal(amount: Decimal, places: str = "0.0000") -> str:
    return str(amount.quantize(Decimal(places)))


def format_percent(rate: Decimal) -> str:
    return f"{(rate * Decimal('100')).quantize(Decimal('0.01'))}%"


def describe_price_comparison(side: str, copied_price: Decimal, actual_price: Decimal) -> str:
    if copied_price <= ZERO:
        return "pricing unavailable"

    if side == "BUY":
        delta = (actual_price - copied_price) / copied_price
    elif side == "SELL":
        delta = (copied_price - actual_price) / copied_price
    else:
        return "pricing unavailable"

    if abs(delta) <= Decimal("0.00005"):
        return "matched copied price"
    if delta > ZERO:
        return f"worse by {format_percent(delta)}"
    return f"better by {format_percent(abs(delta))}"


def best_book_price(levels: list[tuple[Decimal, Decimal]]) -> Decimal:
    for price, size in levels:
        if price > ZERO and size > ZERO:
            return price
    return ZERO


def short_wallet(wallet: str) -> str:
    if len(wallet) <= 12:
        return wallet
    return f"{wallet[:8]}...{wallet[-6:]}"


def truncate_text(text: str, max_width: int) -> str:
    if max_width <= 1:
        return text[:max_width]
    if len(text) <= max_width:
        return text
    return f"{text[: max_width - 1]}…"


def sample_series(values: list[Decimal], target_width: int) -> list[Decimal]:
    if len(values) <= target_width:
        return values

    sampled: list[Decimal] = []
    max_index = len(values) - 1
    for slot in range(target_width):
        position = int(round(slot * max_index / (target_width - 1)))
        sampled.append(values[position])
    return sampled


def sample_equity_samples(samples: list[EquitySample], target_width: int) -> list[EquitySample]:
    if len(samples) <= target_width:
        return samples

    sampled: list[EquitySample] = []
    max_index = len(samples) - 1
    for slot in range(target_width):
        position = int(round(slot * max_index / (target_width - 1)))
        sampled.append(samples[position])
    return sampled


def http_json(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    method: str = "GET",
    payload: Any | None = None,
    timeout: int = 15,
) -> Any:
    url = path
    if params:
        url = f"{url}?{urlencode(params)}"

    data = None
    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=data, headers=headers, method=method)

    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc


class PolymarketCopyBot:
    def __init__(self, config: BotConfig | None = None) -> None:
        self.config = config or BotConfig.from_env()
        recent_history_length = max(
            360,
            int(RECENT_CHART_WINDOW.total_seconds() / max(self.config.poll_interval_seconds, 0.25)) + 2,
        )
        self.portfolio = PaperPortfolio(starting_cash=self.config.starting_cash)
        self.wallet_portfolios = {
            wallet: PaperPortfolio(starting_cash=self.config.starting_cash)
            for wallet in self.config.tracked_wallets
        }
        self.events: deque[Event] = deque(maxlen=self.config.event_history_length)
        self.equity_history_recent: deque[EquitySample] = deque(maxlen=recent_history_length)
        self.wallet_equity_history_recent = {
            wallet: deque(maxlen=recent_history_length)
            for wallet in self.config.tracked_wallets
        }
        self.equity_history_archive: list[EquitySample] = []
        self.wallet_equity_history_archive = {wallet: [] for wallet in self.config.tracked_wallets}
        self._last_archive_minute: str | None = None
        self.seen_trade_keys: set[str] = set()
        self.status = "IDLE"
        self.last_error = ""
        self.latest_action = "Waiting for new trades"
        self.last_poll_at: datetime | None = None
        self.last_trade_at: datetime | None = None
        self.seeded_count = 0
        self.current_equity = self.config.starting_cash
        self.current_wallet_equity = {
            wallet: self.config.starting_cash for wallet in self.config.tracked_wallets
        }
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, name="polymarket-copy-bot", daemon=True)
            self._thread.start()

    def stop(self, timeout: float = 10) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread:
            thread.join(timeout=timeout)

    def is_running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            positions = []
            summary_unrealized_pnl = ZERO
            for position in sorted(self.portfolio.positions.values(), key=lambda item: item.title.lower()):
                position_snapshot, unrealized = self._position_snapshot(position)
                summary_unrealized_pnl += unrealized
                positions.append(position_snapshot)

            wallet_performance = []
            for wallet in self.config.tracked_wallets:
                portfolio = self.wallet_portfolios[wallet]
                unrealized = self._portfolio_unrealized(portfolio)
                wallet_performance.append(
                    {
                        "wallet": wallet,
                        "cash": float(portfolio.cash),
                        "equity": float(self.current_wallet_equity[wallet]),
                        "realized_pnl": float(portfolio.realized_pnl),
                        "unrealized_pnl": float(unrealized),
                        "open_positions": len(portfolio.positions),
                        "equity_history": [
                            float(sample.equity) for sample in self.wallet_equity_history_recent[wallet]
                        ],
                    }
                )

            return {
                "summary": {
                    "status": self.status,
                    "cash": float(self.portfolio.cash),
                    "equity": float(self.current_equity),
                    "realized_pnl": float(self.portfolio.realized_pnl),
                    "unrealized_pnl": float(summary_unrealized_pnl),
                    "open_positions": len(self.portfolio.positions),
                    "last_poll_at": self._isoformat(self.last_poll_at),
                    "last_trade_at": self._isoformat(self.last_trade_at),
                    "seeded_count": self.seeded_count,
                    "latest_action": self.latest_action,
                    "last_error": self.last_error,
                },
                "config": {
                    "tracked_wallets": list(self.config.tracked_wallets),
                    "copy_notional_usd": float(self.config.copy_notional_usd),
                    "max_slippage": float(self.config.max_slippage),
                    "relative_slippage_rate": float(self.config.relative_slippage_rate),
                    "poll_interval_seconds": self.config.poll_interval_seconds,
                    "heartbeat_seconds": self.config.heartbeat_seconds,
                    "recent_trade_limit": self.config.recent_trade_limit,
                    "public_url": self._load_public_url(),
                },
                "positions": positions,
                "events": [
                    {"timestamp": event.timestamp, "kind": event.kind, "message": event.message}
                    for event in self.events
                ],
                "equity_history": [float(sample.equity) for sample in self.equity_history_recent],
                "wallet_performance": wallet_performance,
            }

    def chart_snapshot(self, series: str, window: str) -> dict[str, Any]:
        if window not in CHART_WINDOWS:
            raise ValueError(f"unsupported chart window {window}")

        with self._lock:
            recent_history, archive_history = self._history_sources_for_series(series)
            recent_history_copy = list(recent_history)
            archive_history_copy = list(archive_history)

        samples = self._chart_samples_for_window(
            recent_history=recent_history_copy,
            archive_history=archive_history_copy,
            window=window,
        )
        return {
            "series": series,
            "window": window,
            "samples": [
                {"timestamp": sample.recorded_at.isoformat(), "equity": float(sample.equity)}
                for sample in samples
            ],
        }

    def _run_loop(self) -> None:
        last_heartbeat_at = 0.0
        seeded = False

        while not self._stop_event.is_set():
            try:
                if not seeded:
                    seeded_trades = self._fetch_recent_trades(self.config.recent_trade_limit)
                    with self._lock:
                        self.seen_trade_keys = {self._trade_key(trade) for trade in seeded_trades}
                        self.seeded_count = len(seeded_trades)
                        self.status = "RUNNING"
                    self._mark_positions()
                    self._add_event(
                        "status",
                        f"Tracking {len(self.config.tracked_wallets)} wallets: "
                        f"{', '.join(short_wallet(wallet) for wallet in self.config.tracked_wallets)}. "
                        f"Seeded {len(seeded_trades)} recent trades so old fills are ignored.",
                    )
                    self._add_event(
                        "status",
                        f"Paper account ready with cash={format_money(self.portfolio.cash)} "
                        f"equity={format_money(self.current_equity)} "
                        f"copy_notional={format_money(self.config.copy_notional_usd)} "
                        f"slippage=min({format_decimal(self.config.max_slippage)}, "
                        f"{format_percent(self.config.relative_slippage_rate)} of price)",
                    )
                    seeded = True
                    last_heartbeat_at = time.time()

                recent_trades = self._fetch_recent_trades(self.config.recent_trade_limit)
                with self._lock:
                    self.last_poll_at = datetime.now().astimezone()
                self._process_new_trades(recent_trades)
                self._mark_positions()

                now = time.time()
                if now - last_heartbeat_at >= self.config.heartbeat_seconds:
                    self._add_event(
                        "status",
                        f"BALANCE cash={format_money(self.portfolio.cash)} "
                        f"equity={format_money(self.current_equity)} "
                        f"open_positions={len(self.portfolio.positions)} [{self._format_positions()}]",
                    )
                    last_heartbeat_at = now

                if self._stop_event.wait(self.config.poll_interval_seconds):
                    break
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self.status = "DEGRADED"
                    self.last_error = f"Loop error: {exc}"
                self._add_event("error", self.last_error)
                self._mark_positions()
                if self._stop_event.wait(self.config.poll_interval_seconds):
                    break
                with self._lock:
                    self.status = "RUNNING" if seeded else "STARTING"

        with self._lock:
            self.status = "STOPPED"
        self._mark_positions()
        self._add_event(
            "status",
            f"Stopped with cash={format_money(self.portfolio.cash)} "
            f"equity={format_money(self.current_equity)} "
            f"open_positions={len(self.portfolio.positions)} [{self._format_positions()}]",
        )

    def _add_event(self, kind: str, message: str) -> None:
        with self._lock:
            timestamp = datetime.now().astimezone().strftime("%H:%M:%S")
            event = Event(timestamp=timestamp, kind=kind.upper(), message=message)
            self.events.appendleft(event)
            if kind in {"copied", "skipped"}:
                self.latest_action = message
                self.last_trade_at = datetime.now().astimezone()
            if kind == "error":
                self.last_error = message

    def _record_equities(self, equity: Decimal, wallet_equities: dict[str, Decimal]) -> None:
        recorded_at = datetime.now().astimezone()
        minute_key = recorded_at.strftime("%Y-%m-%dT%H:%M")
        with self._lock:
            self.current_equity = equity
            current_sample = EquitySample(recorded_at=recorded_at, equity=equity)
            self.equity_history_recent.append(current_sample)
            should_archive = self._last_archive_minute != minute_key
            if should_archive:
                self.equity_history_archive.append(current_sample)
            for wallet, wallet_equity in wallet_equities.items():
                self.current_wallet_equity[wallet] = wallet_equity
                wallet_sample = EquitySample(recorded_at=recorded_at, equity=wallet_equity)
                self.wallet_equity_history_recent[wallet].append(wallet_sample)
                if should_archive:
                    self.wallet_equity_history_archive[wallet].append(wallet_sample)
            if should_archive:
                self._last_archive_minute = minute_key

    def _fetch_recent_trades(self, limit: int) -> list[dict[str, Any]]:
        trades: list[dict[str, Any]] = []
        for wallet in self.config.tracked_wallets:
            raw_trades = http_json(
                f"{DATA_API_BASE}/activity",
                params={"user": wallet, "limit": limit, "type": "TRADE"},
            )
            for raw_trade in raw_trades:
                trades.append(
                    {
                        "wallet": wallet,
                        "asset_id": raw_trade["asset"],
                        "title": raw_trade.get("title", "Unknown market"),
                        "outcome": raw_trade.get("outcome", "Unknown outcome"),
                        "side": str(raw_trade["side"]).upper(),
                        "price": decimalize(raw_trade["price"]),
                        "size": decimalize(raw_trade.get("size")),
                        "usdc_size": decimalize(raw_trade.get("usdcSize")),
                        "timestamp": int(raw_trade["timestamp"]),
                        "transaction_hash": raw_trade["transactionHash"],
                    }
                )

        trades.sort(
            key=lambda trade: (
                trade["timestamp"],
                trade["transaction_hash"],
                trade["wallet"],
                trade["asset_id"],
            )
        )
        return trades

    def _fetch_books(self, asset_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not asset_ids:
            return {}

        payload = [{"token_id": asset_id} for asset_id in asset_ids]
        raw_books = http_json(f"{CLOB_API_BASE}/books", method="POST", payload=payload)

        books: dict[str, dict[str, Any]] = {}
        for raw_book in raw_books:
            bids = [
                (decimalize(level["price"]), decimalize(level["size"]))
                for level in raw_book.get("bids", [])
            ]
            asks = [
                (decimalize(level["price"]), decimalize(level["size"]))
                for level in raw_book.get("asks", [])
            ]
            books[raw_book["asset_id"]] = {
                "bids": sorted(bids, key=lambda level: level[0], reverse=True),
                "asks": sorted(asks, key=lambda level: level[0]),
                "last_trade_price": decimalize(raw_book.get("last_trade_price")),
            }
        return books

    def _trade_key(self, trade: dict[str, Any]) -> str:
        return (
            f"{trade['wallet']}|{trade['timestamp']}|{trade['transaction_hash']}|{trade['asset_id']}|"
            f"{trade['side']}|{trade['price']}"
        )

    def _compute_mark_price(self, book: dict[str, Any]) -> Decimal:
        best_bid = book["bids"][0][0] if book["bids"] else ZERO
        best_ask = book["asks"][0][0] if book["asks"] else ZERO

        if best_bid > ZERO and best_ask > ZERO:
            return (best_bid + best_ask) / Decimal("2")
        if best_bid > ZERO:
            return best_bid
        if best_ask > ZERO:
            return best_ask
        return book["last_trade_price"]

    def _effective_slippage(self, limit_price: Decimal) -> Decimal:
        if limit_price <= ZERO:
            return ZERO
        return min(self.config.max_slippage, limit_price * self.config.relative_slippage_rate)

    def _mark_positions(self) -> None:
        marks: dict[str, Decimal] = {}
        with self._lock:
            asset_ids = list(self.portfolio.positions.keys())
            for portfolio in self.wallet_portfolios.values():
                asset_ids.extend(portfolio.positions.keys())

        if asset_ids:
            books = self._fetch_books(sorted(set(asset_ids)))
            for asset_id, book in books.items():
                marks[asset_id] = self._compute_mark_price(book)

        with self._lock:
            equity = self.portfolio.equity(marks)
            wallet_equities = {
                wallet: portfolio.equity(marks)
                for wallet, portfolio in self.wallet_portfolios.items()
            }
        self._record_equities(equity, wallet_equities)

    def _try_copy_trade_on_portfolio(
        self,
        portfolio: PaperPortfolio,
        trade: dict[str, Any],
        book: dict[str, Any],
    ) -> tuple[bool, str]:
        limit_price = trade["price"]
        if limit_price <= ZERO:
            return False, "trade price was zero or invalid"

        if trade["side"] == "BUY":
            allowed_slippage = self._effective_slippage(limit_price)
            max_fill_price = limit_price + allowed_slippage
            best_ask = best_book_price(book["asks"])

            existing_position = portfolio.positions.get(trade["asset_id"])
            if existing_position is not None and existing_position.shares > EPSILON:
                return (
                    False,
                    f"already holding this asset, so the repeat buy was ignored "
                    f"(copied {format_decimal(limit_price)}, max {format_decimal(max_fill_price)}, "
                    f"best ask {format_decimal(best_ask)})",
                )

            if portfolio.cash + EPSILON < self.config.copy_notional_usd:
                return (
                    False,
                    f"cash balance is below the $1 copy notional "
                    f"(copied {format_decimal(limit_price)}, max {format_decimal(max_fill_price)}, "
                    f"best ask {format_decimal(best_ask)})",
                )

            filled_shares = ZERO
            spent = ZERO
            order_mode = "market-style"
            limit_min_cost = self._minimum_limit_fill_cost(book["asks"], max_fill_price, LIMIT_MIN_SHARES)
            if limit_min_cost is not None and limit_min_cost <= self.config.copy_notional_usd + EPSILON:
                order_mode = "limit-style"
                filled_shares, spent = self._fill_buy_to_share_target(
                    asks=book["asks"],
                    max_fill_price=max_fill_price,
                    target_shares=LIMIT_MIN_SHARES,
                )
            else:
                filled_shares, spent = self._fill_buy_to_notional_target(
                    asks=book["asks"],
                    max_fill_price=max_fill_price,
                    target_notional=self.config.copy_notional_usd,
                )

            if order_mode == "market-style" and spent + EPSILON < self.config.copy_notional_usd:
                return (
                    False,
                    f"no ask liquidity at or below copied limit {format_decimal(limit_price)} + "
                    f"{format_decimal(allowed_slippage)} slippage "
                    f"(best ask {format_decimal(best_ask)}, max {format_decimal(max_fill_price)})",
                )
            if order_mode == "limit-style" and filled_shares + EPSILON < LIMIT_MIN_SHARES:
                return (
                    False,
                    f"not enough ask liquidity to reach the 5-share limit minimum "
                    f"(best ask {format_decimal(best_ask)}, max {format_decimal(max_fill_price)})",
                )

            average_price = spent / filled_shares
            portfolio.buy(trade, filled_shares, spent, average_price)
            return (
                True,
                f"paper BUY {format_decimal(filled_shares, '0.000000')} shares at "
                f"{format_decimal(average_price)} for {format_money(spent)} "
                f"(copied {format_decimal(limit_price)}, max {format_decimal(max_fill_price)}, "
                f"best ask {format_decimal(best_ask)}, slip {format_decimal(allowed_slippage)}, "
                f"simulated {order_mode} minimum, "
                f"{describe_price_comparison('BUY', limit_price, average_price)})",
            )

        if trade["side"] == "SELL":
            allowed_slippage = self._effective_slippage(limit_price)
            min_fill_price = max(limit_price - allowed_slippage, ZERO)
            best_bid = best_book_price(book["bids"])

            position = portfolio.positions.get(trade["asset_id"])
            if position is None or position.shares <= EPSILON:
                return (
                    False,
                    f"no inventory in this asset, so the sell copy was skipped "
                    f"(copied {format_decimal(limit_price)}, min {format_decimal(min_fill_price)}, "
                    f"best bid {format_decimal(best_bid)})",
                )

            shares_to_sell = position.shares
            sold_shares = ZERO
            proceeds = ZERO

            for price, size in book["bids"]:
                if price < min_fill_price:
                    break
                if price <= ZERO or size <= ZERO or shares_to_sell <= EPSILON:
                    continue

                shares = min(size, shares_to_sell)
                if shares <= ZERO:
                    continue

                sold_shares += shares
                proceeds += price * shares
                shares_to_sell -= shares

                if shares_to_sell <= EPSILON:
                    break

            if shares_to_sell > EPSILON:
                return (
                    False,
                    f"not enough bid liquidity to fully exit at or above copied limit {format_decimal(limit_price)} - "
                    f"{format_decimal(allowed_slippage)} slippage "
                    f"(best bid {format_decimal(best_bid)}, min {format_decimal(min_fill_price)})",
                )

            average_price = proceeds / sold_shares
            realized = portfolio.sell(trade["asset_id"], sold_shares, proceeds, average_price)
            return (
                True,
                f"paper CLOSE {format_decimal(sold_shares, '0.000000')} shares at "
                f"{format_decimal(average_price)} for {format_money(proceeds)} "
                f"(copied {format_decimal(limit_price)}, min {format_decimal(min_fill_price)}, "
                f"best bid {format_decimal(best_bid)}, slip {format_decimal(allowed_slippage)}, "
                f"{describe_price_comparison('SELL', limit_price, average_price)}) | "
                f"realized {format_signed_money(realized)}",
            )

        return False, f"unsupported side {trade['side']}"

    def _process_new_trades(self, trades: list[dict[str, Any]]) -> None:
        with self._lock:
            unseen_trades = [trade for trade in trades if self._trade_key(trade) not in self.seen_trade_keys]
        if not unseen_trades:
            return

        books = self._fetch_books(sorted({trade["asset_id"] for trade in unseen_trades}))

        for trade in unseen_trades:
            with self._lock:
                self.seen_trade_keys.add(self._trade_key(trade))
            book = books.get(trade["asset_id"])
            if book is None:
                self._add_event(
                    "skipped",
                    f"{short_wallet(trade['wallet'])} {trade['side']} {trade['outcome']} @ "
                    f"{format_decimal(trade['price'])} in {trade['title']} | skipped: missing order book",
                )
                continue

            with self._lock:
                copied, result = self._try_copy_trade_on_portfolio(self.portfolio, trade, book)
                self._try_copy_trade_on_portfolio(self.wallet_portfolios[trade["wallet"]], trade, book)

            self._mark_positions()
            status = "copied" if copied else "skipped"
            self._add_event(
                status,
                f"{short_wallet(trade['wallet'])} {trade['side']} {trade['outcome']} @ "
                f"{format_decimal(trade['price'])} ({format_money(trade['usdc_size'])} original) "
                f"in {trade['title']} | {status}: {result} | "
                f"cash={format_money(self.portfolio.cash)} equity={format_money(self.current_equity)}",
            )

    def _format_positions(self) -> str:
        if not self.portfolio.positions:
            return "none"

        rendered: list[str] = []
        for position in sorted(self.portfolio.positions.values(), key=lambda current: current.title.lower())[:5]:
            rendered.append(
                f"{position.outcome} ({position.title}) {format_decimal(position.shares, '0.000000')}"
            )
        suffix = "" if len(self.portfolio.positions) <= 5 else f" +{len(self.portfolio.positions) - 5} more"
        return ", ".join(rendered) + suffix

    @staticmethod
    def _isoformat(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

    @staticmethod
    def _load_public_url() -> str | None:
        configured = os.getenv("PUBLIC_APP_URL", "").strip()
        if configured:
            return configured
        if PUBLIC_URL_FILE.exists():
            value = PUBLIC_URL_FILE.read_text(encoding="utf-8").strip()
            return value or None
        return None

    @staticmethod
    def _position_snapshot(position: Position) -> tuple[dict[str, Any], Decimal]:
        avg_entry = position.total_cost / position.shares if position.shares > EPSILON else ZERO
        value = position.last_mark * position.shares
        unrealized = value - position.total_cost
        return (
            {
                "asset_id": position.asset_id,
                "title": position.title,
                "outcome": position.outcome,
                "shares": float(position.shares),
                "avg_entry": float(avg_entry),
                "mark": float(position.last_mark),
                "value": float(value),
                "unrealized_pnl": float(unrealized),
            },
            unrealized,
        )

    @staticmethod
    def _portfolio_unrealized(portfolio: PaperPortfolio) -> Decimal:
        unrealized = ZERO
        for position in portfolio.positions.values():
            unrealized += (position.last_mark * position.shares) - position.total_cost
        return unrealized

    def _history_sources_for_series(self, series: str) -> tuple[deque[EquitySample], list[EquitySample]]:
        if series == "all":
            return self.equity_history_recent, self.equity_history_archive
        if series not in self.wallet_equity_history_recent:
            raise ValueError(f"unknown chart series {series}")
        return self.wallet_equity_history_recent[series], self.wallet_equity_history_archive[series]

    def _chart_samples_for_window(
        self,
        *,
        recent_history: list[EquitySample],
        archive_history: list[EquitySample],
        window: str,
    ) -> list[EquitySample]:
        delta = CHART_WINDOWS[window]
        cutoff = datetime.now().astimezone() - delta if delta is not None else None

        if delta is not None and delta <= RECENT_CHART_WINDOW:
            window_samples = self._filter_samples(recent_history, cutoff)
            return sample_equity_samples(window_samples, CHART_POINT_LIMIT)

        archived_window = self._filter_samples(archive_history, cutoff)
        latest_archived_at = archived_window[-1].recorded_at if archived_window else None
        recent_tail = [
            sample
            for sample in self._filter_samples(recent_history, cutoff)
            if latest_archived_at is None or sample.recorded_at > latest_archived_at
        ]
        return sample_equity_samples(archived_window + recent_tail, CHART_POINT_LIMIT)

    @staticmethod
    def _filter_samples(
        samples: list[EquitySample],
        cutoff: datetime | None,
    ) -> list[EquitySample]:
        if cutoff is None:
            return samples
        return [sample for sample in samples if sample.recorded_at >= cutoff]

    @staticmethod
    def _minimum_limit_fill_cost(
        asks: list[tuple[Decimal, Decimal]],
        max_fill_price: Decimal,
        target_shares: Decimal,
    ) -> Decimal | None:
        remaining_shares = target_shares
        spent = ZERO

        for price, size in asks:
            if price > max_fill_price:
                break
            if price <= ZERO or size <= ZERO:
                continue

            fill_shares = min(size, remaining_shares)
            spent += fill_shares * price
            remaining_shares -= fill_shares
            if remaining_shares <= EPSILON:
                return spent

        return None

    @staticmethod
    def _fill_buy_to_share_target(
        *,
        asks: list[tuple[Decimal, Decimal]],
        max_fill_price: Decimal,
        target_shares: Decimal,
    ) -> tuple[Decimal, Decimal]:
        remaining_shares = target_shares
        filled_shares = ZERO
        spent = ZERO

        for price, size in asks:
            if price > max_fill_price:
                break
            if price <= ZERO or size <= ZERO:
                continue

            fill_shares = min(size, remaining_shares)
            if fill_shares <= ZERO:
                continue

            filled_shares += fill_shares
            spent += fill_shares * price
            remaining_shares -= fill_shares
            if remaining_shares <= EPSILON:
                break

        return filled_shares, spent

    @staticmethod
    def _fill_buy_to_notional_target(
        *,
        asks: list[tuple[Decimal, Decimal]],
        max_fill_price: Decimal,
        target_notional: Decimal,
    ) -> tuple[Decimal, Decimal]:
        remaining_notional = target_notional
        filled_shares = ZERO
        spent = ZERO

        for price, size in asks:
            if price > max_fill_price:
                break
            if price <= ZERO or size <= ZERO:
                continue

            max_notional = price * size
            fill_notional = min(remaining_notional, max_notional)
            if fill_notional <= ZERO:
                continue

            shares = fill_notional / price
            filled_shares += shares
            spent += fill_notional
            remaining_notional -= fill_notional
            if remaining_notional <= EPSILON:
                break

        return filled_shares, spent
