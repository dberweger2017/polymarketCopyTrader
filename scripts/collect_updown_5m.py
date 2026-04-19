#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
import shutil
import subprocess
import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from polymarket_live import (
    LiveDependencyError,
    OrderType,
    UserChannel,
    build_auth_client,
    build_live_account_state,
    calculate_live_sell_shares,
    ensure_live_dependencies,
    execute_market_order,
    fetch_live_account_state,
    load_env as load_live_env,
    wait_for_order_confirmation,
)

try:
    import pandas as pd
    import plotext as plt
    import websockets
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from tqdm import tqdm
    from websockets.exceptions import ConnectionClosed
except ImportError as exc:  # pragma: no cover - runtime environment guard
    raise SystemExit(
        "Missing dependency: pandas/websockets/tqdm/rich/plotext. Create a local venv and run "
        "`.venv/bin/python -m pip install -r scripts/requirements-market-recorder.txt`."
    ) from exc


GAMMA_API_BASE = "https://gamma-api.polymarket.com"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
COINBASE_MARKET_WS_URL = "wss://advanced-trade-ws.coinbase.com"
CHAINLINK_API_BASE = "https://data.chain.link/api"
CHAINLINK_LIVE_QUERY = "LIVE_STREAM_REPORTS_QUERY"
DISCOVERY_LIMIT = 600
DISCOVERY_TAGS = frozenset({"up-or-down", "5M", "recurring"})
WINDOW_DURATION = timedelta(minutes=5)
RECURRING_MARKET_STEP_SECONDS = int(WINDOW_DURATION.total_seconds())
NETWORK_RECOVERY_SLEEP_SECONDS = 300
OFFICIAL_RESOLUTION_PRICE_THRESHOLD = 0.99
PAPER_TRADE_NOTIONAL_USD = 1.0
MARKET_ORDER_EXECUTION_DELAY_SECONDS = 0.5
MARKET_ORDER_TRACE_DELAYS_SECONDS = (0.0, 0.25, 0.5, 0.75)
NO_NEW_TRADES_LAST_SECONDS = 5.0
LIVE_ACCOUNT_REFRESH_SECONDS = 1.0
LIVE_RESERVED_CASH_EPSILON_USDC = 1e-6
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
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


@dataclass
class RecorderStats:
    total_rows: int = 0
    session_rows: int = 0
    message_rows: int = 0
    book_level_rows: int = 0
    change_level_rows: int = 0
    underlying_rows: int = 0


@dataclass(frozen=True)
class StreamStats:
    reconnects: int
    started_at: datetime
    finished_at: datetime


@dataclass(frozen=True)
class UnderlyingConfig:
    asset: str
    symbol: str
    source: str
    feed_id: str
    stream_slug: str


@dataclass(frozen=True)
class UnderlyingQuote:
    source: str
    symbol: str
    feed_id: str
    timestamp: datetime
    price: str
    bid: str
    ask: str
    raw_json: str


@dataclass(frozen=True)
class OfficialResolution:
    up_wins: bool
    price_to_beat: float | None = None
    final_price: float | None = None


@dataclass(frozen=True)
class SessionAlphaStats:
    completed_market_max_alphas: tuple[float, ...] = ()

    @property
    def average_max_alpha(self) -> float | None:
        if not self.completed_market_max_alphas:
            return None
        return sum(self.completed_market_max_alphas) / len(self.completed_market_max_alphas)

    def with_market(self, market_max_alpha: float) -> "SessionAlphaStats":
        return SessionAlphaStats(
            completed_market_max_alphas=self.completed_market_max_alphas + (market_max_alpha,),
        )


@dataclass(frozen=True)
class MarketPerformanceStats:
    trade_count: int = 0
    closed_trade_units: float = 0.0
    winning_trade_units: float = 0.0
    buy_trade_count: int = 0
    buy_alpha_sum: float = 0.0
    market_return: float = 0.0


@dataclass(frozen=True)
class SessionPerformanceStats:
    completed_market_count: int = 0
    total_trade_count: int = 0
    total_closed_trade_units: float = 0.0
    total_winning_trade_units: float = 0.0
    total_buy_trade_count: int = 0
    total_buy_alpha_sum: float = 0.0
    total_market_return_sum: float = 0.0

    @property
    def average_win_rate_per_trade(self) -> float | None:
        if self.total_closed_trade_units <= 0:
            return None
        return self.total_winning_trade_units / self.total_closed_trade_units

    @property
    def average_trades_per_market(self) -> float | None:
        if self.completed_market_count <= 0:
            return None
        return self.total_trade_count / self.completed_market_count

    @property
    def average_buy_alpha(self) -> float | None:
        if self.total_buy_trade_count <= 0:
            return None
        return self.total_buy_alpha_sum / self.total_buy_trade_count

    @property
    def average_return_per_market(self) -> float | None:
        if self.completed_market_count <= 0:
            return None
        return self.total_market_return_sum / self.completed_market_count

    def with_market(self, market_stats: MarketPerformanceStats) -> "SessionPerformanceStats":
        return SessionPerformanceStats(
            completed_market_count=self.completed_market_count + 1,
            total_trade_count=self.total_trade_count + market_stats.trade_count,
            total_closed_trade_units=self.total_closed_trade_units + market_stats.closed_trade_units,
            total_winning_trade_units=self.total_winning_trade_units + market_stats.winning_trade_units,
            total_buy_trade_count=self.total_buy_trade_count + market_stats.buy_trade_count,
            total_buy_alpha_sum=self.total_buy_alpha_sum + market_stats.buy_alpha_sum,
            total_market_return_sum=self.total_market_return_sum + market_stats.market_return,
        )

    def including_market(self, market_stats: MarketPerformanceStats) -> "SessionPerformanceStats":
        return SessionPerformanceStats(
            completed_market_count=self.completed_market_count + 1,
            total_trade_count=self.total_trade_count + market_stats.trade_count,
            total_closed_trade_units=self.total_closed_trade_units + market_stats.closed_trade_units,
            total_winning_trade_units=self.total_winning_trade_units + market_stats.winning_trade_units,
            total_buy_trade_count=self.total_buy_trade_count + market_stats.buy_trade_count,
            total_buy_alpha_sum=self.total_buy_alpha_sum + market_stats.buy_alpha_sum,
            total_market_return_sum=self.total_market_return_sum + market_stats.market_return,
        )


RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
WHITE = "\033[37m"

UNDERLYING_CONFIGS: dict[str, UnderlyingConfig] = {
    "ethereum": UnderlyingConfig(
        asset="ethereum",
        symbol="ETH/USD",
        source="coinbase_advanced_trade_ws",
        feed_id="ETH-USD",
        stream_slug="ticker",
    ),
    "solana": UnderlyingConfig(
        asset="solana",
        symbol="SOL/USD",
        source="coinbase_advanced_trade_ws",
        feed_id="SOL-USD",
        stream_slug="ticker",
    ),
}

CHAINLINK_UNDERLYING_CONFIGS: dict[str, UnderlyingConfig] = {
    "ethereum": UnderlyingConfig(
        asset="ethereum",
        symbol="ETH/USD",
        source="chainlink_public_delayed",
        feed_id="0x000362205e10b3a147d02792eccee483dca6c7b44ecce7012cb8c6e0b68b3ae9",
        stream_slug="eth-usd-cexprice-streams",
    ),
    "solana": UnderlyingConfig(
        asset="solana",
        symbol="SOL/USD",
        source="chainlink_public_delayed",
        feed_id="0x0003b778d3f6b2ac4991302b89cb313f99a42467d6c9c5f96f57c29c0d2bc24f",
        stream_slug="sol-usd-cexprice-streams",
    ),
}

CONSOLE = Console()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def supports_color() -> bool:
    return sys.stdout.isatty()


def color(text: str, tone: str) -> str:
    if not supports_color():
        return text
    palette = {
        "bold": BOLD,
        "dim": DIM,
        "red": RED,
        "green": GREEN,
        "yellow": YELLOW,
        "cyan": CYAN,
        "white": WHITE,
    }
    prefix = palette.get(tone, "")
    return f"{prefix}{text}{RESET}" if prefix else text


def emit(message: str) -> None:
    if supports_color():
        tqdm.write(message)
    else:
        print(message, flush=True)


def log(message: str, *, label: str = "INFO", tone: str = "white") -> None:
    timestamp = utc_now().strftime("%Y-%m-%d %H:%M:%S")
    emit(f"[{timestamp} UTC] {color(f'{label:<10}', tone)} {message}")


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_duration(total_seconds: float) -> str:
    seconds = max(int(total_seconds), 0)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:d}h {minutes:02d}m {seconds:02d}s"
    if minutes > 0:
        return f"{minutes:d}m {seconds:02d}s"
    return f"{seconds:d}s"


def print_banner(asset: str, output_path: Path | None, *, max_markets: int, write_csv: bool) -> None:
    width = max(88, min(shutil.get_terminal_size((120, 40)).columns, 140))
    run_mode = "forever" if max_markets == 0 else str(max_markets)
    emit(color("POLYMARKET LIVE PAPER TRADER  Version 2.0", "bold"))
    emit(color("-" * width, "dim"))
    emit(
        f"{color('Asset:', 'dim')} {color(asset, 'cyan')}  "
        f"{color('Window:', 'dim')} 5-minute recurring Up/Down  "
        f"{color('Max markets:', 'dim')} {run_mode}"
    )
    emit(f"{color('CSV:', 'dim')} {'enabled' if write_csv else 'off'}")
    if write_csv and output_path is not None:
        emit(f"{color('CSV dir:', 'dim')} {output_path}")
    emit(color("-" * width, "dim"))


def log_market_window(
    cycle_number: int,
    market: MarketWindow,
    *,
    phase: str,
    tone: str,
    output_path: Path | None = None,
) -> None:
    log(
        f"Cycle #{cycle_number} | {market.title}",
        label=phase,
        tone=tone,
    )
    details = (
        f"window {format_utc(market.window_start)} -> {format_utc(market.window_end)} UTC  "
        f"| slug={market.slug}  | market_id={market.market_id}"
    )
    if output_path is not None:
        details = f"{details}  | csv={output_path}"
    log(details, label="MARKET", tone="dim")


SUMMARY_CSV_FIELDNAMES = (
    "recorded_at_utc",
    "summary_phase",
    "cycle_number",
    "session_asset",
    "session_event_id",
    "session_market_id",
    "session_condition_id",
    "session_title",
    "session_slug",
    "window_start_utc",
    "window_end_utc",
    "time_left",
    "balance",
    "cash",
    "open_value",
    "exposure",
    "lifetime_buys",
    "realized",
    "unrealized",
    "trades",
    "market_max_alpha",
    "avg_max_alpha",
    "first_entry_rule",
    "total_trades",
    "avg_win_rate_per_trade",
    "avg_trades_per_market",
    "avg_buy_alpha",
    "balance_std_dev",
    "overall_return",
    "return_per_market",
    "return_per_day",
    "this_market",
    "start_ref",
    "current_ref",
    "sigma_per_sqrt_s",
    "fair_up",
    "cycle_duration",
    "reconnects",
)


def upsert_market_summary_row(summary_path: Path, row: dict[str, str]) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = summary_path.with_suffix(f"{summary_path.suffix}.tmp")
    new_row = pd.DataFrame([row], columns=list(SUMMARY_CSV_FIELDNAMES))
    if summary_path.exists():
        existing = pd.read_csv(summary_path, dtype=str).fillna("")
        if "balance" not in existing.columns and "final_balance" in existing.columns:
            existing["balance"] = existing["final_balance"]
        existing = existing.reindex(columns=list(SUMMARY_CSV_FIELDNAMES), fill_value="")
        existing = existing[existing["session_market_id"] != row["session_market_id"]]
        result = pd.concat([existing, new_row], ignore_index=True)
    else:
        result = new_row
    result.to_csv(temp_path, index=False)
    temp_path.replace(summary_path)


def build_market_terminal_summary(
    *,
    prefix: str,
    strategy: "LivePaperStrategy",
    session_alpha_stats: SessionAlphaStats,
    session_performance_stats: SessionPerformanceStats,
    cycle_number: int | None = None,
    stream_stats: StreamStats | None = None,
) -> tuple[str, str]:
    average_win_rate_per_trade = session_performance_stats.average_win_rate_per_trade
    average_trades_per_market = session_performance_stats.average_trades_per_market
    average_buy_alpha = session_performance_stats.average_buy_alpha
    average_return_per_market = session_performance_stats.average_return_per_market
    overall_return = strategy.overall_return()
    market_return = strategy.current_market_return()
    balance_std_dev = strategy.balance_std_dev()
    snapshot_time = strategy.latest_underlying_timestamp or utc_now()
    remaining = max((strategy.market.window_end - snapshot_time).total_seconds(), 0.0)
    fair_up = strategy.fair_up_probability(snapshot_time)
    sigma_per_sqrt_second = strategy.sigma_per_sqrt_second()
    avg_win_rate_text = format_percent(average_win_rate_per_trade) if average_win_rate_per_trade is not None else "-"
    avg_trades_text = f"{average_trades_per_market:.2f}" if average_trades_per_market is not None else "-"
    avg_buy_alpha_text = f"{average_buy_alpha:+.4f}" if average_buy_alpha is not None else "-"
    avg_max_alpha_text = (
        f"{session_alpha_stats.average_max_alpha:.4f}"
        if session_alpha_stats.average_max_alpha is not None
        else "-"
    )
    balance_std_text = format_money(balance_std_dev) if balance_std_dev is not None else "-"
    overall_return_text = format_percent(overall_return) if overall_return is not None else "-"
    return_market_text = format_percent(average_return_per_market) if average_return_per_market is not None else "-"
    return_day_text = format_percent(average_return_per_market * 288.0) if average_return_per_market is not None else "-"
    this_market_text = format_percent(market_return) if market_return is not None else "-"
    total_trades_text = str(session_performance_stats.total_trade_count)
    open_value_text = format_money(strategy.current_liquidation_value())
    exposure_text = f"{format_money(strategy.current_market_exposure())} / {format_money(strategy.max_market_exposure_usd)}"
    lifetime_buys_text = f"{format_money(strategy.market_buy_budget_used_usd)} / {format_money(strategy.max_market_exposure_usd)}"
    unrealized_text = format_money(strategy.unrealized_pnl())
    first_entry_rule_text = "armed" if not strategy.initial_trade_taken else "used"
    start_ref_text = f"{strategy.start_underlying_price:.4f}" if strategy.start_underlying_price is not None else "-"
    current_ref_text = f"{strategy.latest_underlying_price:.4f}" if strategy.latest_underlying_price is not None else "-"
    sigma_text = f"{sigma_per_sqrt_second:.6f}" if sigma_per_sqrt_second is not None else "-"
    fair_up_text = f"{fair_up:.4f}" if fair_up is not None else "-"
    cycle_text = f"Cycle #{cycle_number} " if cycle_number is not None else ""
    summary_text = (
        f"{cycle_text}{prefix} "
        f"| Mode={strategy.execution_mode} "
        f"| Market={strategy.market.title} "
        f"| Time Left={format_duration(remaining)} "
        f"| balance={format_money(strategy.current_balance())} "
        f"| cash={format_money(strategy.cash)} "
        f"| Open Value={open_value_text} "
        f"| Exposure={exposure_text} "
        f"| Lifetime Buys={lifetime_buys_text} "
        f"| realized={format_money(strategy.realized_pnl)} "
        f"| Unrealized={unrealized_text} "
        f"| Trades={strategy.trade_count} "
        f"| market_max_alpha={strategy.market_max_buy_alpha_net:.4f} "
        f"| Avg Max Alpha={avg_max_alpha_text} "
        f"| First Entry Rule={first_entry_rule_text} "
        f"| Total Trades={total_trades_text} "
        f"| Avg Win Rate/Trade={avg_win_rate_text} "
        f"| Avg Trades/Market={avg_trades_text} "
        f"| Avg Buy Alpha={avg_buy_alpha_text} "
        f"| Balance Std Dev={balance_std_text} "
        f"| Overall Return={overall_return_text} "
        f"| Return/Market={return_market_text} "
        f"| Return/Day={return_day_text} "
        f"| This Market={this_market_text} "
        f"| Start Ref={start_ref_text} "
        f"| Current Ref={current_ref_text} "
        f"| Sigma/sqrt(s)={sigma_text} "
        f"| Fair Up={fair_up_text}"
    )
    if strategy.uses_live_execution():
        summary_text = (
            f"{summary_text} "
            f"| Marked Open={format_money(strategy.marked_open_value())} "
            f"| Exec Open={format_money(strategy.executable_open_value())} "
            f"| Reserved={format_money(strategy.open_order_reserved_cash())}"
        )
        if strategy.average_live_buy_match_ms() is not None:
            summary_text = f"{summary_text} | Avg Buy Match ms={strategy.average_live_buy_match_ms():.1f}"
        else:
            summary_text = f"{summary_text} | Avg Buy Match ms=-"
        if strategy.average_live_sell_match_ms() is not None:
            summary_text = f"{summary_text} | Avg Sell Match ms={strategy.average_live_sell_match_ms():.1f}"
        else:
            summary_text = f"{summary_text} | Avg Sell Match ms=-"
    if stream_stats is not None:
        summary_text = (
            f"{summary_text} "
            f"| reconnects={stream_stats.reconnects}"
        )
    return summary_text, format_money(strategy.current_balance())


def build_market_summary_row(
    *,
    recorded_at: datetime,
    summary_phase: str,
    cycle_number: int,
    market: MarketWindow,
    strategy: "LivePaperStrategy",
    session_alpha_stats: SessionAlphaStats,
    session_performance_stats: SessionPerformanceStats,
    stream_stats: StreamStats | None = None,
) -> dict[str, str]:
    average_win_rate_per_trade = session_performance_stats.average_win_rate_per_trade
    average_trades_per_market = session_performance_stats.average_trades_per_market
    average_buy_alpha = session_performance_stats.average_buy_alpha
    average_return_per_market = session_performance_stats.average_return_per_market
    overall_return = strategy.overall_return()
    market_return = strategy.current_market_return()
    balance_std_dev = strategy.balance_std_dev()
    snapshot_time = strategy.latest_underlying_timestamp or recorded_at
    fair_up = strategy.fair_up_probability(snapshot_time)
    sigma_per_sqrt_second = strategy.sigma_per_sqrt_second()

    return {
        "recorded_at_utc": recorded_at.isoformat(),
        "summary_phase": summary_phase,
        "cycle_number": str(cycle_number),
        "session_asset": market.asset,
        "session_event_id": market.event_id,
        "session_market_id": market.market_id,
        "session_condition_id": market.condition_id,
        "session_title": market.title,
        "session_slug": market.slug,
        "window_start_utc": market.window_start.isoformat(),
        "window_end_utc": market.window_end.isoformat(),
        "time_left": format_duration(max((market.window_end - snapshot_time).total_seconds(), 0.0)),
        "balance": format_money(strategy.current_balance()),
        "cash": format_money(strategy.cash),
        "open_value": (
            format_money(strategy.executable_open_value())
            if strategy.uses_live_execution()
            else format_money(strategy.current_liquidation_value())
        ),
        "exposure": f"{format_money(strategy.current_market_exposure())} / {format_money(strategy.max_market_exposure_usd)}",
        "lifetime_buys": f"{format_money(strategy.market_buy_budget_used_usd)} / {format_money(strategy.max_market_exposure_usd)}",
        "realized": format_money(strategy.realized_pnl),
        "unrealized": format_money(strategy.unrealized_pnl()),
        "trades": str(strategy.trade_count),
        "market_max_alpha": f"{strategy.market_max_buy_alpha_net:.4f}",
        "avg_max_alpha": (
            f"{session_alpha_stats.average_max_alpha:.4f}"
            if session_alpha_stats.average_max_alpha is not None
            else "-"
        ),
        "first_entry_rule": "armed" if not strategy.initial_trade_taken else "used",
        "total_trades": str(session_performance_stats.total_trade_count),
        "avg_win_rate_per_trade": (
            format_percent(average_win_rate_per_trade)
            if average_win_rate_per_trade is not None
            else "-"
        ),
        "avg_trades_per_market": (
            f"{average_trades_per_market:.2f}"
            if average_trades_per_market is not None
            else "-"
        ),
        "avg_buy_alpha": f"{average_buy_alpha:+.4f}" if average_buy_alpha is not None else "-",
        "balance_std_dev": format_money(balance_std_dev) if balance_std_dev is not None else "-",
        "overall_return": format_percent(overall_return) if overall_return is not None else "-",
        "return_per_market": format_percent(average_return_per_market) if average_return_per_market is not None else "-",
        "return_per_day": (
            format_percent(average_return_per_market * 288.0)
            if average_return_per_market is not None
            else "-"
        ),
        "this_market": format_percent(market_return) if market_return is not None else "-",
        "start_ref": f"{strategy.start_underlying_price:.4f}" if strategy.start_underlying_price is not None else "-",
        "current_ref": f"{strategy.latest_underlying_price:.4f}" if strategy.latest_underlying_price is not None else "-",
        "sigma_per_sqrt_s": f"{sigma_per_sqrt_second:.6f}" if sigma_per_sqrt_second is not None else "-",
        "fair_up": f"{fair_up:.4f}" if fair_up is not None else "-",
        "cycle_duration": (
            format_duration((stream_stats.finished_at - stream_stats.started_at).total_seconds())
            if stream_stats is not None
            else "-"
        ),
        "reconnects": str(stream_stats.reconnects) if stream_stats is not None else "-",
    }

def is_recoverable_network_error(exc: Exception) -> bool:
    message = str(exc)
    if isinstance(exc, RuntimeError) and message.startswith("Network error for "):
        return True
    if isinstance(exc, OSError):
        return True
    cause = exc.__cause__
    if isinstance(cause, Exception):
        return is_recoverable_network_error(cause)
    return False


async def sleep_for_network_recovery(*, reason: str, seconds: int = NETWORK_RECOVERY_SLEEP_SECONDS) -> None:
    log(
        f"{reason} Sleeping for {format_duration(seconds)} before retrying. Session state will be preserved.",
        label="RECOVER",
        tone="yellow",
    )
    await asyncio.sleep(seconds)


def http_json(path: str, *, params: dict[str, Any] | None = None, timeout: int = 20) -> Any:
    url = path
    if params:
        url = f"{url}?{urlencode(params)}"

    try:
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
        )
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        if exc.code == 403:
            return curl_json(url, timeout=timeout)
        raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc


def curl_json(url: str, *, timeout: int) -> Any:
    result = subprocess.run(
        [
            "curl",
            "-sS",
            "-A",
            USER_AGENT,
            "-H",
            "Accept: application/json",
            "--max-time",
            str(timeout),
            url,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def decimal_from_integer_string(value: str | None, *, scale: int = 18) -> str:
    if not value:
        return ""
    try:
        decimal_value = Decimal(value) / (Decimal(10) ** scale)
    except (InvalidOperation, ValueError):
        return ""
    return f"{decimal_value:.8f}".rstrip("0").rstrip(".")


def parse_token_ids(raw_token_ids: str | list[str] | None) -> tuple[str, str] | None:
    if raw_token_ids is None:
        return None
    if isinstance(raw_token_ids, str):
        token_ids = json.loads(raw_token_ids)
    else:
        token_ids = raw_token_ids
    if not isinstance(token_ids, list) or len(token_ids) != 2:
        return None
    first, second = str(token_ids[0]), str(token_ids[1])
    return first, second


def parse_outcome_labels(raw_outcomes: str | list[str] | None) -> tuple[str, str] | None:
    if raw_outcomes is None:
        return None
    if isinstance(raw_outcomes, str):
        outcomes = json.loads(raw_outcomes)
    else:
        outcomes = raw_outcomes
    if not isinstance(outcomes, list) or len(outcomes) != 2:
        return None
    first, second = str(outcomes[0]), str(outcomes[1])
    return first, second


def parse_float_values(raw_values: str | list[Any] | None) -> list[float]:
    if raw_values is None:
        return []
    if isinstance(raw_values, str):
        try:
            values = json.loads(raw_values)
        except json.JSONDecodeError:
            return []
    else:
        values = raw_values
    if not isinstance(values, list):
        return []

    parsed: list[float] = []
    for value in values:
        numeric = float_or_none(value)
        if numeric is not None:
            parsed.append(numeric)
    return parsed


def first_json_object(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                return item
    return None


def recurring_previous_slug(slug: str) -> str | None:
    prefix, separator, suffix = slug.rpartition("-")
    if not prefix or separator != "-":
        return None
    try:
        start_stamp = int(suffix)
    except ValueError:
        return None
    previous_start_stamp = start_stamp - RECURRING_MARKET_STEP_SECONDS
    if previous_start_stamp <= 0:
        return None
    return f"{prefix}-{previous_start_stamp}"


def fetch_event_by_slug(slug: str) -> dict[str, Any] | None:
    payload = http_json(
        f"{GAMMA_API_BASE}/events",
        params={"slug": slug},
        timeout=20,
    )
    return first_json_object(payload)


def extract_official_resolution(event_payload: dict[str, Any]) -> OfficialResolution | None:
    metadata = event_payload.get("eventMetadata")
    if isinstance(metadata, dict):
        price_to_beat = float_or_none(metadata.get("priceToBeat"))
        final_price = float_or_none(metadata.get("finalPrice"))
        if price_to_beat is not None and final_price is not None:
            return OfficialResolution(
                up_wins=final_price >= price_to_beat,
                price_to_beat=price_to_beat,
                final_price=final_price,
            )

    markets = event_payload.get("markets")
    if not isinstance(markets, list) or not markets:
        return None
    market = markets[0]
    if not isinstance(market, dict):
        return None

    outcome_labels = parse_outcome_labels(market.get("outcomes"))
    outcome_prices = parse_float_values(market.get("outcomePrices"))
    if outcome_labels is None or len(outcome_prices) != 2:
        return None

    for label, price in zip(outcome_labels, outcome_prices, strict=True):
        if price >= OFFICIAL_RESOLUTION_PRICE_THRESHOLD or math.isclose(price, 1.0, abs_tol=1e-9):
            return OfficialResolution(up_wins=label.lower() == "up")
    return None


def describe_resolution_poll(event_payload: dict[str, Any]) -> str:
    metadata = event_payload.get("eventMetadata")
    metadata_keys = sorted(metadata.keys()) if isinstance(metadata, dict) else []

    markets = event_payload.get("markets")
    market = markets[0] if isinstance(markets, list) and markets and isinstance(markets[0], dict) else {}

    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        outcome_prices_text = outcome_prices
    elif isinstance(outcome_prices, list):
        outcome_prices_text = json.dumps(outcome_prices, separators=(",", ":"))
    else:
        outcome_prices_text = "-"

    return (
        f"closed={event_payload.get('closed')} "
        f"active={event_payload.get('active')} "
        f"uma={market.get('umaResolutionStatus') or '-'} "
        f"uma_hist={market.get('umaResolutionStatuses') or '-'} "
        f"outcomePrices={outcome_prices_text} "
        f"metadataKeys={','.join(metadata_keys) if metadata_keys else '-'}"
    )


def fetch_previous_market_reference(market: MarketWindow) -> tuple[float, str] | None:
    previous_slug = recurring_previous_slug(market.slug)
    if previous_slug is None:
        return None

    previous_event = fetch_event_by_slug(previous_slug)
    if previous_event is None:
        return None

    resolution = extract_official_resolution(previous_event)
    if resolution is None or resolution.final_price is None:
        return None
    return resolution.final_price, previous_slug


def fetch_chainlink_quotes(
    config: UnderlyingConfig,
    *,
    after: datetime | None = None,
) -> list[UnderlyingQuote]:
    payload = http_json(
        f"{CHAINLINK_API_BASE}/query-timescale",
        params={
            "query": CHAINLINK_LIVE_QUERY,
            "variables": json.dumps({"feedId": config.feed_id}, separators=(",", ":")),
        },
        timeout=20,
    )
    nodes = payload.get("data", {}).get("liveStreamReports", {}).get("nodes", [])
    quotes: list[UnderlyingQuote] = []
    for node in reversed(nodes):
        if not isinstance(node, dict):
            continue
        timestamp = parse_iso_datetime(str(node.get("validFromTimestamp") or ""))
        if timestamp is None:
            continue
        if after is not None and timestamp <= after:
            continue
        quotes.append(
            UnderlyingQuote(
                source=config.source,
                symbol=config.symbol,
                feed_id=config.feed_id,
                timestamp=timestamp,
                price=decimal_from_integer_string(str(node.get("price") or "")),
                bid=decimal_from_integer_string(str(node.get("bid") or "")),
                ask=decimal_from_integer_string(str(node.get("ask") or "")),
                raw_json=json.dumps(node, separators=(",", ":"), sort_keys=True),
            )
        )
    return quotes


async def stream_coinbase_underlying_quotes(
    config: UnderlyingConfig,
    recorder: CsvRecorder,
    strategy: LivePaperStrategy,
    *,
    stop_event: asyncio.Event,
) -> None:
    subscribe_messages = [
        {"type": "subscribe", "product_ids": [config.feed_id], "channel": "ticker"},
        {"type": "subscribe", "channel": "heartbeats"},
    ]
    reconnect_backoff = 1.0

    while not stop_event.is_set():
        try:
            async with websockets.connect(
                COINBASE_MARKET_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                open_timeout=20,
                max_size=None,
            ) as websocket:
                for message in subscribe_messages:
                    await websocket.send(json.dumps(message))
                reconnect_backoff = 1.0

                while not stop_event.is_set():
                    try:
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    except TimeoutError:
                        continue

                    payload = json.loads(raw_message)
                    if not isinstance(payload, dict):
                        continue
                    if str(payload.get("channel") or "") != "ticker":
                        continue

                    timestamp = parse_iso_datetime(str(payload.get("timestamp") or "")) or utc_now()
                    for event in payload.get("events", []):
                        if not isinstance(event, dict):
                            continue
                        tickers = event.get("tickers")
                        if not isinstance(tickers, list):
                            continue
                        for ticker in tickers:
                            if not isinstance(ticker, dict):
                                continue
                            if str(ticker.get("product_id") or "") != config.feed_id:
                                continue
                            quote = UnderlyingQuote(
                                source=config.source,
                                symbol=config.symbol,
                                feed_id=config.feed_id,
                                timestamp=timestamp,
                                price=str(ticker.get("price") or ""),
                                bid=str(ticker.get("best_bid") or ""),
                                ask=str(ticker.get("best_ask") or ""),
                                raw_json=json.dumps(ticker, separators=(",", ":"), sort_keys=True),
                            )
                            recorder.write_underlying_quote(quote)
                            strategy.on_underlying_quote(quote)
        except ConnectionClosed as exc:
            log(
                f"Coinbase feed closed for {config.feed_id}: {exc}. Reconnecting in {reconnect_backoff:.1f}s.",
                label="COINBASE",
                tone="yellow",
            )
        except Exception as exc:  # noqa: BLE001
            log(
                f"Coinbase feed error for {config.feed_id}: {exc}. Reconnecting in {reconnect_backoff:.1f}s.",
                label="COINBASE",
                tone="yellow",
            )

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=reconnect_backoff)
        except TimeoutError:
            reconnect_backoff = min(reconnect_backoff * 2, 15.0)
            continue
        return


async def stream_chainlink_underlying_quotes(
    config: UnderlyingConfig,
    recorder: CsvRecorder,
    strategy: LivePaperStrategy,
    *,
    stop_event: asyncio.Event,
    poll_seconds: float = 1.0,
) -> None:
    last_timestamp: datetime | None = None
    while not stop_event.is_set():
        try:
            quotes = fetch_chainlink_quotes(config, after=last_timestamp)
            if quotes:
                for quote in quotes:
                    recorder.write_underlying_quote(quote)
                    strategy.on_underlying_quote(quote)
                last_timestamp = quotes[-1].timestamp
        except Exception as exc:  # noqa: BLE001
            log(
                f"Underlying quote poll failed for {config.symbol}: {exc}",
                label="CHAINLINK",
                tone="yellow",
            )

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_seconds)
        except TimeoutError:
            continue


async def stream_underlying_quotes(
    config: UnderlyingConfig,
    recorder: CsvRecorder,
    strategy: LivePaperStrategy,
    *,
    stop_event: asyncio.Event,
    poll_seconds: float = 1.0,
) -> None:
    if config.source == "coinbase_advanced_trade_ws":
        await stream_coinbase_underlying_quotes(
            config,
            recorder,
            strategy,
            stop_event=stop_event,
        )
        return

    await stream_chainlink_underlying_quotes(
        config,
        recorder,
        strategy,
        stop_event=stop_event,
        poll_seconds=poll_seconds,
    )


def safe_slug(value: str) -> str:
    cleaned = []
    for char in value.lower():
        if char.isalnum():
            cleaned.append(char)
        elif char in {"-", "_"}:
            cleaned.append(char)
        else:
            cleaned.append("-")
    slug = "".join(cleaned).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "market"


def window_filename(market: MarketWindow) -> str:
    end_stamp = market.window_end.strftime("%Y%m%dT%H%M%SZ")
    return f"{end_stamp}__{safe_slug(market.slug)}__{market.market_id}.csv"


def session_filename(*, asset: str, execution_mode: str, started_at: datetime) -> str:
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}__{safe_slug(asset)}__{execution_mode}__session.csv"


def resolve_underlying_config(asset: str, source: str) -> UnderlyingConfig:
    if source == "coinbase":
        return UNDERLYING_CONFIGS[asset]
    if source == "chainlink-delayed":
        return CHAINLINK_UNDERLYING_CONFIGS[asset]
    raise ValueError(f"unsupported underlying source {source}")


def discover_next_market(
    asset: str,
    processed_market_ids: set[str],
    *,
    require_future_start: bool,
) -> MarketWindow | None:
    events = http_json(
        f"{GAMMA_API_BASE}/events",
        params={
            "tag_slug": asset,
            "closed": "false",
            "order": "start_date",
            "ascending": "false",
            "limit": DISCOVERY_LIMIT,
        },
    )

    now = utc_now()
    candidates: list[MarketWindow] = []
    for event in events:
        event_tags = {str(tag.get("slug")) for tag in event.get("tags", []) if tag.get("slug")}
        if not DISCOVERY_TAGS.issubset(event_tags):
            continue

        for market in event.get("markets", []):
            market_id = str(market.get("id", ""))
            if not market_id or market_id in processed_market_ids:
                continue
            if not market.get("enableOrderBook", False):
                continue

            window_end = parse_iso_datetime(market.get("endDate"))
            if window_end is None or window_end <= now:
                continue
            window_start = (
                parse_iso_datetime(market.get("eventStartTime"))
                or parse_iso_datetime(event.get("startTime"))
                or (window_end - WINDOW_DURATION)
            )
            if require_future_start and window_start <= now:
                continue

            token_ids = parse_token_ids(market.get("clobTokenIds"))
            if token_ids is None:
                continue
            outcome_labels = parse_outcome_labels(market.get("outcomes"))
            if outcome_labels is None:
                continue

            title = str(event.get("title") or market.get("question") or "")
            slug = str(market.get("slug") or event.get("slug") or market_id)
            candidates.append(
                MarketWindow(
                    asset=asset,
                    event_id=str(event.get("id", "")),
                    market_id=market_id,
                    condition_id=str(market.get("conditionId") or ""),
                    title=title,
                    slug=slug,
                    token_ids=token_ids,
                    outcome_labels=outcome_labels,
                    window_start=window_start,
                    window_end=window_end,
                )
            )

    candidates.sort(key=lambda current: (current.window_start, current.window_end, current.market_id))
    return candidates[0] if candidates else None


def compute_best_price(levels: list[dict[str, Any]], *, reverse: bool = False) -> str:
    prices: list[float] = []
    for level in levels:
        try:
            prices.append(float(level["price"]))
        except (KeyError, TypeError, ValueError):
            continue
    if not prices:
        return ""
    return f"{(max(prices) if reverse else min(prices)):.6f}"


def compute_best_level(levels: list[dict[str, Any]], *, reverse: bool = False) -> tuple[float | None, float | None]:
    best_price: float | None = None
    best_size: float | None = None
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = float_or_none(level.get("price"))
        size = float_or_none(level.get("size"))
        if price is None or size is None:
            continue
        if best_price is None or (price > best_price if reverse else price < best_price):
            best_price = price
            best_size = size
    return best_price, best_size


def normalize_book_levels(levels: list[dict[str, Any]], *, reverse: bool = False) -> list[tuple[float, float]]:
    normalized: list[tuple[float, float]] = []
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = float_or_none(level.get("price"))
        size = float_or_none(level.get("size"))
        if price is None or size is None or price <= 0 or size <= 0:
            continue
        normalized.append((price, size))
    normalized.sort(key=lambda item: item[0], reverse=reverse)
    return normalized


def fill_buy_for_notional(
    asks: list[tuple[float, float]],
    *,
    target_notional: float,
) -> tuple[float, float, float] | None:
    remaining_notional = target_notional
    bought_shares = 0.0
    spent = 0.0
    fee = 0.0

    for price, size in asks:
        if remaining_notional <= 1e-9:
            break
        level_notional = price * size
        buy_notional = min(level_notional, remaining_notional)
        buy_shares = buy_notional / price
        bought_shares += buy_shares
        spent += buy_notional
        fee += approx_crypto_5m_taker_fee_per_share(price) * buy_shares
        remaining_notional -= buy_notional

    if remaining_notional > 1e-9 or bought_shares <= 1e-9:
        return None
    return bought_shares, spent, fee


def fill_sell_for_notional(
    bids: list[tuple[float, float]],
    *,
    target_notional: float,
    max_shares: float,
    allow_partial_position: bool = False,
) -> tuple[float, float, float] | None:
    remaining_notional = target_notional
    remaining_shares = max_shares
    sold_shares = 0.0
    gross_proceeds = 0.0
    fee = 0.0

    for price, size in bids:
        if remaining_shares <= 1e-9:
            break
        available_shares = min(size, remaining_shares)
        if available_shares <= 1e-9:
            continue
        sell_shares = available_shares
        if not allow_partial_position:
            sell_shares = min(sell_shares, remaining_notional / price)
        if sell_shares <= 1e-9:
            continue
        gross = sell_shares * price
        sold_shares += sell_shares
        gross_proceeds += gross
        fee += approx_crypto_5m_taker_fee_per_share(price) * sell_shares
        remaining_shares -= sell_shares
        remaining_notional -= gross
        if not allow_partial_position and remaining_notional <= 1e-9:
            break

    if sold_shares <= 1e-9:
        return None
    if not allow_partial_position and remaining_notional > 1e-9:
        return None
    return sold_shares, gross_proceeds - fee, fee


def float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def approx_crypto_5m_taker_fee_per_share(price: float) -> float:
    bounded = max(0.0, min(price, 1.0))
    return 0.0156 * 4.0 * bounded * (1.0 - bounded)


def format_money(value: float) -> str:
    return f"${value:,.2f}"


def format_signed(value: float) -> str:
    return f"{value:+.4f}"


def format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def live_state_is_flat(
    state: dict[str, Any],
    *,
    reserved_cash_epsilon: float = LIVE_RESERVED_CASH_EPSILON_USDC,
) -> bool:
    return (
        int(state.get("open_positions_count") or 0) == 0
        and int(state.get("open_orders_count") or 0) == 0
        and abs(float(state.get("open_order_reserved_cash_usdc") or 0.0)) <= reserved_cash_epsilon
    )


def describe_live_state_guard(state: dict[str, Any]) -> str:
    details: list[str] = []
    positions = state.get("positions", [])
    if positions:
        labels = ", ".join(f"{row['label']} {row['shares']}" for row in positions)
        details.append(f"positions={labels}")
    open_orders_count = int(state.get("open_orders_count") or 0)
    if open_orders_count > 0:
        details.append(f"open_orders={open_orders_count}")
    reserved_cash = float(state.get("open_order_reserved_cash_usdc") or 0.0)
    if abs(reserved_cash) > LIVE_RESERVED_CASH_EPSILON_USDC:
        details.append(f"reserved_cash={reserved_cash:.6f}")
    return ", ".join(details) or "flat"


def sparkline(values: list[float], *, width: int) -> str:
    if width <= 0:
        return ""
    if not values:
        return "." * width
    source = values[-width:]
    low = min(source)
    high = max(source)
    if math.isclose(low, high):
        return "-" * len(source)
    glyphs = " .:-=+*#%@"
    chars = []
    span = high - low
    for value in source:
        index = int((value - low) / span * (len(glyphs) - 1))
        chars.append(glyphs[max(0, min(index, len(glyphs) - 1))])
    return "".join(chars)


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

    if (
        x_min is not None
        and x_max is not None
        and x_max > x_min
        and hasattr(plt, "xlim")
    ):
        plt.xlim(x_min, x_max)

    if y_min is not None and y_max is not None and not math.isclose(y_min, y_max):
        plt.ylim(y_min, y_max)

    for label, values, color_name in series:
        if not values:
            continue
        points = x_values[-len(values):]
        plt.plot(points, values, color=color_name, label=label)

    return plt.build()


@dataclass
class BookState:
    asset_id: str
    label: str
    best_bid: float | None = None
    best_bid_size: float | None = None
    best_ask: float | None = None
    best_ask_size: float | None = None
    bid_levels: list[tuple[float, float]] = field(default_factory=list)
    ask_levels: list[tuple[float, float]] = field(default_factory=list)
    last_trade_price: float | None = None
    updated_at: datetime | None = None

    @property
    def mid(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0


@dataclass
class PaperPosition:
    asset_id: str
    label: str
    shares: float = 0.0
    cost_basis: float = 0.0
    opened_at: datetime | None = None

    @property
    def average_cost(self) -> float:
        if self.shares <= 0:
            return 0.0
        return self.cost_basis / self.shares


@dataclass
class LiveLatencyStats:
    buy_match_count: int = 0
    buy_match_sum_ms: float = 0.0
    sell_match_count: int = 0
    sell_match_sum_ms: float = 0.0

    @property
    def average_buy_match_ms(self) -> float | None:
        if self.buy_match_count <= 0:
            return None
        return self.buy_match_sum_ms / self.buy_match_count

    @property
    def average_sell_match_ms(self) -> float | None:
        if self.sell_match_count <= 0:
            return None
        return self.sell_match_sum_ms / self.sell_match_count

    def record(self, *, side: str, matched_ms: float) -> None:
        if side == "BUY":
            self.buy_match_count += 1
            self.buy_match_sum_ms += matched_ms
            return
        self.sell_match_count += 1
        self.sell_match_sum_ms += matched_ms


class LivePaperStrategy:
    def __init__(
        self,
        market: MarketWindow,
        recorder: CsvRecorder,
        *,
        execution_mode: str,
        starting_balance: float,
        session_starting_balance: float,
        paper_trade_shares: float,
        live_buy_notional_usd: float,
        min_alpha_net: float,
        max_position_shares: float,
        max_market_exposure_usd: float,
        historical_average_max_alpha: float | None,
        session_performance_stats: SessionPerformanceStats,
        trade_cooldown_seconds: float,
        env_file: str | None = None,
        initial_trade_wait_seconds: float = 30.0,
        history_points: int | None = None,
    ) -> None:
        self.market = market
        self.recorder = recorder
        self.execution_mode = execution_mode
        self.starting_balance = starting_balance
        self.session_starting_balance = session_starting_balance
        self.cash = starting_balance
        self.paper_trade_shares = paper_trade_shares
        self.live_buy_notional_usd = live_buy_notional_usd
        self.min_alpha_net = min_alpha_net
        self.max_position_shares = max_position_shares
        self.max_market_exposure_usd = max_market_exposure_usd
        self.historical_average_max_alpha = historical_average_max_alpha
        self.session_performance_stats = session_performance_stats
        self.trade_cooldown_seconds = trade_cooldown_seconds
        self.initial_trade_wait_seconds = initial_trade_wait_seconds
        history_limit = history_points
        if history_limit is None:
            history_limit = max(int((self.market.window_end - self.market.window_start).total_seconds()) + 5, 90)
        self.trade_count = 0
        self.realized_pnl = 0.0
        self.market_max_buy_alpha_net = 0.0
        self.initial_trade_taken = False
        # Session analytics only; exposure checks use live open cost basis.
        self.market_buy_budget_used_usd = 0.0
        self._recent_events: deque[str] = deque(maxlen=8)
        self._last_trade_at: dict[tuple[str, str], datetime] = {}
        self._pending_order_tasks: dict[str, asyncio.Task[None]] = {}
        self._initial_trade_pending = False
        self._late_trade_halt_logged = False
        self._underlying_by_second: deque[tuple[datetime, float]] = deque(maxlen=600)
        self._history_seconds: deque[datetime] = deque(maxlen=history_limit)
        self._history_fair_up: deque[float] = deque(maxlen=history_limit)
        self._history_up_mid: deque[float] = deque(maxlen=history_limit)
        self._history_up_alpha: deque[float] = deque(maxlen=history_limit)
        self._balance_history_seconds: deque[datetime] = deque(maxlen=history_limit)
        self._history_balance: deque[float] = deque(maxlen=history_limit)
        self._closed_trade_units = 0.0
        self._winning_trade_units = 0.0
        self._buy_trade_count = 0
        self._buy_alpha_sum = 0.0
        self.latest_underlying_price: float | None = None
        self.latest_underlying_timestamp: datetime | None = None
        self.start_underlying_price: float | None = None
        self._live_state: dict[str, Any] = build_live_account_state(
            cash_balance_micro_usdc="0",
            open_orders=[],
            positions=[],
        )
        self._live_state_stop: asyncio.Event | None = None
        self._live_state_task: asyncio.Task[None] | None = None
        self._live_confirmation_tasks: set[asyncio.Task[None]] = set()
        self._live_env = load_live_env(Path(env_file)) if self.execution_mode == "live" and env_file is not None else None
        self._live_client: Any | None = None
        self._live_user_channel: UserChannel | None = None
        self._live_proxy_address = ""
        self._live_last_order_summary: dict[str, Any] | None = None
        self._live_latency_stats = LiveLatencyStats()
        self._books = {
            asset_id: BookState(asset_id=asset_id, label=label)
            for asset_id, label in zip(self.market.token_ids, self.market.outcome_labels, strict=True)
        }
        self._positions = {
            asset_id: PaperPosition(asset_id=asset_id, label=label)
            for asset_id, label in zip(self.market.token_ids, self.market.outcome_labels, strict=True)
        }
        if self.execution_mode == "live":
            ensure_live_dependencies()
            if self._live_env is None:
                raise SystemExit("Live mode requires --env-file with Polymarket credentials.")
            self._live_client = build_auth_client(self._live_env)
            self._live_proxy_address = self._live_env["POLYMARKET_PROXY_ADDRESS"]

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

    def uses_live_execution(self) -> bool:
        return self.execution_mode == "live"

    def trade_notional_usd(self) -> float:
        if self.uses_live_execution():
            return self.live_buy_notional_usd
        return PAPER_TRADE_NOTIONAL_USD

    def _current_queue_delay_seconds(self) -> float:
        if self.uses_live_execution():
            return 0.0
        return MARKET_ORDER_EXECUTION_DELAY_SECONDS

    def _live_position_row(self, asset_id: str) -> dict[str, Any] | None:
        return self._live_state.get("positions_by_asset", {}).get(asset_id)

    def _live_marked_price(self, asset_id: str) -> float:
        row = self._live_position_row(asset_id)
        if row is not None:
            marked_price = float_or_none(row.get("marked_price"))
            if marked_price is not None and marked_price > 0:
                return marked_price
        book = self._books[asset_id]
        if book.mid is not None:
            return book.mid
        return 0.0

    def _live_executable_price(self, asset_id: str) -> float:
        row = self._live_position_row(asset_id)
        if row is not None:
            executable_sell_price = float_or_none(row.get("executable_sell_price"))
            if executable_sell_price is not None and executable_sell_price > 0:
                return executable_sell_price
        book = self._books[asset_id]
        if book.best_bid is not None:
            return book.best_bid
        return 0.0

    def position_shares(self, asset_id: str) -> float:
        return self._positions[asset_id].shares

    def position_average_cost(self, asset_id: str) -> float:
        return self._positions[asset_id].average_cost

    def position_cost_basis(self, asset_id: str) -> float:
        return self._positions[asset_id].cost_basis

    def marked_open_value(self) -> float:
        if not self.uses_live_execution():
            return self.current_balance() - self.cash
        return sum(
            position.shares * self._live_marked_price(asset_id)
            for asset_id, position in self._positions.items()
            if position.shares > 0
        )

    def executable_open_value(self) -> float:
        if not self.uses_live_execution():
            return self.current_liquidation_value()
        return sum(
            position.shares * self._live_executable_price(asset_id)
            for asset_id, position in self._positions.items()
            if position.shares > 0
        )

    def marked_total_account_value(self) -> float:
        if not self.uses_live_execution():
            return self.current_balance()
        return float(self._live_state.get("marked_total_account_value_usdc") or 0.0)

    def executable_total_account_value(self) -> float:
        if not self.uses_live_execution():
            return self.current_balance()
        return float(self._live_state.get("executable_total_account_value_usdc") or 0.0)

    def open_order_reserved_cash(self) -> float:
        if not self.uses_live_execution():
            return 0.0
        return float(self._live_state.get("open_order_reserved_cash_usdc") or 0.0)

    def average_live_buy_match_ms(self) -> float | None:
        return self._live_latency_stats.average_buy_match_ms

    def average_live_sell_match_ms(self) -> float | None:
        return self._live_latency_stats.average_sell_match_ms

    def last_live_order_status(self) -> str:
        if self._live_last_order_summary is None:
            return "-"
        status = self._live_last_order_summary.get("status") or "-"
        side = self._live_last_order_summary.get("side") or ""
        return f"{side} {status}".strip()

    def live_account_is_flat(self) -> bool:
        if not self.uses_live_execution():
            return all(position.shares <= 1e-9 for position in self._positions.values())
        return live_state_is_flat(self._live_state)

    async def start_runtime(self) -> None:
        if not self.uses_live_execution():
            return
        assert self._live_env is not None
        self._live_user_channel = await UserChannel(
            self._live_env["POLYMARKET_API_KEY"],
            self._live_env["POLYMARKET_API_SECRET"],
            self._live_env["POLYMARKET_API_PASSPHRASE"],
            self.market.condition_id,
        ).__aenter__()
        await self.refresh_live_state(force=True)
        self._live_state_stop = asyncio.Event()
        self._live_state_task = asyncio.create_task(self._live_state_loop())

    async def stop_runtime(self) -> None:
        if not self.uses_live_execution():
            return
        if self._live_state_stop is not None:
            self._live_state_stop.set()
        if self._live_state_task is not None:
            await self._live_state_task
            self._live_state_task = None
        if self._live_confirmation_tasks:
            for task in list(self._live_confirmation_tasks):
                task.cancel()
            await asyncio.gather(*self._live_confirmation_tasks, return_exceptions=True)
            self._live_confirmation_tasks.clear()
        if self._live_user_channel is not None:
            await self._live_user_channel.__aexit__(None, None, None)
            self._live_user_channel = None

    async def _live_state_loop(self) -> None:
        assert self._live_state_stop is not None
        while not self._live_state_stop.is_set():
            try:
                await self.refresh_live_state(force=True)
            except Exception as exc:  # noqa: BLE001
                self._recent_events.append(f"Live state refresh failed: {exc}")
            try:
                await asyncio.wait_for(self._live_state_stop.wait(), timeout=LIVE_ACCOUNT_REFRESH_SECONDS)
            except TimeoutError:
                continue

    def _build_live_executable_price_overrides(self) -> dict[str, float | None]:
        overrides: dict[str, float | None] = {}
        for asset_id, book in self._books.items():
            overrides[asset_id] = book.best_bid
        return overrides

    async def refresh_live_state(self, *, force: bool = False) -> None:
        if not self.uses_live_execution():
            return
        assert self._live_client is not None
        state = await asyncio.to_thread(
            fetch_live_account_state,
            self._live_client,
            self._live_proxy_address,
            executable_sell_prices=self._build_live_executable_price_overrides(),
        )
        self.apply_live_state(state)

    def apply_live_state(self, state: dict[str, Any]) -> None:
        self._live_state = state
        self.cash = float(state["cash_balance_usdc"])

    def live_guard_note(self) -> str:
        if not self.uses_live_execution():
            return ""
        return describe_live_state_guard(self._live_state)

    def _holding_time_seconds(self, asset_id: str, execution_timestamp: datetime) -> float | None:
        opened_at = self._positions[asset_id].opened_at
        if opened_at is None:
            return None
        return max((execution_timestamp - opened_at).total_seconds(), 0.0)

    def _trade_trigger_alpha(self, side: str, decision_snapshot: dict[str, float | None]) -> float | None:
        if side == "buy":
            return decision_snapshot.get("buy_alpha_net")
        return decision_snapshot.get("sell_alpha_net")

    def _trade_log_context(
        self,
        *,
        side: str,
        decision_timestamp: datetime,
        execution_timestamp: datetime,
        decision_snapshot: dict[str, float | None],
        desired_price: float | None,
        actual_price: float | None,
        execution_delay_ms: float | None,
        entry_type: str | None = None,
        holding_time_seconds: float | None = None,
    ) -> dict[str, Any]:
        trigger_alpha = self._trade_trigger_alpha(side, decision_snapshot)
        time_left_seconds = max((self.market.window_end - execution_timestamp).total_seconds(), 0.0)
        sigma = self.sigma_per_sqrt_second()
        fair_up = self.fair_up_probability(execution_timestamp)
        return {
            "desired_price": desired_price,
            "actual_price": actual_price,
            "trigger_alpha": trigger_alpha,
            "fair_value_at_decision": decision_snapshot.get("fair_value"),
            "best_bid_at_decision": decision_snapshot.get("decision_best_bid"),
            "best_ask_at_decision": decision_snapshot.get("decision_best_ask"),
            "underlying_price_to_beat": self.start_underlying_price,
            "underlying_price_actual": self.latest_underlying_price,
            "time_left_seconds": time_left_seconds,
            "seconds_to_expiry": time_left_seconds,
            "sigma_per_sqrt_s": sigma,
            "fair_up": fair_up,
            "execution_time_utc": execution_timestamp.isoformat(),
            "matched_time_utc": execution_timestamp.isoformat(),
            "trade_number_in_market": self.trade_count,
            "execution_delay_ms": execution_delay_ms,
            "cash_balance": self.cash,
            "account_value": self.current_balance(),
            "decision_time_utc": decision_timestamp.isoformat(),
            "side": side,
            "entry_type": entry_type,
            "holding_time_seconds": holding_time_seconds,
        }

    def _compose_trade_raw_json(
        self,
        *,
        trade_log_context: dict[str, Any],
        execution_trace: str | dict[str, Any],
    ) -> str:
        payload: dict[str, Any] = {"trade_log": trade_log_context}
        if isinstance(execution_trace, str):
            try:
                payload["execution_trace"] = json.loads(execution_trace)
            except json.JSONDecodeError:
                payload["execution_trace"] = execution_trace
        else:
            payload["execution_trace"] = execution_trace
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)

    def _pending_order_exists(self, asset_id: str) -> bool:
        task = self._pending_order_tasks.get(asset_id)
        return task is not None and not task.done()

    def _current_side_price(self, asset_id: str, side: str) -> float | None:
        book = self._books[asset_id]
        if side == "buy":
            return book.best_ask
        return book.best_bid

    def _build_market_order_trace_payload(
        self,
        *,
        asset_id: str,
        side: str,
        decision_timestamp: datetime,
        decision_snapshot: dict[str, float | None],
        samples: list[dict[str, float | str | None]],
        status: str,
        executed_shares: float | None = None,
        executed_price: float | None = None,
        fee: float | None = None,
        proceeds_or_notional: float | None = None,
    ) -> str:
        payload = {
            "mode": "delayed_market_order",
            "asset_id": asset_id,
            "side": side,
            "decision_timestamp_utc": decision_timestamp.isoformat(),
            "decision_snapshot": decision_snapshot,
            "samples": samples,
            "status": status,
            "executed_shares": executed_shares,
            "executed_price": executed_price,
            "fee": fee,
            "notional": proceeds_or_notional,
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)

    def _schedule_market_order(
        self,
        *,
        asset_id: str,
        side: str,
        timestamp: datetime,
        snapshot: dict[str, float | None],
        note: str,
        is_initial_trade: bool = False,
    ) -> None:
        if self._pending_order_exists(asset_id):
            return
        if timestamp + timedelta(seconds=self._current_queue_delay_seconds()) >= self.market.window_end:
            return
        if is_initial_trade and self._initial_trade_pending:
            return

        if is_initial_trade:
            self._initial_trade_pending = True

        if self.uses_live_execution():
            task = asyncio.create_task(
                self._execute_live_market_order(
                    asset_id=asset_id,
                    side=side,
                    decision_timestamp=timestamp,
                    decision_snapshot=snapshot,
                    note=note,
                    is_initial_trade=is_initial_trade,
                )
            )
        else:
            task = asyncio.create_task(
                self._execute_delayed_market_order(
                    asset_id=asset_id,
                    side=side,
                    decision_timestamp=timestamp,
                    decision_snapshot=snapshot,
                    note=note,
                    is_initial_trade=is_initial_trade,
                )
            )
        self._pending_order_tasks[asset_id] = task

        def _cleanup(completed: asyncio.Task[None], *, key: str = asset_id, initial: bool = is_initial_trade) -> None:
            current = self._pending_order_tasks.get(key)
            if current is completed:
                self._pending_order_tasks.pop(key, None)
            if initial and not self.initial_trade_taken:
                self._initial_trade_pending = False

        task.add_done_callback(_cleanup)

    async def _execute_delayed_market_order(
        self,
        *,
        asset_id: str,
        side: str,
        decision_timestamp: datetime,
        decision_snapshot: dict[str, float | None],
        note: str,
        is_initial_trade: bool,
    ) -> None:
        samples: list[dict[str, float | str | None]] = []
        previous_delay = 0.0

        try:
            for delay in MARKET_ORDER_TRACE_DELAYS_SECONDS:
                wait_seconds = max(delay - previous_delay, 0.0)
                previous_delay = delay
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)

                sample_time = utc_now()
                side_price = self._current_side_price(asset_id, side)
                samples.append(
                    {
                        "offset_ms": int(delay * 1000),
                        "timestamp_utc": sample_time.isoformat(),
                        "side_price": side_price,
                    }
                )

                if not math.isclose(delay, MARKET_ORDER_EXECUTION_DELAY_SECONDS, abs_tol=1e-9):
                    continue

                if side == "buy":
                    await self._finalize_delayed_buy(
                        asset_id=asset_id,
                        decision_timestamp=decision_timestamp,
                        decision_snapshot=decision_snapshot,
                        samples=samples,
                        note=note,
                        is_initial_trade=is_initial_trade,
                    )
                else:
                    await self._finalize_delayed_sell(
                        asset_id=asset_id,
                        decision_timestamp=decision_timestamp,
                        decision_snapshot=decision_snapshot,
                        samples=samples,
                    )
        except asyncio.CancelledError:
            self._recent_events.append(f"Canceled pending {side.upper()} for {self._books[asset_id].label}")
            raise

    async def _finalize_delayed_buy(
        self,
        *,
        asset_id: str,
        decision_timestamp: datetime,
        decision_snapshot: dict[str, float | None],
        samples: list[dict[str, float | str | None]],
        note: str,
        is_initial_trade: bool,
    ) -> None:
        position = self._positions[asset_id]
        book = self._books[asset_id]
        if (self.market.window_end - utc_now()).total_seconds() <= NO_NEW_TRADES_LAST_SECONDS:
            self._recent_events.append(
                f"BUY {book.label} canceled after 500ms: market is inside final {NO_NEW_TRADES_LAST_SECONDS:.0f}s"
            )
            return
        buy_fill = fill_buy_for_notional(book.ask_levels, target_notional=PAPER_TRADE_NOTIONAL_USD)
        if buy_fill is None:
            self._recent_events.append(f"BUY {book.label} aborted after 500ms: no full $1 ask fill available")
            return

        buy_shares, buy_notional, fee = buy_fill
        total_cost = buy_notional + fee
        market_exposure = self.current_market_exposure()
        if (
            position.shares + buy_shares > self.max_position_shares
            or self.cash < total_cost
            or market_exposure + total_cost > self.max_market_exposure_usd
        ):
            self._recent_events.append(f"BUY {book.label} aborted after 500ms: balance/exposure limit hit")
            return

        was_flat = position.shares <= 1e-9
        average_buy_price = buy_notional / buy_shares
        self.cash -= total_cost
        self.market_buy_budget_used_usd += total_cost
        position.shares += buy_shares
        position.cost_basis += total_cost
        if was_flat:
            position.opened_at = utc_now()
        self.trade_count += 1
        self._record_buy_alpha(decision_snapshot["buy_alpha_net"])
        if is_initial_trade:
            self.initial_trade_taken = True
        recorded_at = utc_now()
        if was_flat:
            position.opened_at = recorded_at
        self._last_trade_at[(asset_id, "buy")] = recorded_at
        desired_price = float_or_none(samples[0].get("side_price")) if samples else None
        execution_delay_ms = max((recorded_at - decision_timestamp).total_seconds() * 1000.0, 0.0)
        trade_log_context = self._trade_log_context(
            side="buy",
            decision_timestamp=decision_timestamp,
            execution_timestamp=recorded_at,
            decision_snapshot=decision_snapshot,
            desired_price=desired_price,
            actual_price=average_buy_price,
            execution_delay_ms=execution_delay_ms,
            entry_type="first_entry" if was_flat else "later_add",
        )
        self._recent_events.append(
            f"BUY {book.label} {buy_shares:.4f} for {format_money(buy_notional)} @ {average_buy_price:.4f} after 500ms"
        )
        self.recorder.write_paper_trade(
            asset_id=asset_id,
            token_label=book.label,
            action="buy",
            shares=buy_shares,
            price=average_buy_price,
            fee=fee,
            fair_value=decision_snapshot["fair_value"],
            market_mid=decision_snapshot["market_mid"],
            buy_alpha_net=decision_snapshot["buy_alpha_net"],
            sell_alpha_net=decision_snapshot["sell_alpha_net"],
            cash_balance=self.cash,
            realized_pnl=self.realized_pnl,
            position_shares=position.shares,
            position_average_cost=position.average_cost,
            note=note,
            recorded_at=recorded_at,
            decision_time_utc=str(trade_log_context["decision_time_utc"]),
            matched_time_utc=str(trade_log_context["matched_time_utc"]),
            seconds_to_expiry=trade_log_context["seconds_to_expiry"],
            fair_value_at_decision=trade_log_context["fair_value_at_decision"],
            best_bid_at_decision=trade_log_context["best_bid_at_decision"],
            best_ask_at_decision=trade_log_context["best_ask_at_decision"],
            alpha_at_decision=trade_log_context["trigger_alpha"],
            entry_type=str(trade_log_context["entry_type"] or ""),
            desired_price=desired_price,
            actual_price=average_buy_price,
            trigger_alpha=trade_log_context["trigger_alpha"],
            underlying_price_to_beat=trade_log_context["underlying_price_to_beat"],
            underlying_price_actual=trade_log_context["underlying_price_actual"],
            time_left_seconds=trade_log_context["time_left_seconds"],
            sigma_per_sqrt_s=trade_log_context["sigma_per_sqrt_s"],
            fair_up=trade_log_context["fair_up"],
            execution_time_utc=str(trade_log_context["execution_time_utc"]),
            trade_number_in_market=int(trade_log_context["trade_number_in_market"]),
            execution_delay_ms=trade_log_context["execution_delay_ms"],
            account_value=float(trade_log_context["account_value"]),
            raw_json=self._compose_trade_raw_json(
                trade_log_context=trade_log_context,
                execution_trace=self._build_market_order_trace_payload(
                    asset_id=asset_id,
                    side="buy",
                    decision_timestamp=decision_timestamp,
                    decision_snapshot=decision_snapshot,
                    samples=samples,
                    status="filled",
                    executed_shares=buy_shares,
                    executed_price=average_buy_price,
                    fee=fee,
                    proceeds_or_notional=buy_notional,
                ),
            ),
        )

    async def _finalize_delayed_sell(
        self,
        *,
        asset_id: str,
        decision_timestamp: datetime,
        decision_snapshot: dict[str, float | None],
        samples: list[dict[str, float | str | None]],
    ) -> None:
        position = self._positions[asset_id]
        book = self._books[asset_id]
        if (self.market.window_end - utc_now()).total_seconds() <= NO_NEW_TRADES_LAST_SECONDS:
            self._recent_events.append(
                f"SELL {book.label} canceled after 500ms: market is inside final {NO_NEW_TRADES_LAST_SECONDS:.0f}s"
            )
            return
        allow_partial_position = (
            book.best_bid is not None
            and book.best_bid > 0
            and position.shares * book.best_bid < PAPER_TRADE_NOTIONAL_USD
        )
        sell_fill = fill_sell_for_notional(
            book.bid_levels,
            target_notional=PAPER_TRADE_NOTIONAL_USD,
            max_shares=position.shares,
            allow_partial_position=allow_partial_position,
        )
        if sell_fill is None:
            self._recent_events.append(f"SELL {book.label} aborted after 500ms: bid depth insufficient")
            return

        sell_shares, proceeds, fee = sell_fill
        average_sell_price = (proceeds + fee) / sell_shares
        cost_removed = position.average_cost * sell_shares
        realized = proceeds - cost_removed
        holding_time_seconds = self._holding_time_seconds(asset_id, utc_now())
        self.cash += proceeds
        self.realized_pnl += realized
        position.shares -= sell_shares
        position.cost_basis -= cost_removed
        self._record_closed_trade(sell_shares, realized)
        if position.shares <= 1e-9:
            position.shares = 0.0
            position.cost_basis = 0.0
            position.opened_at = None
        self.trade_count += 1
        recorded_at = utc_now()
        self._last_trade_at[(asset_id, "sell")] = recorded_at
        desired_price = float_or_none(samples[0].get("side_price")) if samples else None
        execution_delay_ms = max((recorded_at - decision_timestamp).total_seconds() * 1000.0, 0.0)
        trade_log_context = self._trade_log_context(
            side="sell",
            decision_timestamp=decision_timestamp,
            execution_timestamp=recorded_at,
            decision_snapshot=decision_snapshot,
            desired_price=desired_price,
            actual_price=average_sell_price,
            execution_delay_ms=execution_delay_ms,
            holding_time_seconds=holding_time_seconds,
        )
        self._recent_events.append(
            f"SELL {book.label} {sell_shares:.4f} for {format_money(proceeds + fee)} @ {average_sell_price:.4f} after 500ms"
        )
        self.recorder.write_paper_trade(
            asset_id=asset_id,
            token_label=book.label,
            action="sell",
            shares=sell_shares,
            price=average_sell_price,
            fee=fee,
            fair_value=decision_snapshot["fair_value"],
            market_mid=decision_snapshot["market_mid"],
            buy_alpha_net=decision_snapshot["buy_alpha_net"],
            sell_alpha_net=decision_snapshot["sell_alpha_net"],
            cash_balance=self.cash,
            realized_pnl=self.realized_pnl,
            position_shares=position.shares,
            position_average_cost=position.average_cost,
            note="positive bid alpha",
            recorded_at=recorded_at,
            decision_time_utc=str(trade_log_context["decision_time_utc"]),
            matched_time_utc=str(trade_log_context["matched_time_utc"]),
            seconds_to_expiry=trade_log_context["seconds_to_expiry"],
            fair_value_at_decision=trade_log_context["fair_value_at_decision"],
            best_bid_at_decision=trade_log_context["best_bid_at_decision"],
            best_ask_at_decision=trade_log_context["best_ask_at_decision"],
            alpha_at_decision=trade_log_context["trigger_alpha"],
            realized_pnl_on_close=realized,
            holding_time_seconds=trade_log_context["holding_time_seconds"],
            desired_price=desired_price,
            actual_price=average_sell_price,
            trigger_alpha=trade_log_context["trigger_alpha"],
            underlying_price_to_beat=trade_log_context["underlying_price_to_beat"],
            underlying_price_actual=trade_log_context["underlying_price_actual"],
            time_left_seconds=trade_log_context["time_left_seconds"],
            sigma_per_sqrt_s=trade_log_context["sigma_per_sqrt_s"],
            fair_up=trade_log_context["fair_up"],
            execution_time_utc=str(trade_log_context["execution_time_utc"]),
            trade_number_in_market=int(trade_log_context["trade_number_in_market"]),
            execution_delay_ms=trade_log_context["execution_delay_ms"],
            account_value=float(trade_log_context["account_value"]),
            raw_json=self._compose_trade_raw_json(
                trade_log_context=trade_log_context,
                execution_trace=self._build_market_order_trace_payload(
                    asset_id=asset_id,
                    side="sell",
                    decision_timestamp=decision_timestamp,
                    decision_snapshot=decision_snapshot,
                    samples=samples,
                    status="filled",
                    executed_shares=sell_shares,
                    executed_price=average_sell_price,
                    fee=fee,
                    proceeds_or_notional=proceeds + fee,
                ),
            ),
        )

    async def _execute_live_market_order(
        self,
        *,
        asset_id: str,
        side: str,
        decision_timestamp: datetime,
        decision_snapshot: dict[str, float | None],
        note: str,
        is_initial_trade: bool,
    ) -> None:
        if (self.market.window_end - utc_now()).total_seconds() <= NO_NEW_TRADES_LAST_SECONDS:
            self._recent_events.append(
                f"{side.upper()} {self._books[asset_id].label} canceled immediately: market is inside final {NO_NEW_TRADES_LAST_SECONDS:.0f}s"
            )
            return

        assert self._live_client is not None
        quote_side = "BUY" if side == "buy" else "SELL"
        quote_payload = await asyncio.to_thread(self._live_client.get_price, asset_id, quote_side)
        quote_price = Decimal(str(quote_payload["price"]))
        if quote_price <= 0:
            self._recent_events.append(f"{side.upper()} {self._books[asset_id].label} aborted: no executable quote")
            return

        if side == "buy":
            requested_amount = Decimal(str(self.live_buy_notional_usd))
            estimated_shares = float(requested_amount / quote_price) if quote_price > 0 else 0.0
            estimated_fee = approx_crypto_5m_taker_fee_per_share(float(quote_price)) * estimated_shares
            estimated_total_cost = float(requested_amount) + estimated_fee
            if self.cash < float(requested_amount):
                self._recent_events.append(f"BUY {self._books[asset_id].label} aborted: insufficient live cash")
                return
            if self.current_market_exposure() + estimated_total_cost > self.max_market_exposure_usd:
                self._recent_events.append(f"BUY {self._books[asset_id].label} aborted: live exposure limit hit")
                return
        else:
            available_shares = Decimal(str(self.position_shares(asset_id)))
            requested_amount = calculate_live_sell_shares(
                Decimal(str(self.live_buy_notional_usd)),
                quote_price,
                available_shares,
            )
            if requested_amount <= 0:
                self._recent_events.append(f"SELL {self._books[asset_id].label} aborted: no live position to sell")
                return

        result = await execute_market_order(
            client=self._live_client,
            stream=self._live_user_channel,
            condition_id=self.market.condition_id,
            token_id=asset_id,
            side=side.upper(),
            amount=requested_amount,
            order_type=OrderType.FAK,
            current_price=quote_price,
            tick_size=Decimal("0.01"),
        )
        matched_event = result["matched_event"]
        matched_shares = float(matched_event.get("size") or 0.0)
        matched_price = float(matched_event.get("price") or 0.0)
        fee = approx_crypto_5m_taker_fee_per_share(matched_price) * matched_shares if matched_price > 0 else 0.0
        gross_notional = matched_shares * matched_price
        decision_to_matched_ms = float(result["decision_to_trade_matched_ms"])
        self._live_latency_stats.record(side=side.upper(), matched_ms=decision_to_matched_ms)
        self.trade_count += 1
        execution_timestamp = parse_iso_datetime(str(matched_event.get("received_at_utc") or "")) or utc_now()

        if side == "buy":
            total_cost = gross_notional + fee
            position = self._positions[asset_id]
            was_flat = position.shares <= 1e-9
            position.shares += matched_shares
            position.cost_basis += total_cost
            if was_flat:
                position.opened_at = execution_timestamp
            self.market_buy_budget_used_usd += total_cost
            self._record_buy_alpha(decision_snapshot["buy_alpha_net"])
            if is_initial_trade:
                self.initial_trade_taken = True
            note_text = note
            realized_delta = None
            proceeds_or_notional = gross_notional
            entry_type = "first_entry" if was_flat else "later_add"
            holding_time_seconds = None
        else:
            position = self._positions[asset_id]
            holding_time_seconds = self._holding_time_seconds(asset_id, execution_timestamp)
            cost_removed = min(position.average_cost * matched_shares, position.cost_basis)
            proceeds_or_notional = max(gross_notional - fee, 0.0)
            realized_delta = proceeds_or_notional - cost_removed
            self.realized_pnl += realized_delta
            position.shares = max(position.shares - matched_shares, 0.0)
            position.cost_basis = max(position.cost_basis - cost_removed, 0.0)
            if position.shares <= 1e-9:
                position.shares = 0.0
                position.cost_basis = 0.0
                position.opened_at = None
            self._record_closed_trade(matched_shares, realized_delta)
            note_text = "positive bid alpha"
            entry_type = ""

        self._last_trade_at[(asset_id, side)] = execution_timestamp
        self._recent_events.append(
            f"{side.upper()} {self._books[asset_id].label} matched in {decision_to_matched_ms:.1f}ms @ {matched_price:.4f}"
        )
        self._live_last_order_summary = {
            "side": side.upper(),
            "status": "MATCHED",
            "order_id": result["order_id"],
            "decision_to_matched_ms": decision_to_matched_ms,
            "decision_to_http_response_ms": result["decision_to_http_response_ms"],
            "response_status": result["response_status"],
        }
        await self.refresh_live_state(force=True)
        trade_log_context = self._trade_log_context(
            side=side,
            decision_timestamp=decision_timestamp,
            execution_timestamp=execution_timestamp,
            decision_snapshot=decision_snapshot,
            desired_price=float(quote_price),
            actual_price=matched_price,
            execution_delay_ms=decision_to_matched_ms,
            entry_type=entry_type,
            holding_time_seconds=holding_time_seconds,
        )
        self.recorder.write_live_trade(
            asset_id=asset_id,
            token_label=self._books[asset_id].label,
            action=side,
            requested_amount=float(result["requested_amount"]),
            filled_shares=matched_shares,
            matched_price=matched_price,
            fee=fee,
            fair_value=decision_snapshot["fair_value"],
            market_mid=decision_snapshot["market_mid"],
            buy_alpha_net=decision_snapshot["buy_alpha_net"],
            sell_alpha_net=decision_snapshot["sell_alpha_net"],
            cash_balance=self.cash,
            realized_pnl=self.realized_pnl,
            position_shares=self.position_shares(asset_id),
            position_average_cost=self.position_average_cost(asset_id),
            response_status=result["response_status"],
            order_id=result["order_id"],
            http_submit_latency_ms=result["http_submit_latency_ms"],
            decision_to_http_response_ms=result["decision_to_http_response_ms"],
            decision_to_matched_ms=decision_to_matched_ms,
            decision_to_confirmed_ms=None,
            note=note_text,
            recorded_at=execution_timestamp,
            decision_time_utc=str(trade_log_context["decision_time_utc"]),
            matched_time_utc=str(trade_log_context["matched_time_utc"]),
            seconds_to_expiry=trade_log_context["seconds_to_expiry"],
            fair_value_at_decision=trade_log_context["fair_value_at_decision"],
            best_bid_at_decision=trade_log_context["best_bid_at_decision"],
            best_ask_at_decision=trade_log_context["best_ask_at_decision"],
            alpha_at_decision=trade_log_context["trigger_alpha"],
            realized_pnl_on_close=realized_delta,
            holding_time_seconds=trade_log_context["holding_time_seconds"],
            entry_type=str(trade_log_context["entry_type"] or ""),
            desired_price=float(quote_price),
            actual_price=matched_price,
            trigger_alpha=trade_log_context["trigger_alpha"],
            underlying_price_to_beat=trade_log_context["underlying_price_to_beat"],
            underlying_price_actual=trade_log_context["underlying_price_actual"],
            time_left_seconds=trade_log_context["time_left_seconds"],
            sigma_per_sqrt_s=trade_log_context["sigma_per_sqrt_s"],
            fair_up=trade_log_context["fair_up"],
            execution_time_utc=str(trade_log_context["execution_time_utc"]),
            trade_number_in_market=int(trade_log_context["trade_number_in_market"]),
            execution_delay_ms=trade_log_context["execution_delay_ms"],
            account_value=float(trade_log_context["account_value"]),
            raw_json=self._compose_trade_raw_json(
                trade_log_context=trade_log_context,
                execution_trace=result,
            ),
        )
        self._spawn_live_confirmation_watch(
            asset_id=asset_id,
            side=side.upper(),
            order_id=result["order_id"],
            decision_monotonic_ns=int(result["decision_monotonic_ns"]),
        )

    def _spawn_live_confirmation_watch(
        self,
        *,
        asset_id: str,
        side: str,
        order_id: str,
        decision_monotonic_ns: int,
    ) -> None:
        task = asyncio.create_task(
            self._watch_live_confirmation(
                asset_id=asset_id,
                side=side,
                order_id=order_id,
                decision_monotonic_ns=decision_monotonic_ns,
            )
        )
        self._live_confirmation_tasks.add(task)

        def _cleanup(done: asyncio.Task[None]) -> None:
            self._live_confirmation_tasks.discard(done)

        task.add_done_callback(_cleanup)

    async def _watch_live_confirmation(
        self,
        *,
        asset_id: str,
        side: str,
        order_id: str,
        decision_monotonic_ns: int,
    ) -> None:
        try:
            assert self._live_client is not None
            confirmation = await wait_for_order_confirmation(
                stream=self._live_user_channel,
                client=self._live_client,
                condition_id=self.market.condition_id,
                order_id=order_id,
                decision_monotonic_ns=decision_monotonic_ns,
            )
        except Exception as exc:  # noqa: BLE001
            self._recent_events.append(f"{side} confirmation for {asset_id[-6:]} pending: {exc}")
            return

        self._live_last_order_summary = {
            "side": side,
            "status": "CONFIRMED",
            "order_id": order_id,
            "decision_to_confirmed_ms": confirmation["decision_to_trade_confirmed_ms"],
        }
        self.recorder.write_session_row(
            "live_trade_confirmation",
            {
                "asset_id": asset_id,
                "order_id": order_id,
                "side": side,
                "decision_to_confirmed_ms": confirmation["decision_to_trade_confirmed_ms"],
                "confirmed_event": confirmation["confirmed_event"],
            },
        )

    async def cancel_pending_orders(self) -> None:
        tasks = [task for task in self._pending_order_tasks.values() if not task.done()]
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def seed_reference_price(self, price: float, *, note: str) -> None:
        self.start_underlying_price = price
        self._recent_events.append(f"Reference seeded at {price:.4f} from {note}")

    def on_underlying_quote(self, quote: UnderlyingQuote) -> None:
        bid = float_or_none(quote.bid)
        ask = float_or_none(quote.ask)
        trade = float_or_none(quote.price)
        if bid is not None and ask is not None:
            price = (bid + ask) / 2.0
        else:
            price = trade
        if price is None:
            return
        self.latest_underlying_price = price
        self.latest_underlying_timestamp = quote.timestamp
        if quote.timestamp < self.market.window_start:
            return

        second = quote.timestamp.replace(microsecond=0)
        if self.start_underlying_price is None:
            self.start_underlying_price = price
            self._recent_events.append(f"Reference locked at {price:.4f}")

        if self._underlying_by_second and self._underlying_by_second[-1][0] == second:
            self._underlying_by_second[-1] = (second, price)
        else:
            self._underlying_by_second.append((second, price))

        self._maybe_trade(quote.timestamp)
        self._capture_history(quote.timestamp)

    def on_book_message(self, payload: dict[str, Any]) -> None:
        if str(payload.get("event_type") or payload.get("type") or "") != "book":
            return
        asset_id = str(payload.get("asset_id") or "")
        book = self._books.get(asset_id)
        if book is None:
            return

        message_timestamp = self._message_timestamp(payload)
        bids = payload.get("bids") if isinstance(payload.get("bids"), list) else []
        asks = payload.get("asks") if isinstance(payload.get("asks"), list) else []
        book.bid_levels = normalize_book_levels(bids, reverse=True)
        book.ask_levels = normalize_book_levels(asks)
        book.best_bid, book.best_bid_size = compute_best_level(bids, reverse=True)
        book.best_ask, book.best_ask_size = compute_best_level(asks)
        book.last_trade_price = float_or_none(payload.get("last_trade_price") or payload.get("price"))
        book.updated_at = message_timestamp

        self._maybe_trade(message_timestamp)
        self._capture_history(message_timestamp)

    def sigma_per_sqrt_second(self) -> float | None:
        if len(self._underlying_by_second) < 3:
            return None
        diffs = [
            current_price - previous_price
            for (_, previous_price), (_, current_price) in zip(self._underlying_by_second, list(self._underlying_by_second)[1:])
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
        return fair_up if asset_id == self.up_asset_id else 1.0 - fair_up

    def alpha_snapshot(self, asset_id: str, *, timestamp: datetime) -> dict[str, float | None]:
        book = self._books[asset_id]
        fair_value = self.fair_value_for(asset_id, timestamp=timestamp)
        market_mid = book.mid
        ask_fee = approx_crypto_5m_taker_fee_per_share(book.best_ask) if book.best_ask is not None else None
        bid_fee = approx_crypto_5m_taker_fee_per_share(book.best_bid) if book.best_bid is not None else None
        buy_alpha_gross = fair_value - book.best_ask if fair_value is not None and book.best_ask is not None else None
        sell_alpha_gross = book.best_bid - fair_value if fair_value is not None and book.best_bid is not None else None
        buy_alpha_net = buy_alpha_gross - ask_fee if buy_alpha_gross is not None and ask_fee is not None else None
        sell_alpha_net = sell_alpha_gross - bid_fee if sell_alpha_gross is not None and bid_fee is not None else None
        return {
            "fair_value": fair_value,
            "market_mid": market_mid,
            "buy_alpha_gross": buy_alpha_gross,
            "buy_alpha_net": buy_alpha_net,
            "sell_alpha_gross": sell_alpha_gross,
            "sell_alpha_net": sell_alpha_net,
        }

    def current_balance(self) -> float:
        if self.uses_live_execution():
            return self.marked_total_account_value()
        return self.cash + self.current_liquidation_value()

    def current_liquidation_value(self) -> float:
        if self.uses_live_execution():
            return self.executable_open_value()
        now = self.latest_underlying_timestamp or utc_now()
        return sum(self._position_liquidation_value(asset_id, timestamp=now) for asset_id in self._positions)

    def current_market_exposure(self) -> float:
        if self.uses_live_execution():
            return sum(position.cost_basis for position in self._positions.values())
        return sum(position.cost_basis for position in self._positions.values())

    def remaining_buy_budget(self) -> float:
        return max(self.max_market_exposure_usd - self.current_market_exposure(), 0.0)

    def unrealized_pnl(self) -> float:
        if self.uses_live_execution():
            return self.marked_open_value() - self.current_market_exposure()
        total = 0.0
        now = self.latest_underlying_timestamp or utc_now()
        for asset_id, position in self._positions.items():
            if position.shares <= 0:
                continue
            total += self._position_liquidation_value(asset_id, timestamp=now) - position.cost_basis
        return total

    def balance_std_dev(self) -> float | None:
        if len(self._history_balance) < 2:
            return None
        values = list(self._history_balance)
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
        return math.sqrt(max(variance, 0.0))

    def current_market_performance(self) -> MarketPerformanceStats:
        return MarketPerformanceStats(
            trade_count=self.trade_count,
            closed_trade_units=self._closed_trade_units,
            winning_trade_units=self._winning_trade_units,
            buy_trade_count=self._buy_trade_count,
            buy_alpha_sum=self._buy_alpha_sum,
            market_return=self.current_market_return() or 0.0,
        )

    def session_performance(self) -> SessionPerformanceStats:
        return self.session_performance_stats.including_market(self.current_market_performance())

    def current_market_return(self) -> float | None:
        if self.starting_balance <= 0:
            return None
        return (self.current_balance() / self.starting_balance) - 1.0

    def overall_return(self) -> float | None:
        if self.session_starting_balance <= 0:
            return None
        return (self.current_balance() / self.session_starting_balance) - 1.0

    def settle(self, *, timestamp: datetime, official_resolution: OfficialResolution | None = None) -> None:
        if self.uses_live_execution():
            self._recent_events.append("Live mode does not settle locally; waiting for Polymarket account state.")
            return
        resolution_note = ""
        if official_resolution is not None:
            up_wins = official_resolution.up_wins
            if official_resolution.price_to_beat is not None:
                self.start_underlying_price = official_resolution.price_to_beat
            if official_resolution.final_price is not None:
                self.latest_underlying_price = official_resolution.final_price
                self.latest_underlying_timestamp = timestamp
            if official_resolution.price_to_beat is not None and official_resolution.final_price is not None:
                resolution_note = (
                    f" ({official_resolution.final_price:.4f} vs {official_resolution.price_to_beat:.4f})"
                )
        else:
            if self.start_underlying_price is None or self.latest_underlying_price is None:
                self._recent_events.append("Settlement skipped: missing underlying reference.")
                return
            up_wins = self.latest_underlying_price >= self.start_underlying_price

        for asset_id, position in self._positions.items():
            if position.shares <= 0:
                continue
            label = self._books[asset_id].label
            payout_per_share = 1.0 if ((label.lower() == "up" and up_wins) or (label.lower() == "down" and not up_wins)) else 0.0
            proceeds = payout_per_share * position.shares
            realized = proceeds - position.cost_basis
            holding_time_seconds = self._holding_time_seconds(asset_id, timestamp)
            self.cash += proceeds
            self.realized_pnl += realized
            self._record_closed_trade(position.shares, realized)
            self.recorder.write_paper_trade(
                asset_id=asset_id,
                token_label=label,
                action="settle",
                shares=position.shares,
                price=payout_per_share,
                fee=0.0,
                fair_value=payout_per_share,
                market_mid=payout_per_share,
                buy_alpha_net=None,
                sell_alpha_net=None,
                cash_balance=self.cash,
                realized_pnl=self.realized_pnl,
                position_shares=0.0,
                position_average_cost=0.0,
                note=f"resolved {'Up' if up_wins else 'Down'}{resolution_note}",
                recorded_at=timestamp,
                matched_time_utc=timestamp.isoformat(),
                seconds_to_expiry=0.0,
                fair_value_at_decision=payout_per_share,
                realized_pnl_on_close=realized,
                holding_time_seconds=holding_time_seconds,
                desired_price=payout_per_share,
                actual_price=payout_per_share,
                underlying_price_to_beat=self.start_underlying_price,
                underlying_price_actual=self.latest_underlying_price,
                time_left_seconds=0.0,
                sigma_per_sqrt_s=self.sigma_per_sqrt_second(),
                fair_up=self.fair_up_probability(timestamp),
                execution_time_utc=timestamp.isoformat(),
                account_value=self.current_balance(),
            )
            self._recent_events.append(
                f"Settled {label} {position.shares:.0f} @ {payout_per_share:.2f}{resolution_note} | "
                f"realized {format_money(realized)}"
            )
            position.shares = 0.0
            position.cost_basis = 0.0
            position.opened_at = None

    def render_dashboard(self) -> Group:
        now = utc_now()
        remaining = max((self.market.window_end - now).total_seconds(), 0.0)
        fair_up = self.fair_up_probability(now)
        up_snapshot = self.alpha_snapshot(self.up_asset_id, timestamp=now)
        down_snapshot = self.alpha_snapshot(self.down_asset_id, timestamp=now)
        session_performance = self.session_performance()
        balance_std_dev = self.balance_std_dev()
        overall_return = self.overall_return()
        market_return = self.current_market_return()
        average_return_per_market = session_performance.average_return_per_market

        summary = Table.grid(expand=True, padding=(0, 2))
        summary.add_column(justify="left", ratio=1)
        summary.add_column(justify="left", ratio=2)
        summary.add_column(justify="left", ratio=1)
        summary.add_column(justify="left", ratio=2)

        left_summary_rows = [
            ("Mode", self.execution_mode),
            ("Market", self.market.title),
            ("Time Left", format_duration(remaining)),
            ("Balance", format_money(self.current_balance())),
            ("Cash", format_money(self.cash)),
        ]
        if self.uses_live_execution():
            left_summary_rows.extend(
                [
                    ("Marked Open", format_money(self.marked_open_value())),
                    ("Exec Open", format_money(self.executable_open_value())),
                    (
                        "Reserved Cash",
                        format_money(self.open_order_reserved_cash()),
                    ),
                    (
                        "Exposure",
                        f"{format_money(self.current_market_exposure())} / {format_money(self.max_market_exposure_usd)}",
                    ),
                    ("Realized", format_money(self.realized_pnl)),
                    ("Unrealized", format_money(self.unrealized_pnl())),
                    ("Trades", str(self.trade_count)),
                ]
            )
        else:
            left_summary_rows.extend(
                [
                    ("Open Value", format_money(self.current_liquidation_value())),
                    (
                        "Exposure",
                        f"{format_money(self.current_market_exposure())} / {format_money(self.max_market_exposure_usd)}",
                    ),
                    (
                        "Lifetime Buys",
                        f"{format_money(self.market_buy_budget_used_usd)} / {format_money(self.max_market_exposure_usd)}",
                    ),
                    ("Realized", format_money(self.realized_pnl)),
                    ("Unrealized", format_money(self.unrealized_pnl())),
                    ("Trades", str(self.trade_count)),
                ]
            )

        right_summary_rows = [
            ("Market Max Alpha", f"{self.market_max_buy_alpha_net:.4f}"),
            (
                "Avg Max Alpha",
                f"{self.historical_average_max_alpha:.4f}" if self.historical_average_max_alpha is not None else "-",
            ),
            ("First Entry Rule", "armed" if not self.initial_trade_taken else "used"),
            ("Total Trades", str(session_performance.total_trade_count)),
            (
                "Avg Win Rate/Trade",
                format_percent(session_performance.average_win_rate_per_trade)
                if session_performance.average_win_rate_per_trade is not None
                else "-",
            ),
            (
                "Avg Trades/Market",
                f"{session_performance.average_trades_per_market:.2f}"
                if session_performance.average_trades_per_market is not None
                else "-",
            ),
            (
                "Avg Buy Alpha",
                f"{session_performance.average_buy_alpha:+.4f}" if session_performance.average_buy_alpha is not None else "-",
            ),
            ("Balance Std Dev", format_money(balance_std_dev) if balance_std_dev is not None else "-"),
            ("Overall Return", format_percent(overall_return) if overall_return is not None else "-"),
            (
                "Return/Market",
                format_percent(average_return_per_market) if average_return_per_market is not None else "-",
            ),
            (
                "Return/Day",
                format_percent(average_return_per_market * 288.0) if average_return_per_market is not None else "-",
            ),
            ("This Market", format_percent(market_return) if market_return is not None else "-"),
        ]
        if self.uses_live_execution():
            right_summary_rows.extend(
                [
                    (
                        "Avg Buy Match ms",
                        f"{self.average_live_buy_match_ms():.1f}" if self.average_live_buy_match_ms() is not None else "-",
                    ),
                    (
                        "Avg Sell Match ms",
                        f"{self.average_live_sell_match_ms():.1f}" if self.average_live_sell_match_ms() is not None else "-",
                    ),
                    ("Last Order", self.last_live_order_status()),
                    (
                        "Marked Total",
                        format_money(self.marked_total_account_value()),
                    ),
                    (
                        "Exec Total",
                        format_money(self.executable_total_account_value()),
                    ),
                ]
            )
        row_count = max(len(left_summary_rows), len(right_summary_rows))
        for index in range(row_count):
            left_label, left_value = left_summary_rows[index] if index < len(left_summary_rows) else ("", "")
            right_label, right_value = right_summary_rows[index] if index < len(right_summary_rows) else ("", "")
            summary.add_row(left_label, left_value, right_label, right_value)

        underlying = Table.grid(expand=True)
        underlying.add_column(justify="left")
        underlying.add_column(justify="left")
        underlying.add_row("Start Ref", f"{self.start_underlying_price:.4f}" if self.start_underlying_price is not None else "-")
        underlying.add_row("Current Ref", f"{self.latest_underlying_price:.4f}" if self.latest_underlying_price is not None else "-")
        underlying.add_row("Sigma/sqrt(s)", f"{self.sigma_per_sqrt_second():.6f}" if self.sigma_per_sqrt_second() is not None else "-")
        underlying.add_row("Fair Up", f"{fair_up:.4f}" if fair_up is not None else "-")

        books = Table(expand=True)
        books.add_column("Token")
        books.add_column("Bid", justify="right")
        books.add_column("Ask", justify="right")
        books.add_column("Mid", justify="right")
        books.add_column("Fair", justify="right")
        books.add_column("Ask Alpha", justify="right")
        books.add_column("Bid Alpha", justify="right")
        books.add_column("Pos", justify="right")
        for asset_id in (self.up_asset_id, self.down_asset_id):
            book = self._books[asset_id]
            snapshot = up_snapshot if asset_id == self.up_asset_id else down_snapshot
            books.add_row(
                book.label,
                f"{book.best_bid:.4f}" if book.best_bid is not None else "-",
                f"{book.best_ask:.4f}" if book.best_ask is not None else "-",
                f"{snapshot['market_mid']:.4f}" if snapshot["market_mid"] is not None else "-",
                f"{snapshot['fair_value']:.4f}" if snapshot["fair_value"] is not None else "-",
                format_signed(snapshot["buy_alpha_net"]) if snapshot["buy_alpha_net"] is not None else "-",
                format_signed(snapshot["sell_alpha_net"]) if snapshot["sell_alpha_net"] is not None else "-",
                f"{self.position_shares(asset_id):.4f}" if self.position_shares(asset_id) > 0 else "0",
            )

        x_values = [
            max((timestamp - self.market.window_start).total_seconds(), 0.0)
            for timestamp in self._history_seconds
        ]
        market_duration_seconds = max((self.market.window_end - self.market.window_start).total_seconds(), 1.0)
        prob_chart = build_plotext_chart(
            x_values=x_values,
            series=[
                ("fair", list(self._history_fair_up), "cyan"),
                ("mid", list(self._history_up_mid), "yellow"),
            ],
            width=max(60, min(CONSOLE.size.width - 16, 120)),
            height=12,
            title="Fair Up vs Market Mid",
            x_min=0.0,
            x_max=market_duration_seconds,
            y_min=0.0,
            y_max=1.0,
        )
        alpha_low = min(self._history_up_alpha) if self._history_up_alpha else -0.05
        alpha_high = max(self._history_up_alpha) if self._history_up_alpha else 0.05
        padding = max((alpha_high - alpha_low) * 0.15, 0.01)
        alpha_chart = build_plotext_chart(
            x_values=x_values,
            series=[("alpha", list(self._history_up_alpha), "magenta")],
            width=max(60, min(CONSOLE.size.width - 16, 120)),
            height=10,
            title="Net Ask Alpha (Fair - Ask - Fee)",
            x_min=0.0,
            x_max=market_duration_seconds,
            y_min=alpha_low - padding,
            y_max=alpha_high + padding,
        )
        events = Text("\n".join(self._recent_events) if self._recent_events else "No paper trades yet.")

        return Group(
            Panel(summary, title="Session  |  Version 2.0", border_style="cyan"),
            Panel(underlying, title="Underlying", border_style="blue"),
            Panel(books, title="Live Book vs Fair", border_style="green"),
            Panel(Text.from_ansi(prob_chart), title="Probability Chart", border_style="magenta"),
            Panel(Text.from_ansi(alpha_chart), title="Alpha Chart", border_style="red"),
            Panel(events, title="Recent Events", border_style="yellow"),
        )

    def _capture_history(self, timestamp: datetime) -> None:
        if timestamp < self.market.window_start:
            return
        second = timestamp.replace(microsecond=0)
        balance = self.current_balance()
        if self._balance_history_seconds and self._balance_history_seconds[-1] == second:
            self._history_balance[-1] = balance
        else:
            self._balance_history_seconds.append(second)
            self._history_balance.append(balance)
        up_snapshot = self.alpha_snapshot(self.up_asset_id, timestamp=timestamp)
        if up_snapshot["fair_value"] is None or up_snapshot["market_mid"] is None or up_snapshot["buy_alpha_net"] is None:
            return
        if self._history_seconds and self._history_seconds[-1] == second:
            self._history_fair_up[-1] = up_snapshot["fair_value"]
            self._history_up_mid[-1] = up_snapshot["market_mid"]
            self._history_up_alpha[-1] = up_snapshot["buy_alpha_net"]
            return
        self._history_seconds.append(second)
        self._history_fair_up.append(up_snapshot["fair_value"])
        self._history_up_mid.append(up_snapshot["market_mid"])
        self._history_up_alpha.append(up_snapshot["buy_alpha_net"])

    def _mark_price(self, asset_id: str, *, timestamp: datetime) -> float:
        book = self._books[asset_id]
        if book.mid is not None:
            return book.mid
        fair_value = self.fair_value_for(asset_id, timestamp=timestamp)
        return fair_value if fair_value is not None else 0.0

    def _liquidation_price(self, asset_id: str, *, timestamp: datetime) -> float:
        book = self._books[asset_id]
        if book.best_bid is not None:
            return max(book.best_bid - approx_crypto_5m_taker_fee_per_share(book.best_bid), 0.0)
        proxy_mark = self._mark_price(asset_id, timestamp=timestamp)
        if proxy_mark <= 0:
            return 0.0
        return max(proxy_mark - approx_crypto_5m_taker_fee_per_share(proxy_mark), 0.0)

    def _position_liquidation_value(self, asset_id: str, *, timestamp: datetime) -> float:
        position = self._positions[asset_id]
        if position.shares <= 0:
            return 0.0
        return position.shares * self._liquidation_price(asset_id, timestamp=timestamp)

    def _trade_units(self, shares: float) -> float:
        if self.uses_live_execution():
            return 1.0 if shares > 1e-9 else 0.0
        if self.paper_trade_shares <= 0:
            return 0.0
        return max(shares / self.paper_trade_shares, 0.0)

    def _record_buy_alpha(self, buy_alpha_net: float | None) -> None:
        if buy_alpha_net is None:
            return
        self._buy_trade_count += 1
        self._buy_alpha_sum += buy_alpha_net

    def _record_closed_trade(self, shares: float, realized: float) -> None:
        trade_units = self._trade_units(shares)
        if trade_units <= 0:
            return
        self._closed_trade_units += trade_units
        if realized > 0:
            self._winning_trade_units += trade_units

    def _series_points(self, timestamps: deque[datetime], values: deque[float]) -> list[dict[str, float | str]]:
        return [
            {"timestamp": timestamp.isoformat(), "value": value}
            for timestamp, value in zip(timestamps, values, strict=True)
        ]

    def snapshot(self, *, session_status: str) -> dict[str, Any]:
        now = utc_now()
        snapshot_time = self.latest_underlying_timestamp or now
        fair_up = self.fair_up_probability(snapshot_time)
        books: list[dict[str, Any]] = []
        positions: list[dict[str, Any]] = []

        for asset_id, label in zip(self.market.token_ids, self.market.outcome_labels, strict=True):
            book = self._books[asset_id]
            alpha = self.alpha_snapshot(asset_id, timestamp=snapshot_time)
            if self.uses_live_execution():
                shares = self.position_shares(asset_id)
                avg_cost = self.position_average_cost(asset_id)
                cost_basis = self.position_cost_basis(asset_id)
                liquidation_price = self._live_executable_price(asset_id)
                liquidation_value = shares * liquidation_price
            else:
                position = self._positions[asset_id]
                liquidation_price = self._liquidation_price(asset_id, timestamp=snapshot_time)
                liquidation_value = self._position_liquidation_value(asset_id, timestamp=snapshot_time)
                shares = position.shares
                avg_cost = position.average_cost
                cost_basis = position.cost_basis
            row = {
                "asset_id": asset_id,
                "label": label,
                "best_bid": book.best_bid,
                "best_ask": book.best_ask,
                "mid": book.mid,
                "fair_value": alpha["fair_value"],
                "buy_alpha_net": alpha["buy_alpha_net"],
                "sell_alpha_net": alpha["sell_alpha_net"],
                "shares": shares,
                "avg_cost": avg_cost,
                "cost_basis": cost_basis,
                "liquidation_price": liquidation_price,
                "liquidation_value": liquidation_value,
                "unrealized_pnl": liquidation_value - cost_basis,
            }
            books.append(row)
            if shares > 0:
                positions.append(row)

        return {
            "updated_at_utc": now.isoformat(),
            "status": session_status,
            "market": {
                "asset": self.market.asset,
                "title": self.market.title,
                "slug": self.market.slug,
                "market_id": self.market.market_id,
                "condition_id": self.market.condition_id,
                "window_start_utc": self.market.window_start.isoformat(),
                "window_end_utc": self.market.window_end.isoformat(),
                "token_ids": list(self.market.token_ids),
                "outcome_labels": list(self.market.outcome_labels),
            },
            "summary": {
                "execution_mode": self.execution_mode,
                "cash": self.cash,
                "balance": self.current_balance(),
                "liquidation_value": self.current_liquidation_value(),
                "open_cost_basis": self.current_market_exposure(),
                "remaining_buy_budget": self.remaining_buy_budget(),
                "max_market_exposure_usd": self.max_market_exposure_usd,
                "realized_pnl": self.realized_pnl,
                "unrealized_pnl": self.unrealized_pnl(),
                "trade_count": self.trade_count,
                "time_left_seconds": max((self.market.window_end - now).total_seconds(), 0.0),
                "start_underlying_price": self.start_underlying_price,
                "latest_underlying_price": self.latest_underlying_price,
                "fair_up": fair_up,
                "market_max_buy_alpha_net": self.market_max_buy_alpha_net,
                "initial_trade_taken": self.initial_trade_taken,
                "paper_trade_shares": self.paper_trade_shares,
                "live_buy_notional_usd": self.live_buy_notional_usd,
                "min_alpha_net": self.min_alpha_net,
                "max_position_shares": self.max_position_shares,
                "open_order_reserved_cash_usdc": self.open_order_reserved_cash(),
                "marked_open_value_usdc": self.marked_open_value(),
                "executable_open_value_usdc": self.executable_open_value(),
                "marked_total_account_value_usdc": self.marked_total_account_value(),
                "executable_total_account_value_usdc": self.executable_total_account_value(),
                "avg_buy_match_latency_ms": self.average_live_buy_match_ms(),
                "avg_sell_match_latency_ms": self.average_live_sell_match_ms(),
                "last_live_order": self._live_last_order_summary,
            },
            "books": books,
            "positions": positions,
            "events": list(self._recent_events),
            "charts": {
                "balance": self._series_points(self._balance_history_seconds, self._history_balance),
                "fair_up": self._series_points(self._history_seconds, self._history_fair_up),
                "up_mid": self._series_points(self._history_seconds, self._history_up_mid),
                "up_alpha": self._series_points(self._history_seconds, self._history_up_alpha),
            },
        }

    def _maybe_trade(self, timestamp: datetime) -> None:
        seconds_since_start = (timestamp - self.market.window_start).total_seconds()
        seconds_remaining = (self.market.window_end - timestamp).total_seconds()
        queue_cutoff_seconds = NO_NEW_TRADES_LAST_SECONDS + self._current_queue_delay_seconds()
        if seconds_remaining <= queue_cutoff_seconds:
            if not self._late_trade_halt_logged:
                self._recent_events.append(
                    f"No new orders queued near expiry: final {NO_NEW_TRADES_LAST_SECONDS:.0f}s trading halt active"
                )
                self._late_trade_halt_logged = True
            return
        for asset_id in (self.up_asset_id, self.down_asset_id):
            snapshot = self.alpha_snapshot(asset_id, timestamp=timestamp)
            book = self._books[asset_id]
            snapshot["decision_best_bid"] = book.best_bid
            snapshot["decision_best_ask"] = book.best_ask
            if snapshot["buy_alpha_net"] is not None:
                self.market_max_buy_alpha_net = max(self.market_max_buy_alpha_net, snapshot["buy_alpha_net"])

            buy_fill = None
            if self.uses_live_execution():
                if book.best_ask is not None and self.cash >= self.live_buy_notional_usd:
                    buy_fill = (self.live_buy_notional_usd / max(book.best_ask, 1e-9), self.live_buy_notional_usd, 0.0)
            else:
                buy_fill = fill_buy_for_notional(
                    book.ask_levels,
                    target_notional=PAPER_TRADE_NOTIONAL_USD,
                )
            if (
                snapshot["buy_alpha_net"] is not None
                and buy_fill is not None
                and not self._pending_order_exists(asset_id)
                and self._cooldown_passed(asset_id, "buy", timestamp)
            ):
                is_initial_trade = not self.initial_trade_taken
                required_alpha = self.min_alpha_net
                note = "positive ask alpha"

                if is_initial_trade:
                    if self._initial_trade_pending:
                        continue
                    if seconds_since_start < self.initial_trade_wait_seconds:
                        continue
                    required_alpha = max(required_alpha, 0.1)
                    note = "first alpha >= 0.1 after 30s"
                else:
                    required_alpha = max(required_alpha, 0.2)
                    note = "at/above fixed 0.2 alpha threshold"

                if snapshot["buy_alpha_net"] < required_alpha:
                    continue

                self._recent_events.append(
                    (
                        f"QUEUE BUY {book.label} after 500ms | alpha {snapshot['buy_alpha_net']:+.4f}"
                        if not self.uses_live_execution()
                        else f"LIVE BUY {book.label} immediately | alpha {snapshot['buy_alpha_net']:+.4f}"
                    )
                )
                self._schedule_market_order(
                    asset_id=asset_id,
                    side="buy",
                    timestamp=timestamp,
                    snapshot=snapshot,
                    note=note,
                    is_initial_trade=is_initial_trade,
                )

            position_shares = self.position_shares(asset_id)
            allow_partial_position = (
                book.best_bid is not None
                and book.best_bid > 0
                and position_shares * book.best_bid < self.trade_notional_usd()
            )
            if self.uses_live_execution():
                sell_fill = None
                if position_shares > 1e-9 and book.best_bid is not None and book.best_bid > 0:
                    target_shares = min(self.trade_notional_usd() / book.best_bid, position_shares)
                    sell_fill = (target_shares, target_shares * book.best_bid, 0.0)
            else:
                sell_fill = fill_sell_for_notional(
                    book.bid_levels,
                    target_notional=PAPER_TRADE_NOTIONAL_USD,
                    max_shares=position_shares,
                    allow_partial_position=allow_partial_position,
                )
            if (
                snapshot["sell_alpha_net"] is not None
                and sell_fill is not None
                and snapshot["sell_alpha_net"] >= self.min_alpha_net
                and not self._pending_order_exists(asset_id)
                and self._cooldown_passed(asset_id, "sell", timestamp)
            ):
                self._recent_events.append(
                    (
                        f"QUEUE SELL {book.label} after 500ms | alpha {snapshot['sell_alpha_net']:+.4f}"
                        if not self.uses_live_execution()
                        else f"LIVE SELL {book.label} immediately | alpha {snapshot['sell_alpha_net']:+.4f}"
                    )
                )
                self._schedule_market_order(
                    asset_id=asset_id,
                    side="sell",
                    timestamp=timestamp,
                    snapshot=snapshot,
                    note="positive bid alpha",
                )

    def _cooldown_passed(self, asset_id: str, side: str, timestamp: datetime) -> bool:
        previous = self._last_trade_at.get((asset_id, side))
        if previous is None:
            return True
        return (timestamp - previous).total_seconds() >= self.trade_cooldown_seconds

    def _message_timestamp(self, payload: dict[str, Any]) -> datetime:
        raw_value = payload.get("timestamp")
        if raw_value is None:
            return utc_now()
        try:
            milliseconds = int(raw_value)
        except (TypeError, ValueError):
            return utc_now()
        return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)


async def live_dashboard(strategy: LivePaperStrategy, *, stop_event: asyncio.Event) -> None:
    if not CONSOLE.is_terminal:
        return
    with Live(strategy.render_dashboard(), console=CONSOLE, refresh_per_second=4, screen=True) as live:
        while not stop_event.is_set():
            live.update(strategy.render_dashboard())
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.5)
            except TimeoutError:
                continue


async def write_live_snapshots(
    strategy: LivePaperStrategy,
    recorder: "CsvRecorder",
    *,
    stop_event: asyncio.Event,
    status_getter: Callable[[], str],
) -> None:
    if recorder.output_path is None:
        return
    while not stop_event.is_set():
        recorder.write_snapshot(strategy.snapshot(session_status=status_getter()))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=0.5)
        except TimeoutError:
            continue
    recorder.write_snapshot(strategy.snapshot(session_status=status_getter()))


DETAILED_CSV_FIELDNAMES = (
    "market_slug",
    "market_id",
    "side",
    "token",
    "decision_timestamp_utc",
    "matched_timestamp_utc",
    "seconds_to_expiry",
    "fair_value_at_decision",
    "best_bid_at_decision",
    "best_ask_at_decision",
    "alpha_at_decision",
    "matched_price",
    "filled_shares",
    "fee",
    "realized_pnl_on_close",
    "holding_time_seconds",
    "entry_type",
    "underlying_start_ref",
    "underlying_current_ref",
)


class CsvRecorder:
    def __init__(self, output_path: Path | None, market: MarketWindow) -> None:
        self.output_path = output_path
        self.snapshot_path = output_path.with_suffix(".json") if output_path is not None else None
        self.market = market
        self.stats = RecorderStats()
        self._latest_underlying: UnderlyingQuote | None = None
        self._row_buffer: list[dict[str, str]] = []
        self._flush_threshold = 200
        if self.output_path is None:
            return
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.output_path.exists():
            pd.DataFrame(columns=list(DETAILED_CSV_FIELDNAMES)).to_csv(self.output_path, index=False)

    def close(self) -> None:
        self._flush_rows()

    def write_snapshot(self, snapshot: dict[str, Any]) -> None:
        if self.snapshot_path is None:
            return
        temp_path = self.snapshot_path.with_suffix(f"{self.snapshot_path.suffix}.tmp")
        temp_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        temp_path.replace(self.snapshot_path)

    def write_session_row(self, row_kind: str, raw_payload: dict[str, Any] | None = None) -> None:
        return

    def write_message(self, payload: dict[str, Any]) -> None:
        return

    def write_underlying_quote(self, quote: UnderlyingQuote) -> None:
        self._latest_underlying = quote

    def write_paper_trade(
        self,
        *,
        asset_id: str,
        token_label: str,
        action: str,
        shares: float,
        price: float,
        fee: float,
        fair_value: float | None,
        market_mid: float | None,
        buy_alpha_net: float | None,
        sell_alpha_net: float | None,
        cash_balance: float,
        realized_pnl: float,
        position_shares: float,
        position_average_cost: float,
        note: str,
        recorded_at: datetime,
        decision_time_utc: str = "",
        matched_time_utc: str = "",
        seconds_to_expiry: float | None = None,
        fair_value_at_decision: float | None = None,
        best_bid_at_decision: float | None = None,
        best_ask_at_decision: float | None = None,
        alpha_at_decision: float | None = None,
        realized_pnl_on_close: float | None = None,
        holding_time_seconds: float | None = None,
        entry_type: str = "",
        desired_price: float | None = None,
        actual_price: float | None = None,
        trigger_alpha: float | None = None,
        underlying_price_to_beat: float | None = None,
        underlying_price_actual: float | None = None,
        time_left_seconds: float | None = None,
        sigma_per_sqrt_s: float | None = None,
        fair_up: float | None = None,
        execution_time_utc: str = "",
        trade_number_in_market: int | None = None,
        execution_delay_ms: float | None = None,
        account_value: float | None = None,
        raw_json: str = "",
    ) -> None:
        self._write_row(
            market_slug=self.market.slug,
            market_id=self.market.market_id,
            side=action,
            token=token_label,
            decision_timestamp_utc=decision_time_utc or recorded_at.isoformat(),
            matched_timestamp_utc=matched_time_utc or recorded_at.isoformat(),
            seconds_to_expiry=f"{seconds_to_expiry:.3f}" if seconds_to_expiry is not None else "",
            fair_value_at_decision=(
                f"{fair_value_at_decision:.6f}" if fair_value_at_decision is not None else ""
            ),
            best_bid_at_decision=f"{best_bid_at_decision:.6f}" if best_bid_at_decision is not None else "",
            best_ask_at_decision=f"{best_ask_at_decision:.6f}" if best_ask_at_decision is not None else "",
            alpha_at_decision=f"{alpha_at_decision:.6f}" if alpha_at_decision is not None else "",
            matched_price=f"{actual_price if actual_price is not None else price:.6f}",
            filled_shares=f"{shares:.6f}",
            fee=f"{fee:.6f}",
            realized_pnl_on_close=(
                f"{realized_pnl_on_close:.6f}" if realized_pnl_on_close is not None else ""
            ),
            holding_time_seconds=f"{holding_time_seconds:.3f}" if holding_time_seconds is not None else "",
            entry_type=entry_type,
            underlying_start_ref=(
                f"{underlying_price_to_beat:.6f}" if underlying_price_to_beat is not None else ""
            ),
            underlying_current_ref=(
                f"{underlying_price_actual:.6f}" if underlying_price_actual is not None else ""
            ),
        )

    def write_live_trade(
        self,
        *,
        asset_id: str,
        token_label: str,
        action: str,
        requested_amount: float,
        filled_shares: float,
        matched_price: float,
        fee: float,
        fair_value: float | None,
        market_mid: float | None,
        buy_alpha_net: float | None,
        sell_alpha_net: float | None,
        cash_balance: float,
        realized_pnl: float,
        position_shares: float,
        position_average_cost: float,
        response_status: str,
        order_id: str,
        http_submit_latency_ms: float | None,
        decision_to_http_response_ms: float | None,
        decision_to_matched_ms: float | None,
        decision_to_confirmed_ms: float | None,
        note: str,
        recorded_at: datetime,
        decision_time_utc: str = "",
        matched_time_utc: str = "",
        seconds_to_expiry: float | None = None,
        fair_value_at_decision: float | None = None,
        best_bid_at_decision: float | None = None,
        best_ask_at_decision: float | None = None,
        alpha_at_decision: float | None = None,
        realized_pnl_on_close: float | None = None,
        holding_time_seconds: float | None = None,
        entry_type: str = "",
        desired_price: float | None = None,
        actual_price: float | None = None,
        trigger_alpha: float | None = None,
        underlying_price_to_beat: float | None = None,
        underlying_price_actual: float | None = None,
        time_left_seconds: float | None = None,
        sigma_per_sqrt_s: float | None = None,
        fair_up: float | None = None,
        execution_time_utc: str = "",
        trade_number_in_market: int | None = None,
        execution_delay_ms: float | None = None,
        account_value: float | None = None,
        raw_json: str = "",
    ) -> None:
        self._write_row(
            market_slug=self.market.slug,
            market_id=self.market.market_id,
            side=action,
            token=token_label,
            decision_timestamp_utc=decision_time_utc or recorded_at.isoformat(),
            matched_timestamp_utc=matched_time_utc or recorded_at.isoformat(),
            seconds_to_expiry=f"{seconds_to_expiry:.3f}" if seconds_to_expiry is not None else "",
            fair_value_at_decision=(
                f"{fair_value_at_decision:.6f}" if fair_value_at_decision is not None else ""
            ),
            best_bid_at_decision=f"{best_bid_at_decision:.6f}" if best_bid_at_decision is not None else "",
            best_ask_at_decision=f"{best_ask_at_decision:.6f}" if best_ask_at_decision is not None else "",
            alpha_at_decision=f"{alpha_at_decision:.6f}" if alpha_at_decision is not None else "",
            matched_price=f"{actual_price if actual_price is not None else matched_price:.6f}",
            filled_shares=f"{filled_shares:.6f}",
            fee=f"{fee:.6f}",
            realized_pnl_on_close=(
                f"{realized_pnl_on_close:.6f}" if realized_pnl_on_close is not None else ""
            ),
            holding_time_seconds=f"{holding_time_seconds:.3f}" if holding_time_seconds is not None else "",
            entry_type=entry_type,
            underlying_start_ref=(
                f"{underlying_price_to_beat:.6f}" if underlying_price_to_beat is not None else ""
            ),
            underlying_current_ref=(
                f"{underlying_price_actual:.6f}" if underlying_price_actual is not None else ""
            ),
        )

    def write_market_resolution(
        self,
        *,
        execution_mode: str,
        resolved_outcome: str,
        resolution_source: str,
        price_to_beat: float | None,
        final_price: float | None,
        recorded_at: datetime,
        cash_balance: float | None = None,
        account_value: float | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> None:
        return

    def _write_levels(self, *, event_type: str, side: str, asset_id: str, levels: list[dict[str, Any]]) -> None:
        return

    def _write_row(self, **overrides: str) -> None:
        row = {
            "market_slug": self.market.slug,
            "market_id": self.market.market_id,
            "side": "",
            "token": "",
            "decision_timestamp_utc": "",
            "matched_timestamp_utc": "",
            "seconds_to_expiry": "",
            "fair_value_at_decision": "",
            "best_bid_at_decision": "",
            "best_ask_at_decision": "",
            "alpha_at_decision": "",
            "matched_price": "",
            "filled_shares": "",
            "fee": "",
            "realized_pnl_on_close": "",
            "holding_time_seconds": "",
            "entry_type": "",
            "underlying_start_ref": "",
            "underlying_current_ref": "",
        }
        row.update(overrides)
        if self.output_path is not None:
            self._row_buffer.append({key: str(value) for key, value in row.items()})
            if len(self._row_buffer) >= self._flush_threshold:
                self._flush_rows()
        self.stats.total_rows += 1

    def _flush_rows(self) -> None:
        if self.output_path is None or not self._row_buffer:
            return
        frame = pd.DataFrame(self._row_buffer, columns=list(DETAILED_CSV_FIELDNAMES))
        write_header = not self.output_path.exists() or self.output_path.stat().st_size == 0
        frame.to_csv(self.output_path, mode="a", header=write_header, index=False)
        self._row_buffer.clear()


def fetch_assumed_resolution(market: MarketWindow) -> OfficialResolution | None:
    try:
        event_payload = http_json(f"{GAMMA_API_BASE}/events/{market.event_id}", timeout=20)
    except Exception as exc:  # noqa: BLE001
        log(
            f"Resolution lookup failed for {market.slug}: {exc}",
            label="RESOLVE",
            tone="yellow",
        )
        return None

    poll_state = describe_resolution_poll(event_payload)
    resolution = extract_official_resolution(event_payload)
    if resolution is not None:
        if resolution.price_to_beat is not None and resolution.final_price is not None:
            log(
                (
                    f"Assumed resolution for {market.slug}: "
                    f"{'Up' if resolution.up_wins else 'Down'} "
                    f"({resolution.final_price:.4f} vs {resolution.price_to_beat:.4f}) | "
                    f"{poll_state}"
                ),
                label="RESOLVE",
                tone="green",
            )
        else:
            log(
                f"Assumed resolution for {market.slug}: "
                f"{'Up' if resolution.up_wins else 'Down'} | {poll_state}",
                label="RESOLVE",
                tone="green",
            )
        return resolution

    log(
        f"Could not assume resolution for {market.slug}; falling back to local price comparison | {poll_state}",
        label="RESOLVE",
        tone="yellow",
    )
    return None


def build_market_resolution_payload(
    *,
    market: MarketWindow,
    strategy: LivePaperStrategy,
    official_resolution: OfficialResolution | None,
    recorded_at: datetime,
) -> dict[str, Any]:
    if official_resolution is not None:
        resolved_outcome = "Up" if official_resolution.up_wins else "Down"
        resolution_source = "official"
        price_to_beat = official_resolution.price_to_beat
        final_price = official_resolution.final_price
    elif strategy.start_underlying_price is not None and strategy.latest_underlying_price is not None:
        resolved_outcome = "Up" if strategy.latest_underlying_price >= strategy.start_underlying_price else "Down"
        resolution_source = "local_inference"
        price_to_beat = strategy.start_underlying_price
        final_price = strategy.latest_underlying_price
    else:
        resolved_outcome = "unknown"
        resolution_source = "unknown"
        price_to_beat = strategy.start_underlying_price
        final_price = strategy.latest_underlying_price

    return {
        "market_id": market.market_id,
        "slug": market.slug,
        "title": market.title,
        "execution_mode": strategy.execution_mode,
        "resolved_outcome": resolved_outcome,
        "resolution_source": resolution_source,
        "price_to_beat": price_to_beat,
        "final_price": final_price,
        "window_start_utc": market.window_start.isoformat(),
        "window_end_utc": market.window_end.isoformat(),
        "recorded_at_utc": recorded_at.isoformat(),
        "cash_balance": strategy.cash,
        "account_value": strategy.current_balance(),
    }


async def stream_market(
    market: MarketWindow,
    recorder: CsvRecorder,
    *,
    strategy: LivePaperStrategy,
    post_close_grace_seconds: int,
    underlying_config: UnderlyingConfig,
) -> StreamStats:
    subscribe_message = {"type": "market", "assets_ids": list(market.token_ids)}
    reconnect_backoff = 1.0
    reconnects = 0
    started_at = utc_now()
    session_deadline = market.window_end + timedelta(seconds=post_close_grace_seconds)
    if strategy.start_underlying_price is None:
        try:
            seeded_reference = fetch_previous_market_reference(market)
        except Exception as exc:  # noqa: BLE001
            seeded_reference = None
            log(
                f"Previous market reference lookup failed for {market.slug}: {exc}",
                label="REF",
                tone="yellow",
            )
        if seeded_reference is not None:
            reference_price, previous_slug = seeded_reference
            strategy.seed_reference_price(reference_price, note=f"Polymarket {previous_slug} finalPrice")
    underlying_stop = asyncio.Event()
    underlying_task = asyncio.create_task(
        stream_underlying_quotes(
            underlying_config,
            recorder,
            strategy,
            stop_event=underlying_stop,
        )
    )
    dashboard_stop = asyncio.Event()
    dashboard_task = asyncio.create_task(live_dashboard(strategy, stop_event=dashboard_stop))
    snapshot_status = {"value": "running"}
    snapshot_stop = asyncio.Event()
    snapshot_task = asyncio.create_task(
        write_live_snapshots(
            strategy,
            recorder,
            stop_event=snapshot_stop,
            status_getter=lambda: snapshot_status["value"],
        )
    )
    recorder.write_session_row(
        "session_start",
        {
            "execution_mode": strategy.execution_mode,
            "market_id": market.market_id,
            "slug": market.slug,
            "title": market.title,
            "window_start_utc": market.window_start.isoformat(),
            "window_end_utc": market.window_end.isoformat(),
            "token_ids": list(market.token_ids),
            "outcomes": list(market.outcome_labels),
            "underlying_source": underlying_config.source,
            "underlying_symbol": underlying_config.symbol,
            "underlying_feed_id": underlying_config.feed_id,
            "underlying_stream_slug": underlying_config.stream_slug,
            "starting_balance": strategy.starting_balance,
            "paper_trade_shares": strategy.paper_trade_shares,
            "live_buy_notional_usd": strategy.live_buy_notional_usd,
            "min_alpha_net": strategy.min_alpha_net,
            "max_position_shares": strategy.max_position_shares,
            "max_market_exposure_usd": strategy.max_market_exposure_usd,
        },
    )
    try:
        await strategy.start_runtime()
        while utc_now() < session_deadline:
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
                    while utc_now() < session_deadline:
                        remaining = max((session_deadline - utc_now()).total_seconds(), 0.1)
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=min(remaining, 1.0))
                        except TimeoutError:
                            continue
                        payload = json.loads(message)
                        entries = payload if isinstance(payload, list) else [payload]
                        for entry in entries:
                            if isinstance(entry, dict):
                                recorder.write_message(entry)
                                strategy.on_book_message(entry)
            except ConnectionClosed as exc:
                reconnects += 1
                log(f"WebSocket closed for {market.slug}: {exc}. Reconnecting in {reconnect_backoff:.1f}s.")
                await asyncio.sleep(reconnect_backoff)
                reconnect_backoff = min(reconnect_backoff * 2, 15.0)
            except Exception as exc:  # noqa: BLE001
                reconnects += 1
                log(f"WebSocket error for {market.slug}: {exc}. Reconnecting in {reconnect_backoff:.1f}s.")
                await asyncio.sleep(reconnect_backoff)
                reconnect_backoff = min(reconnect_backoff * 2, 15.0)
    finally:
        underlying_stop.set()
        await underlying_task
        await strategy.cancel_pending_orders()
        await strategy.stop_runtime()
        dashboard_stop.set()
        await dashboard_task
        log(
            f"Trading window closed for {market.slug}. Returning to terminal log view.",
            label="CLOSE",
            tone="cyan",
        )
        resolution_timestamp = utc_now()
        official_resolution = fetch_assumed_resolution(market)
        resolution_payload = build_market_resolution_payload(
            market=market,
            strategy=strategy,
            official_resolution=official_resolution,
            recorded_at=resolution_timestamp,
        )
        recorder.write_market_resolution(
            execution_mode=strategy.execution_mode,
            resolved_outcome=str(resolution_payload["resolved_outcome"]),
            resolution_source=str(resolution_payload["resolution_source"]),
            price_to_beat=float_or_none(resolution_payload["price_to_beat"]),
            final_price=float_or_none(resolution_payload["final_price"]),
            recorded_at=resolution_timestamp,
            cash_balance=strategy.cash,
            account_value=strategy.current_balance(),
            raw_payload=resolution_payload,
        )
        strategy.settle(timestamp=resolution_timestamp, official_resolution=official_resolution)
        if strategy.uses_live_execution():
            await strategy.refresh_live_state(force=True)
        snapshot_status["value"] = "settled"
        snapshot_stop.set()
        await snapshot_task

    recorder.write_session_row("session_end")
    return StreamStats(
        reconnects=reconnects,
        started_at=started_at,
        finished_at=utc_now(),
    )


async def wait_for_live_account_flat(
    client: Any,
    proxy_address: str,
    *,
    poll_seconds: float,
    reason: str,
) -> dict[str, Any]:
    first_notice = True
    while True:
        state = await asyncio.to_thread(fetch_live_account_state, client, proxy_address)
        if live_state_is_flat(state):
            return state
        if first_notice:
            log(
                f"{reason}. Live account not flat yet: {describe_live_state_guard(state)}. Waiting before trading the next market.",
                label="LIVE",
                tone="yellow",
            )
            first_notice = False
        await asyncio.sleep(poll_seconds)


async def wait_until(target: datetime, *, poll_seconds: int) -> None:
    remaining_total = max(int((target - utc_now()).total_seconds()), 0)
    if remaining_total <= 0:
        return

    with tqdm(total=remaining_total, desc="Waiting for market start", unit="s", dynamic_ncols=True) as progress:
        last_remaining = remaining_total
        while True:
            now = utc_now()
            if now >= target:
                if last_remaining > 0:
                    progress.update(last_remaining)
                return

            remaining = max(int((target - now).total_seconds()), 0)
            decrement = last_remaining - remaining
            if decrement > 0:
                progress.update(decrement)
                last_remaining = remaining
            await asyncio.sleep(min((target - now).total_seconds(), float(poll_seconds), 1.0))


async def run(args: argparse.Namespace) -> None:
    processed_market_ids: set[str] = set()
    recorded_count = 0
    session_balance = args.starting_balance
    session_starting_balance = args.starting_balance
    session_started_at = utc_now()
    live_env: dict[str, str] | None = None
    live_client: Any | None = None
    live_proxy_address = ""
    carry_forward_reference_price: float | None = None
    session_alpha_stats = SessionAlphaStats()
    session_performance_stats = SessionPerformanceStats()
    base_output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir = base_output_dir if args.write_csv else None
    session_output_path = (
        base_output_dir
        / args.asset
        / session_filename(
            asset=args.asset,
            execution_mode=args.execution_mode,
            started_at=session_started_at,
        )
        if args.write_csv
        else None
    )
    summary_output_path = None if args.write_csv else base_output_dir / args.asset / "market_summary.csv"
    underlying_config = resolve_underlying_config(args.asset, args.underlying_source)
    if args.execution_mode == "live":
        try:
            ensure_live_dependencies()
            live_env = load_live_env(Path(args.env_file))
            live_client = build_auth_client(live_env)
            live_proxy_address = live_env["POLYMARKET_PROXY_ADDRESS"]
            initial_live_state = await asyncio.to_thread(
                fetch_live_account_state,
                live_client,
                live_proxy_address,
            )
            session_balance = float(initial_live_state["marked_total_account_value_usdc"])
            session_starting_balance = session_balance
        except LiveDependencyError as exc:
            raise SystemExit(
                "Missing live-trading dependency. Install requests and py-clob-client for --execution-mode live."
            ) from exc
    print_banner(args.asset, output_dir, max_markets=args.max_markets, write_csv=args.write_csv)
    log(
        f"{args.execution_mode.title()} trader watching {args.asset} recurring 5-minute markets.",
        label="READY",
        tone="cyan",
    )
    if underlying_config.source == "coinbase_advanced_trade_ws":
        log(
            f"Underlying source: {underlying_config.symbol} via Coinbase Advanced Trade websocket "
            f"({underlying_config.feed_id}, channel={underlying_config.stream_slug}).",
            label="SOURCE",
            tone="dim",
        )
    else:
        log(
            f"Underlying source: {underlying_config.symbol} via Chainlink public delayed stream "
            f"({underlying_config.stream_slug}).",
            label="SOURCE",
            tone="dim",
        )
    if session_output_path is not None:
        log(
            f"Session CSV enabled at {session_output_path} (appends all markets until this run stops).",
            label="CSV",
            tone="dim",
        )
    if summary_output_path is not None:
        log(
            f"Summary CSV enabled at {summary_output_path} (one row per market).",
            label="SUMMARY",
            tone="dim",
        )
    if args.execution_mode == "live":
        log(
            f"Live baseline locked to {format_money(session_starting_balance)} using Polymarket account state.",
            label="LIVE",
            tone="cyan",
        )
        if not live_state_is_flat(initial_live_state):
            log(
                (
                    "Live account already has unresolved state at startup: "
                    f"{describe_live_state_guard(initial_live_state)}. "
                    "Continuing anyway."
                ),
                label="LIVE",
                tone="yellow",
            )

    while True:
        cycle_number = recorded_count + 1
        try:
            market = discover_next_market(
                args.asset,
                processed_market_ids,
                require_future_start=False,
            )
        except Exception as exc:  # noqa: BLE001
            if not is_recoverable_network_error(exc):
                raise
            await sleep_for_network_recovery(
                reason=(
                    f"Market discovery failed after cycle #{recorded_count} with a network error: {exc}."
                )
            )
            continue
        if market is None:
            log(
                "No future 5-minute market found. Retrying discovery shortly.",
                label="DISCOVER",
                tone="yellow",
            )
            await asyncio.sleep(args.discovery_poll_seconds)
            continue

        if args.dry_run:
            log_market_window(cycle_number, market, phase="NEXT", tone="cyan")
            log(f"token_ids={market.token_ids} | outcomes={market.outcome_labels}", label="TOKENS", tone="dim")
            return

        now = utc_now()
        output_path = session_output_path
        if now < market.window_start:
            log_market_window(
                cycle_number,
                market,
                phase="WAIT",
                tone="yellow",
                output_path=output_path,
            )
            log(
                f"Starts in {format_duration((market.window_start - now).total_seconds())}. "
                f"Recorded so far: {recorded_count}",
                label="COUNTDOWN",
                tone="dim",
            )
            await wait_until(market.window_start, poll_seconds=args.discovery_poll_seconds)

        if utc_now() >= market.window_end:
            processed_market_ids.add(market.market_id)
            log(
                f"Skipped {market.slug} because the window already closed before recording began.",
                label="SKIP",
                tone="yellow",
            )
            continue

        log_market_window(
            cycle_number,
            market,
            phase="LIVE",
            tone="green",
            output_path=output_path,
        )
        if args.execution_mode == "live":
            assert live_client is not None
            assert live_proxy_address
            live_state = await asyncio.to_thread(
                fetch_live_account_state,
                live_client,
                live_proxy_address,
            )
            session_balance = float(live_state["marked_total_account_value_usdc"])
            if not live_state_is_flat(live_state):
                log(
                    (
                        f"Starting {market.slug} with unresolved live state: "
                        f"{describe_live_state_guard(live_state)}."
                    ),
                    label="LIVE",
                    tone="yellow",
                )
        recorder = CsvRecorder(output_path=output_path, market=market)
        strategy = LivePaperStrategy(
            market=market,
            recorder=recorder,
            execution_mode=args.execution_mode,
            starting_balance=session_balance,
            session_starting_balance=session_starting_balance,
            paper_trade_shares=args.paper_trade_shares,
            live_buy_notional_usd=args.live_buy_notional_usd,
            min_alpha_net=args.min_alpha_net,
            max_position_shares=args.max_position_shares,
            max_market_exposure_usd=args.max_market_exposure_usd,
            historical_average_max_alpha=session_alpha_stats.average_max_alpha,
            session_performance_stats=session_performance_stats,
            trade_cooldown_seconds=args.trade_cooldown_seconds,
            env_file=args.env_file,
        )
        if carry_forward_reference_price is not None:
            strategy.seed_reference_price(
                carry_forward_reference_price,
                note="previous market close",
            )
            carry_forward_reference_price = None
        if summary_output_path is not None:
            initial_summary_recorded_at = utc_now()
            upsert_market_summary_row(
                summary_output_path,
                build_market_summary_row(
                    recorded_at=initial_summary_recorded_at,
                    summary_phase="started",
                    cycle_number=cycle_number,
                    market=market,
                    strategy=strategy,
                    session_alpha_stats=session_alpha_stats,
                    session_performance_stats=session_performance_stats,
                ),
            )
            log(
                f"Created initial market summary row for {market.market_id}: {summary_output_path}",
                label="SUMMARY",
                tone="cyan",
            )
        try:
            stream_stats = await stream_market(
                market=market,
                recorder=recorder,
                strategy=strategy,
                post_close_grace_seconds=args.post_close_grace_seconds,
                underlying_config=underlying_config,
            )
        finally:
            recorder.close()

        processed_market_ids.add(market.market_id)
        recorded_count += 1
        session_balance = strategy.current_balance()
        if args.execution_mode == "live" and not strategy.live_account_is_flat():
            log(
                (
                    f"Post-market state for {market.slug}: {strategy.live_guard_note()}. "
                    "Continuing to the next market while prior positions await resolution."
                ),
                label="LIVE",
                tone="yellow",
            )
        carry_forward_reference_price = strategy.latest_underlying_price
        session_alpha_stats = session_alpha_stats.with_market(strategy.market_max_buy_alpha_net)
        session_performance_stats = session_performance_stats.with_market(strategy.current_market_performance())
        cycle_finished_at = utc_now()
        cycle_summary_text, _ = build_market_terminal_summary(
            cycle_number=cycle_number,
            prefix=(
                "finished in "
                f"{format_duration((stream_stats.finished_at - stream_stats.started_at).total_seconds())}"
            ),
            strategy=strategy,
            session_alpha_stats=session_alpha_stats,
            session_performance_stats=session_performance_stats,
            stream_stats=stream_stats,
        )
        log(
            cycle_summary_text,
            label="DONE",
            tone="green",
        )
        if summary_output_path is not None:
            upsert_market_summary_row(
                summary_output_path,
                build_market_summary_row(
                    recorded_at=cycle_finished_at,
                    summary_phase="finished",
                    cycle_number=cycle_number,
                    market=market,
                    strategy=strategy,
                    session_alpha_stats=session_alpha_stats,
                    session_performance_stats=session_performance_stats,
                    stream_stats=stream_stats,
                ),
            )
            log(
                f"Updated market summary CSV with one row for {market.market_id}: {summary_output_path}",
                label="SUMMARY",
                tone="cyan",
            )
        log(
            f"Next cycle starting balance will be {format_money(session_balance)}.",
            label="ROLLOVER",
            tone="dim",
        )
        if output_path is not None:
            log(
                f"Saved CSV #{recorded_count} to {output_path}",
                label="OUTPUT",
                tone="cyan",
            )

        if args.max_markets > 0 and recorded_count >= args.max_markets:
            log("Reached max-markets limit. Exiting.", label="STOP", tone="yellow")
            return


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a live paper trader against recurring 5-minute Polymarket Up/Down crypto markets."
        )
    )
    parser.add_argument(
        "--asset",
        choices=["ethereum", "solana"],
        default="ethereum",
        help="Recurring crypto market family to track.",
    )
    parser.add_argument(
        "--underlying-source",
        choices=["coinbase", "chainlink-delayed"],
        default="coinbase",
        help="Underlying reference feed for fair-value calculations.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/market_recordings",
        help=(
            "Directory where per-market CSV files are stored when --write-csv is enabled, "
            "or where the summary CSV is stored otherwise."
        ),
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Persist market, underlying, and paper trade rows to CSV.",
    )
    parser.add_argument(
        "--execution-mode",
        choices=["paper", "live"],
        default="paper",
        help="Execution backend. Paper keeps local simulated fills; live submits real Polymarket orders.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to Polymarket credentials used by --execution-mode live.",
    )
    parser.add_argument(
        "--discovery-poll-seconds",
        type=int,
        default=15,
        help="How often to refresh market discovery while waiting for the next window.",
    )
    parser.add_argument(
        "--post-close-grace-seconds",
        type=int,
        default=5,
        help="Extra time to keep listening after the market window closes.",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=0,
        help="Stop after recording this many markets. Use 0 to run forever.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the next discovered market and exit without trading.",
    )
    parser.add_argument(
        "--starting-balance",
        type=float,
        default=50.0,
        help="Starting paper balance in USD. Ignored in live mode.",
    )
    parser.add_argument(
        "--paper-trade-shares",
        type=float,
        default=1.0,
        help="Shares to buy or sell per paper trade. Used only in paper mode analytics.",
    )
    parser.add_argument(
        "--live-buy-notional-usd",
        type=float,
        default=1.0,
        help="Fixed USD notional for each live buy. Live sells target the same notional, capped by available shares.",
    )
    parser.add_argument(
        "--min-alpha-net",
        type=float,
        default=0.0,
        help="Minimum net alpha required before a paper trade is taken.",
    )
    parser.add_argument(
        "--max-position-shares",
        type=float,
        default=5.0,
        help="Maximum paper position size per token.",
    )
    parser.add_argument(
        "--max-market-exposure-usd",
        type=float,
        default=3.0,
        help="Maximum open paper cost basis allowed across the whole market.",
    )
    parser.add_argument(
        "--trade-cooldown-seconds",
        type=float,
        default=2.0,
        help="Minimum seconds between same-side paper trades on the same token.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

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
