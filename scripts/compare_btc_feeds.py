#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import plotext as plt
import websockets
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from websockets.exceptions import ConnectionClosed


COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
POLYMARKET_RTDS_URL = "wss://ws-live-data.polymarket.com"
CONSOLE = Console()


@dataclass
class FeedTick:
    price: float
    source_timestamp_ms: int
    received_at: datetime


@dataclass
class ComparisonRow:
    compared_at: datetime
    coinbase_price: float
    coinbase_timestamp_ms: int
    chainlink_price: float
    chainlink_timestamp_ms: int
    price_delta: float
    price_delta_bps: float
    source_timestamp_delta_ms: int
    receive_delta_ms: float


class FeedComparator:
    def __init__(self, *, max_rows: int, csv_path: Path | None) -> None:
        self.coinbase_tick: FeedTick | None = None
        self.chainlink_tick: FeedTick | None = None
        self.rows: deque[ComparisonRow] = deque(maxlen=max_rows)
        self.history_seconds: deque[datetime] = deque(maxlen=max_rows)
        self.history_coinbase: deque[float] = deque(maxlen=max_rows)
        self.history_chainlink: deque[float] = deque(maxlen=max_rows)
        self.csv_path = csv_path
        self._csv_writer: csv.DictWriter | None = None
        self._csv_handle = None
        self._lock = asyncio.Lock()
        self._events: deque[str] = deque(maxlen=12)

    def add_event(self, message: str) -> None:
        timestamp = datetime.now(UTC).strftime("%H:%M:%S")
        self._events.appendleft(f"{timestamp} {message}")

    def open_csv(self) -> None:
        if self.csv_path is None:
            return
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self._csv_handle = self.csv_path.open("w", newline="", encoding="utf-8")
        self._csv_writer = csv.DictWriter(
            self._csv_handle,
            fieldnames=[
                "compared_at_utc",
                "coinbase_price",
                "coinbase_timestamp_ms",
                "chainlink_price",
                "chainlink_timestamp_ms",
                "price_delta",
                "price_delta_bps",
                "source_timestamp_delta_ms",
                "receive_delta_ms",
            ],
        )
        self._csv_writer.writeheader()
        self._csv_handle.flush()

    def close_csv(self) -> None:
        if self._csv_handle is not None:
            self._csv_handle.close()
            self._csv_handle = None
            self._csv_writer = None

    async def update_coinbase(self, *, price: float, source_timestamp_ms: int, received_at: datetime) -> None:
        async with self._lock:
            self.coinbase_tick = FeedTick(price=price, source_timestamp_ms=source_timestamp_ms, received_at=received_at)
            self._record_if_pairable()

    async def update_chainlink(self, *, price: float, source_timestamp_ms: int, received_at: datetime) -> None:
        async with self._lock:
            self.chainlink_tick = FeedTick(price=price, source_timestamp_ms=source_timestamp_ms, received_at=received_at)
            self._record_if_pairable()

    def _record_if_pairable(self) -> None:
        if self.coinbase_tick is None or self.chainlink_tick is None:
            return
        coinbase = self.coinbase_tick
        chainlink = self.chainlink_tick
        compared_at = datetime.now(UTC)
        self._record_price_history(compared_at, coinbase.price, chainlink.price)
        price_delta = chainlink.price - coinbase.price
        price_delta_bps = 0.0 if math.isclose(coinbase.price, 0.0) else (price_delta / coinbase.price) * 10_000.0
        row = ComparisonRow(
            compared_at=compared_at,
            coinbase_price=coinbase.price,
            coinbase_timestamp_ms=coinbase.source_timestamp_ms,
            chainlink_price=chainlink.price,
            chainlink_timestamp_ms=chainlink.source_timestamp_ms,
            price_delta=price_delta,
            price_delta_bps=price_delta_bps,
            source_timestamp_delta_ms=chainlink.source_timestamp_ms - coinbase.source_timestamp_ms,
            receive_delta_ms=(chainlink.received_at - coinbase.received_at).total_seconds() * 1000.0,
        )
        if self.rows:
            previous = self.rows[-1]
            if (
                previous.coinbase_timestamp_ms == row.coinbase_timestamp_ms
                and previous.chainlink_timestamp_ms == row.chainlink_timestamp_ms
            ):
                return
        self.rows.append(row)
        if self._csv_writer is not None:
            self._csv_writer.writerow(
                {
                    "compared_at_utc": row.compared_at.isoformat(),
                    "coinbase_price": f"{row.coinbase_price:.8f}",
                    "coinbase_timestamp_ms": row.coinbase_timestamp_ms,
                    "chainlink_price": f"{row.chainlink_price:.8f}",
                    "chainlink_timestamp_ms": row.chainlink_timestamp_ms,
                    "price_delta": f"{row.price_delta:.8f}",
                    "price_delta_bps": f"{row.price_delta_bps:.6f}",
                    "source_timestamp_delta_ms": row.source_timestamp_delta_ms,
                    "receive_delta_ms": f"{row.receive_delta_ms:.3f}",
                }
            )
            self._csv_handle.flush()

    def _record_price_history(self, timestamp: datetime, coinbase_price: float, chainlink_price: float) -> None:
        second_timestamp = timestamp.replace(microsecond=0)
        if self.history_seconds and self.history_seconds[-1] == second_timestamp:
            self.history_coinbase[-1] = coinbase_price
            self.history_chainlink[-1] = chainlink_price
            return
        self.history_seconds.append(second_timestamp)
        self.history_coinbase.append(coinbase_price)
        self.history_chainlink.append(chainlink_price)

    def snapshot(self) -> dict[str, object]:
        coinbase = self.coinbase_tick
        chainlink = self.chainlink_tick
        rows = list(self.rows)
        abs_deltas = [abs(row.price_delta) for row in rows]
        abs_delta_bps = [abs(row.price_delta_bps) for row in rows]
        ts_lags = [row.source_timestamp_delta_ms for row in rows]
        recv_lags = [row.receive_delta_ms for row in rows]
        return {
            "coinbase": coinbase,
            "chainlink": chainlink,
            "rows": rows[-8:],
            "count": len(rows),
            "avg_abs_delta": (sum(abs_deltas) / len(abs_deltas)) if abs_deltas else None,
            "max_abs_delta": max(abs_deltas) if abs_deltas else None,
            "avg_abs_delta_bps": (sum(abs_delta_bps) / len(abs_delta_bps)) if abs_delta_bps else None,
            "avg_source_timestamp_delta_ms": (sum(ts_lags) / len(ts_lags)) if ts_lags else None,
            "avg_receive_delta_ms": (sum(recv_lags) / len(recv_lags)) if recv_lags else None,
            "history_seconds": self.history_seconds,
            "history_coinbase": self.history_coinbase,
            "history_chainlink": self.history_chainlink,
            "events": list(self._events),
        }


def format_price(value: float | None) -> str:
    return "-" if value is None else f"{value:,.2f}"


def format_ms(value: float | int | None) -> str:
    return "-" if value is None else f"{value:,.1f} ms"


def format_bps(value: float | None) -> str:
    return "-" if value is None else f"{value:+.3f} bps"


def build_plotext_chart(
    *,
    x_values: list[float],
    series: list[tuple[str, list[float], str]],
    width: int,
    height: int,
    title: str,
) -> str:
    if not x_values or not any(values for _, values, _ in series):
        return "Waiting for enough live data to draw chart."

    plt.clear_figure()
    plt.theme("clear")
    plt.plotsize(width, height)
    plt.title(title)
    plt.xlabel("sec from start")
    plt.grid(True, True)

    for label, values, color_name in series:
        if not values:
            continue
        points = x_values[-len(values):]
        plt.plot(points, values, color=color_name, label=label)

    return plt.build()


def render_dashboard(snapshot: dict[str, object]) -> Panel:
    coinbase: FeedTick | None = snapshot["coinbase"]  # type: ignore[assignment]
    chainlink: FeedTick | None = snapshot["chainlink"]  # type: ignore[assignment]
    rows: list[ComparisonRow] = snapshot["rows"]  # type: ignore[assignment]
    events: list[str] = snapshot["events"]  # type: ignore[assignment]
    history_seconds: deque[datetime] = snapshot["history_seconds"]  # type: ignore[assignment]
    history_coinbase: deque[float] = snapshot["history_coinbase"]  # type: ignore[assignment]
    history_chainlink: deque[float] = snapshot["history_chainlink"]  # type: ignore[assignment]

    summary = Table.grid(expand=True)
    summary.add_column()
    summary.add_column()
    summary.add_row("Coinbase BTC-USD", format_price(coinbase.price if coinbase else None))
    summary.add_row("RTDS Chainlink btc/usd", format_price(chainlink.price if chainlink else None))
    summary.add_row("Samples", str(snapshot["count"]))
    summary.add_row("Avg |Delta|", format_price(snapshot["avg_abs_delta"]))  # type: ignore[arg-type]
    summary.add_row("Max |Delta|", format_price(snapshot["max_abs_delta"]))  # type: ignore[arg-type]
    summary.add_row("Avg |Delta| bps", format_bps(snapshot["avg_abs_delta_bps"]))  # type: ignore[arg-type]
    summary.add_row("Avg source ts lag", format_ms(snapshot["avg_source_timestamp_delta_ms"]))  # type: ignore[arg-type]
    summary.add_row("Avg receive lag", format_ms(snapshot["avg_receive_delta_ms"]))  # type: ignore[arg-type]

    table = Table(title="Recent Comparisons", expand=True)
    table.add_column("Compared")
    table.add_column("Coinbase")
    table.add_column("Chainlink")
    table.add_column("Delta")
    table.add_column("Delta bps")
    table.add_column("Source ts lag")
    table.add_column("Receive lag")
    for row in rows:
        table.add_row(
            row.compared_at.strftime("%H:%M:%S"),
            format_price(row.coinbase_price),
            format_price(row.chainlink_price),
            f"{row.price_delta:+.2f}",
            format_bps(row.price_delta_bps),
            format_ms(row.source_timestamp_delta_ms),
            format_ms(row.receive_delta_ms),
        )

    recent_events = "\n".join(events) if events else "Waiting for feed events."
    x_values = []
    if history_seconds:
        start = history_seconds[0]
        x_values = [max((point - start).total_seconds(), 0.0) for point in history_seconds]
    price_chart = build_plotext_chart(
        x_values=x_values,
        series=[
            ("coinbase", list(history_coinbase), "yellow"),
            ("rtds chainlink", list(history_chainlink), "cyan"),
        ],
        width=92,
        height=14,
        title="BTC Price Feeds",
    )
    return Panel(
        Group(summary, Panel(price_chart, title="Price Chart"), table, Panel(recent_events, title="Events")),
        title="BTC Feed Compare",
        border_style="cyan",
    )


async def stream_coinbase(comparator: FeedComparator, stop_event: asyncio.Event) -> None:
    subscribe_messages = [
        {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "ticker"},
        {"type": "subscribe", "channel": "heartbeats"},
    ]
    reconnect_backoff = 1.0
    while not stop_event.is_set():
        try:
            async with websockets.connect(
                COINBASE_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                open_timeout=20,
                max_size=None,
            ) as websocket:
                comparator.add_event("Coinbase connected")
                for message in subscribe_messages:
                    await websocket.send(json.dumps(message))
                reconnect_backoff = 1.0
                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    except TimeoutError:
                        continue
                    payload = json.loads(raw)
                    if str(payload.get("channel") or "") != "ticker":
                        continue
                    received_at = datetime.now(UTC)
                    for event in payload.get("events", []):
                        if not isinstance(event, dict):
                            continue
                        for ticker in event.get("tickers", []):
                            if not isinstance(ticker, dict):
                                continue
                            if str(ticker.get("product_id") or "") != "BTC-USD":
                                continue
                            price = float(ticker["price"])
                            ts = datetime.fromisoformat(str(payload.get("timestamp")).replace("Z", "+00:00"))
                            await comparator.update_coinbase(
                                price=price,
                                source_timestamp_ms=int(ts.timestamp() * 1000),
                                received_at=received_at,
                            )
        except ConnectionClosed as exc:
            comparator.add_event(f"Coinbase closed: {exc}")
        except Exception as exc:  # noqa: BLE001
            comparator.add_event(f"Coinbase error: {exc}")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=reconnect_backoff)
        except TimeoutError:
            reconnect_backoff = min(reconnect_backoff * 2, 15.0)
            continue
        return


async def rtds_ping(websocket: websockets.ClientConnection, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(5.0)
        await websocket.send("PING")


async def stream_polymarket_rtds(comparator: FeedComparator, stop_event: asyncio.Event) -> None:
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
                comparator.add_event("Polymarket RTDS connected")
                await websocket.send(json.dumps(subscribe_message))
                ping_task = asyncio.create_task(rtds_ping(websocket, stop_event))
                reconnect_backoff = 1.0
                try:
                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        except TimeoutError:
                            continue
                        if raw == "PONG":
                            continue
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            if raw:
                                comparator.add_event(f"RTDS non-JSON frame ignored: {raw[:32]!r}")
                            continue
                        if str(payload.get("topic") or "") != "crypto_prices_chainlink":
                            continue
                        inner = payload.get("payload")
                        if not isinstance(inner, dict):
                            continue
                        if str(inner.get("symbol") or "") != "btc/usd":
                            continue
                        await comparator.update_chainlink(
                            price=float(inner["value"]),
                            source_timestamp_ms=int(inner["timestamp"]),
                            received_at=datetime.now(UTC),
                        )
                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
        except ConnectionClosed as exc:
            comparator.add_event(f"RTDS closed: {exc}")
        except Exception as exc:  # noqa: BLE001
            comparator.add_event(f"RTDS error: {exc}")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=reconnect_backoff)
        except TimeoutError:
            reconnect_backoff = min(reconnect_backoff * 2, 15.0)
            continue
        return


async def live_dashboard(comparator: FeedComparator, stop_event: asyncio.Event) -> None:
    with Live(render_dashboard(comparator.snapshot()), console=CONSOLE, refresh_per_second=4, screen=False) as live:
        while not stop_event.is_set():
            live.update(render_dashboard(comparator.snapshot()), refresh=True)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.25)
            except TimeoutError:
                continue


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare Coinbase BTC-USD against Polymarket RTDS Chainlink btc/usd."
    )
    parser.add_argument("--duration-seconds", type=float, default=0.0, help="0 means run until Ctrl+C")
    parser.add_argument("--max-rows", type=int, default=5000)
    parser.add_argument("--csv-path", default="")
    return parser


async def run(args: argparse.Namespace) -> None:
    csv_path = Path(args.csv_path) if args.csv_path else None
    comparator = FeedComparator(max_rows=args.max_rows, csv_path=csv_path)
    comparator.open_csv()
    stop_event = asyncio.Event()
    tasks = [
        asyncio.create_task(stream_coinbase(comparator, stop_event)),
        asyncio.create_task(stream_polymarket_rtds(comparator, stop_event)),
        asyncio.create_task(live_dashboard(comparator, stop_event)),
    ]
    try:
        if args.duration_seconds > 0:
            await asyncio.sleep(args.duration_seconds)
        else:
            await asyncio.Future()
    finally:
        stop_event.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        comparator.close_csv()


def main() -> int:
    args = build_parser().parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
