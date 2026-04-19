#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import threading
from collections.abc import Sequence

import matplotlib.pyplot as plt

from compare_btc_feeds import FeedComparator, stream_coinbase, stream_polymarket_rtds


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Open a live matplotlib window comparing Coinbase BTC-USD vs Polymarket RTDS Chainlink btc/usd."
    )
    parser.add_argument("--max-rows", type=int, default=5000)
    parser.add_argument("--refresh-seconds", type=float, default=0.25)
    return parser


async def _watch_threading_stop(stop_flag: threading.Event, async_stop: asyncio.Event) -> None:
    while not stop_flag.is_set():
        await asyncio.sleep(0.2)
    async_stop.set()


async def _run_feed_tasks(comparator: FeedComparator, stop_flag: threading.Event) -> None:
    async_stop = asyncio.Event()
    tasks = [
        asyncio.create_task(stream_coinbase(comparator, async_stop)),
        asyncio.create_task(stream_polymarket_rtds(comparator, async_stop)),
        asyncio.create_task(_watch_threading_stop(stop_flag, async_stop)),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        async_stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)


def launch_feed_thread(comparator: FeedComparator, stop_flag: threading.Event) -> threading.Thread:
    thread = threading.Thread(
        target=lambda: asyncio.run(_run_feed_tasks(comparator, stop_flag)),
        name="btc-feed-compare",
        daemon=True,
    )
    thread.start()
    return thread


def _set_line_data(line, x_values: Sequence[float], y_values: Sequence[float]) -> None:
    line.set_data(list(x_values), list(y_values))


def run_window(*, max_rows: int, refresh_seconds: float) -> None:
    comparator = FeedComparator(max_rows=max_rows, csv_path=None)
    stop_flag = threading.Event()
    feed_thread = launch_feed_thread(comparator, stop_flag)

    fig, (ax_price, ax_delta) = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
    fig.canvas.manager.set_window_title("BTC Feed Compare")
    fig.suptitle("Coinbase vs Polymarket RTDS Chainlink")

    coinbase_line, = ax_price.plot([], [], label="Coinbase BTC-USD", color="tab:orange", linewidth=1.8)
    chainlink_line, = ax_price.plot([], [], label="RTDS Chainlink btc/usd", color="tab:blue", linewidth=1.8)
    delta_line, = ax_delta.plot([], [], label="Chainlink - Coinbase", color="tab:green", linewidth=1.6)
    zero_line = ax_delta.axhline(0.0, color="gray", linewidth=1.0, linestyle="--")
    zero_line.set_label("_nolegend_")

    ax_price.set_ylabel("Price")
    ax_delta.set_ylabel("Delta")
    ax_delta.set_xlabel("Seconds Since First Pair")
    ax_price.grid(True, alpha=0.3)
    ax_delta.grid(True, alpha=0.3)
    ax_price.legend(loc="upper left")
    ax_delta.legend(loc="upper left")

    stats_text = ax_price.text(
        0.01,
        0.01,
        "",
        transform=ax_price.transAxes,
        fontsize=10,
        va="bottom",
        ha="left",
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    closed = False

    def on_close(_event) -> None:
        nonlocal closed
        closed = True
        stop_flag.set()

    fig.canvas.mpl_connect("close_event", on_close)

    plt.show(block=False)

    while not closed:
        snapshot = comparator.snapshot()
        history_seconds = list(snapshot["history_seconds"])
        history_coinbase = list(snapshot["history_coinbase"])
        history_chainlink = list(snapshot["history_chainlink"])
        rows = list(snapshot["rows"])

        if history_seconds:
            start = history_seconds[0]
            x_values = [max((point - start).total_seconds(), 0.0) for point in history_seconds]
            _set_line_data(coinbase_line, x_values, history_coinbase)
            _set_line_data(chainlink_line, x_values, history_chainlink)

            deltas = [chainlink - coinbase for coinbase, chainlink in zip(history_coinbase, history_chainlink, strict=False)]
            _set_line_data(delta_line, x_values[: len(deltas)], deltas)

            ax_price.relim()
            ax_price.autoscale_view()
            ax_delta.relim()
            ax_delta.autoscale_view()

        stats_text.set_text(
            "\n".join(
                [
                    f"Samples: {snapshot['count']}",
                    f"Avg |Delta|: {snapshot['avg_abs_delta']:.2f}" if snapshot["avg_abs_delta"] is not None else "Avg |Delta|: -",
                    f"Avg |Delta| bps: {snapshot['avg_abs_delta_bps']:+.3f}" if snapshot["avg_abs_delta_bps"] is not None else "Avg |Delta| bps: -",
                    f"Avg source ts lag: {snapshot['avg_source_timestamp_delta_ms']:.1f} ms"
                    if snapshot["avg_source_timestamp_delta_ms"] is not None
                    else "Avg source ts lag: -",
                    f"Avg receive lag: {snapshot['avg_receive_delta_ms']:.1f} ms"
                    if snapshot["avg_receive_delta_ms"] is not None
                    else "Avg receive lag: -",
                    f"Latest price delta: {rows[-1].price_delta:+.2f}" if rows else "Latest price delta: -",
                    f"Latest ts lag: {rows[-1].source_timestamp_delta_ms:+d} ms" if rows else "Latest ts lag: -",
                ]
            )
        )

        fig.canvas.draw_idle()
        plt.pause(refresh_seconds)

    stop_flag.set()
    feed_thread.join(timeout=3.0)


def main() -> int:
    args = build_parser().parse_args()
    run_window(max_rows=args.max_rows, refresh_seconds=args.refresh_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
