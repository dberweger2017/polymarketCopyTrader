from __future__ import annotations

import shutil
import signal
import sys
import time
from datetime import datetime
from decimal import Decimal
from typing import Any

from bot_core import (
    BotConfig,
    PolymarketCopyBot,
    format_decimal,
    format_money,
    format_percent,
    format_signed_money,
    sample_series,
    short_wallet,
    truncate_text,
)


class TerminalUI:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    def __init__(self) -> None:
        self.interactive = sys.stdout.isatty()
        self._seen_event_ids: set[str] = set()

    def color(self, text: str, tone: str) -> str:
        if not self.interactive:
            return text
        palette = {
            "green": self.GREEN,
            "red": self.RED,
            "yellow": self.YELLOW,
            "cyan": self.CYAN,
            "dim": self.DIM,
            "bold": self.BOLD,
            "white": self.WHITE,
        }
        prefix = palette.get(tone, "")
        return f"{prefix}{text}{self.RESET}" if prefix else text

    def refresh(self, snapshot: dict[str, Any]) -> None:
        if self.interactive:
            sys.stdout.write("\033[2J\033[H")
            sys.stdout.write(self.render(snapshot))
            sys.stdout.write("\n")
            sys.stdout.flush()
            return
        self.print_new_events(snapshot)

    def render(self, snapshot: dict[str, Any]) -> str:
        width = max(100, min(shutil.get_terminal_size((120, 40)).columns, 160))
        lines: list[str] = []
        summary = snapshot["summary"]
        config = snapshot["config"]

        top_bar = (
            f"{self.color('POLYMARKET COPY BOT', 'bold')}  "
            f"{self.color('[PAPER]', 'yellow')}  "
            f"{self.color(f'[{summary['status']}]', 'cyan')}"
        )
        lines.append(top_bar)
        lines.append(self.color("-" * width, "dim"))
        lines.extend(self.render_summary(snapshot, width))
        lines.append(self.color("-" * width, "dim"))
        lines.append(self.color("Equity Curve", "bold"))
        lines.extend(self.render_equity_chart(snapshot["equity_history"], width))
        lines.append(self.color("-" * width, "dim"))
        lines.append(self.color("Open Positions", "bold"))
        lines.extend(self.render_positions(snapshot["positions"], width))
        lines.append(self.color("-" * width, "dim"))
        lines.append(self.color("Recent Activity", "bold"))
        lines.extend(self.render_events(snapshot["events"], width))
        return "\n".join(lines)

    def render_summary(self, snapshot: dict[str, Any], width: int) -> list[str]:
        summary = snapshot["summary"]
        config = snapshot["config"]
        values = [
            ("Cash", format_money(Decimal(str(summary["cash"]))), "white"),
            ("Equity", format_money(Decimal(str(summary["equity"]))), "cyan"),
            ("Realized", format_signed_money(Decimal(str(summary["realized_pnl"]))), tone_for_value(summary["realized_pnl"])),
            (
                "Unrealized",
                format_signed_money(Decimal(str(summary["unrealized_pnl"]))),
                tone_for_value(summary["unrealized_pnl"]),
            ),
            ("Open Pos", str(summary["open_positions"]), "white"),
            ("Last Poll", format_time(summary["last_poll_at"]), "dim"),
            ("Last Trade", format_time(summary["last_trade_at"]), "dim"),
        ]
        summary_line = "  ".join(
            f"{self.color(f'{label}:', 'dim')} {self.color(value, tone)}" for label, value, tone in values
        )
        wallet_line = (
            f"{self.color('Wallets:', 'dim')} {len(config['tracked_wallets'])} tracked  "
            f"{self.color('Seeded:', 'dim')} {summary['seeded_count']} old trades ignored  "
            f"{self.color('Copy Size:', 'dim')} {format_money(Decimal(str(config['copy_notional_usd'])))}  "
            f"{self.color('Slippage:', 'dim')} min("
            f"{format_decimal(Decimal(str(config['max_slippage'])))}, "
            f"{format_percent(Decimal(str(config['relative_slippage_rate'])))} of price)  "
            f"{self.color('Poll:', 'dim')} {config['poll_interval_seconds']:.1f}s"
        )
        tracked_line = (
            f"{self.color('Tracked:', 'dim')} "
            f"{truncate_text(', '.join(short_wallet(wallet) for wallet in config['tracked_wallets']), width - 10)}"
        )
        action_line = f"{self.color('Latest:', 'dim')} {truncate_text(summary['latest_action'], width - 9)}"
        lines = [summary_line, wallet_line, tracked_line, action_line]
        if summary["last_error"]:
            lines.append(
                f"{self.color('Last Error:', 'dim')} "
                f"{self.color(truncate_text(summary['last_error'], width - 13), 'red')}"
            )
        return lines

    def render_equity_chart(self, history: list[float], width: int) -> list[str]:
        if not history:
            return [self.color("No equity history yet.", "dim")]

        points = sample_series([Decimal(str(value)) for value in history], max(24, min(width - 14, 96)))
        low = min(points)
        high = max(points)
        span = high - low
        trend = points[-1] - points[0]
        tone = tone_for_value(float(trend))
        height = 6

        if span == 0:
            rows = [height // 2 for _ in points]
        else:
            rows = []
            for point in points:
                normalized = (point - low) / span
                row = height - 1 - int(normalized * (height - 1))
                rows.append(max(0, min(height - 1, row)))

        grid = [[" " for _ in range(len(points))] for _ in range(height)]
        for column, row in enumerate(rows):
            grid[row][column] = "●"

        rendered: list[str] = []
        for index, row in enumerate(grid):
            label = ""
            if index == 0:
                label = format_money(high)
            elif index == height // 2:
                label = format_money(low + (span / Decimal("2")))
            elif index == height - 1:
                label = format_money(low)
            rendered.append(
                f"{self.color(label.rjust(9), 'dim')} {self.color('│', 'dim')} {self.color(''.join(row), tone)}"
            )

        rendered.append(
            f"{self.color('samples:', 'dim')} {len(history)}  "
            f"{self.color('range:', 'dim')} {format_money(low)} to {format_money(high)}  "
            f"{self.color('last:', 'dim')} {self.color(format_money(points[-1]), 'cyan')}  "
            f"{self.color('change:', 'dim')} {self.color(format_signed_money(trend), tone)}"
        )
        return rendered

    def render_positions(self, positions: list[dict[str, Any]], width: int) -> list[str]:
        if not positions:
            return [self.color("No open positions yet.", "dim")]

        columns = [18, max(24, width - 74), 12, 8, 8, 12, 12]
        lines = [
            self.color(format_row(["Outcome", "Market", "Shares", "Avg", "Mark", "Value", "uPnL"], columns), "bold"),
            self.color("-" * min(width, sum(columns) + len(columns) - 1), "dim"),
        ]
        for position in positions[:10]:
            lines.append(
                format_row(
                    [
                        truncate_text(position["outcome"], 18),
                        truncate_text(position["title"], columns[1]),
                        format_decimal(Decimal(str(position["shares"])), "0.0000"),
                        format_decimal(Decimal(str(position["avg_entry"]))),
                        format_decimal(Decimal(str(position["mark"]))),
                        format_money(Decimal(str(position["value"]))),
                        format_signed_money(Decimal(str(position["unrealized_pnl"]))),
                    ],
                    columns,
                )
            )
        if len(positions) > 10:
            lines.append(self.color(f"... {len(positions) - 10} more positions", "dim"))
        return lines

    def render_events(self, events: list[dict[str, Any]], width: int) -> list[str]:
        if not events:
            return [self.color("No activity yet.", "dim")]
        lines = []
        for event in events[:12]:
            tone = {
                "COPIED": "green",
                "SKIPPED": "yellow",
                "STATUS": "cyan",
                "ERROR": "red",
            }.get(event["kind"], "white")
            label = self.color(f"{event['timestamp']} {event['kind']:<7}", tone)
            lines.append(f"{label} {truncate_text(event['message'], width - 18)}")
        return lines

    def print_new_events(self, snapshot: dict[str, Any]) -> None:
        for event in reversed(snapshot["events"]):
            event_id = f"{event['timestamp']}|{event['kind']}|{event['message']}"
            if event_id in self._seen_event_ids:
                continue
            self._seen_event_ids.add(event_id)
            print(f"[{event['timestamp']}] {event['kind']:<8} {event['message']}", flush=True)


def tone_for_value(value: float) -> str:
    if value > 0:
        return "green"
    if value < 0:
        return "red"
    return "white"


def format_time(value: str | None) -> str:
    if not value:
        return "-"
    return datetime.fromisoformat(value).strftime("%H:%M:%S")


def format_row(values: list[str], widths: list[int]) -> str:
    return " ".join(value[:width].ljust(width) for value, width in zip(values, widths))


keep_running = True


def handle_shutdown(signum: int, _frame: Any) -> None:
    del signum
    global keep_running
    keep_running = False


def main() -> int:
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    bot = PolymarketCopyBot(BotConfig.from_env())
    ui = TerminalUI()
    bot.start()

    try:
        while keep_running and bot.is_running():
            ui.refresh(bot.snapshot())
            time.sleep(1)
    finally:
        bot.stop()
        ui.refresh(bot.snapshot())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
