from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_snapshot(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    payload["session_file"] = str(path)
    return payload


def load_options_dashboard(base_dir: Path, *, limit: int = 10) -> dict[str, Any]:
    data_dir = base_dir / "data" / "market_recordings"
    snapshot_files = sorted(
        data_dir.glob("*/*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not snapshot_files:
        return {
            "available": False,
            "message": "No options session snapshots found yet. Run collect_updown_5m.py with --write-csv first.",
            "current": None,
            "recent_sessions": [],
            "data_dir": str(data_dir),
        }

    current: dict[str, Any] | None = None
    recent_sessions: list[dict[str, Any]] = []

    for path in snapshot_files:
        snapshot = _load_snapshot(path)
        if snapshot is None:
            continue
        if current is None:
            current = snapshot

        market = snapshot.get("market", {})
        summary = snapshot.get("summary", {})
        recent_sessions.append(
            {
                "session_file": str(path),
                "updated_at_utc": snapshot.get("updated_at_utc"),
                "status": snapshot.get("status", "unknown"),
                "asset": market.get("asset"),
                "title": market.get("title"),
                "slug": market.get("slug"),
                "window_start_utc": market.get("window_start_utc"),
                "window_end_utc": market.get("window_end_utc"),
                "balance": summary.get("balance"),
                "cash": summary.get("cash"),
                "realized_pnl": summary.get("realized_pnl"),
                "unrealized_pnl": summary.get("unrealized_pnl"),
                "trade_count": summary.get("trade_count"),
            }
        )
        if len(recent_sessions) >= limit:
            break

    if current is None:
        return {
            "available": False,
            "message": "Found snapshot files, but none could be read successfully.",
            "current": None,
            "recent_sessions": [],
            "data_dir": str(data_dir),
        }

    return {
        "available": True,
        "message": "",
        "current": current,
        "recent_sessions": recent_sessions,
        "data_dir": str(data_dir),
    }
