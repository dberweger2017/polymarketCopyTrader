#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from polymarket_live import build_auth_client, fetch_live_account_state, load_env


def classify_position(position: dict[str, Any], market: dict[str, Any]) -> str:
    tokens = market.get("tokens") or []
    has_winner = any(bool(token.get("winner")) for token in tokens if isinstance(token, dict))
    redeemable = bool((position.get("raw") or {}).get("redeemable"))
    is_closed = bool(market.get("closed"))
    if has_winner or redeemable:
        return "resolved"
    if is_closed:
        return "ended"
    return "ongoing"


def summarize_positions(env_path: Path) -> dict[str, Any]:
    env = load_env(env_path)
    client = build_auth_client(env)
    state = fetch_live_account_state(client, env["POLYMARKET_PROXY_ADDRESS"])
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for position in state.get("positions", []):
        condition_id = str(position["condition_id"])
        market = client.get_market(condition_id)
        status = classify_position(position, market)
        counts[status] += 1
        winning_outcome = None
        for token in market.get("tokens") or []:
            if isinstance(token, dict) and token.get("winner"):
                winning_outcome = token.get("outcome")
                break
        rows.append(
            {
                "status": status,
                "title": position["title"],
                "slug": market.get("market_slug") or (position.get("raw") or {}).get("slug") or "",
                "token": position["label"],
                "shares": position["shares"],
                "redeemable": bool((position.get("raw") or {}).get("redeemable")),
                "closed": bool(market.get("closed")),
                "winning_outcome": winning_outcome,
                "condition_id": condition_id,
                "asset_id": position["asset_id"],
            }
        )

    rows.sort(key=lambda row: (row["status"], row["title"], row["token"]))
    return {
        "open_positions_count": int(state.get("open_positions_count") or 0),
        "counts": {
            "ongoing": counts.get("ongoing", 0),
            "ended": counts.get("ended", 0),
            "resolved": counts.get("resolved", 0),
        },
        "positions": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify open Polymarket positions as ongoing, ended, or resolved.")
    parser.add_argument("--env-file", default=".env", help="Path to the Polymarket credentials .env file.")
    parser.add_argument("--json", action="store_true", help="Print the full result as JSON.")
    args = parser.parse_args()

    summary = summarize_positions(Path(args.env_file))
    if args.json:
        print(json.dumps(summary, indent=2))
        return 0

    counts = summary["counts"]
    print(f"open_positions={summary['open_positions_count']}")
    print(
        "status_counts "
        f"ongoing={counts['ongoing']} ended={counts['ended']} resolved={counts['resolved']}"
    )
    for row in summary["positions"]:
        print(
            f"{row['status']}|{row['token']}|{row['shares']}|{row['title']}|"
            f"slug={row['slug']}|redeemable={row['redeemable']}|"
            f"closed={row['closed']}|winner={row['winning_outcome'] or ''}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
