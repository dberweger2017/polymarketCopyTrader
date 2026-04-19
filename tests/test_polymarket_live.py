from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import polymarket_live


class LiveSizingTests(unittest.TestCase):
    def test_calculate_live_buy_shares(self) -> None:
        shares = polymarket_live.calculate_live_buy_shares(Decimal("1.50"), Decimal("0.75"))
        self.assertEqual(shares, Decimal("2"))

    def test_calculate_live_sell_shares_caps_to_available(self) -> None:
        shares = polymarket_live.calculate_live_sell_shares(
            Decimal("1.00"),
            Decimal("0.20"),
            Decimal("3.5"),
        )
        self.assertEqual(shares, Decimal("3.5"))


class LiveAccountStateTests(unittest.TestCase):
    def test_build_live_account_state_normalizes_values(self) -> None:
        state = polymarket_live.build_live_account_state(
            cash_balance_micro_usdc="50000000",
            open_orders=[
                {
                    "side": "BUY",
                    "original_size": "10",
                    "size_matched": "4",
                    "price": "0.25",
                }
            ],
            positions=[
                {
                    "asset": "token-a",
                    "conditionId": "cond-a",
                    "title": "Example Market",
                    "outcome": "Up",
                    "size": "2.5",
                    "avgPrice": "0.40",
                    "initialValue": "1.00",
                    "currentValue": "1.50",
                    "curPrice": "0.60",
                }
            ],
            marked_position_value_usdc=Decimal("1.50"),
            executable_sell_prices={"token-a": Decimal("0.55")},
        )

        self.assertEqual(state["cash_balance_usdc"], "50")
        self.assertEqual(state["open_order_reserved_cash_usdc"], "1.5")
        self.assertEqual(state["free_cash_usdc"], "48.5")
        self.assertEqual(state["marked_position_value_usdc"], "1.5")
        self.assertEqual(state["executable_position_value_usdc"], "1.375")
        self.assertEqual(state["marked_total_account_value_usdc"], "51.5")
        self.assertEqual(state["executable_total_account_value_usdc"], "51.375")
        self.assertEqual(state["positions_by_asset"]["token-a"]["shares"], "2.5")


class TradeStatusFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_for_trade_status_falls_back_to_polling(self) -> None:
        class FakeStream:
            async def wait_for(self, predicate, timeout):  # noqa: ANN001
                raise TimeoutError("missed websocket event")

        class FakeClient:
            def __init__(self) -> None:
                self.calls = 0

            def get_trades(self, params):  # noqa: ANN001
                self.calls += 1
                return [
                    {
                        "taker_order_id": "order-1",
                        "status": "MATCHED",
                        "price": "0.42",
                        "size": "2",
                    }
                ]

        client = FakeClient()
        with mock.patch.object(polymarket_live, "TradeParams", lambda market: {"market": market}):
            event = await polymarket_live.wait_for_trade_status(
                stream=FakeStream(),
                client=client,
                condition_id="cond-1",
                order_id="order-1",
                status="MATCHED",
                timeout=0.6,
            )

        self.assertEqual(event["source"], "poll")
        self.assertEqual(event["status"], "MATCHED")
        self.assertGreaterEqual(client.calls, 1)


if __name__ == "__main__":
    unittest.main()
