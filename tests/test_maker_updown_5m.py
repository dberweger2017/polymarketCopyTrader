from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import maker_updown_5m


class TargetBidTests(unittest.TestCase):
    def test_target_bid_hits_requested_alpha(self) -> None:
        price = maker_updown_5m.solve_target_bid_price(
            fair_value=0.5,
            target_alpha=0.2,
            tick_size=0.0001,
        )
        self.assertIsNotNone(price)
        alpha = maker_updown_5m.alpha_for_buy(fair_value=0.5, price=price)
        self.assertIsNotNone(alpha)
        self.assertAlmostEqual(alpha, 0.2, places=3)

    def test_target_bid_returns_none_when_alpha_is_impossible(self) -> None:
        price = maker_updown_5m.solve_target_bid_price(
            fair_value=0.05,
            target_alpha=0.2,
            tick_size=0.01,
        )
        self.assertIsNone(price)

    def test_pair_all_in_matches_one_minus_two_alpha_when_fair_is_balanced(self) -> None:
        price = maker_updown_5m.solve_target_bid_price(
            fair_value=0.5,
            target_alpha=0.2,
            tick_size=0.0001,
        )
        self.assertIsNotNone(price)
        total = 2 * (price + maker_updown_5m.approx_fee_per_share(price))
        self.assertAlmostEqual(total, 0.6, places=3)


class SideExposureTests(unittest.TestCase):
    def test_both_sides_can_quote_when_balanced(self) -> None:
        self.assertTrue(
            maker_updown_5m.next_fill_within_side_exposure(
                side_shares=0.0,
                opposite_side_shares=0.0,
                order_size_shares=5.0,
                max_side_exposure_shares=5.0,
            )
        )

    def test_filled_side_stops_quoting_until_other_side_catches_up(self) -> None:
        self.assertFalse(
            maker_updown_5m.next_fill_within_side_exposure(
                side_shares=5.0,
                opposite_side_shares=0.0,
                order_size_shares=5.0,
                max_side_exposure_shares=5.0,
            )
        )
        self.assertTrue(
            maker_updown_5m.next_fill_within_side_exposure(
                side_shares=0.0,
                opposite_side_shares=5.0,
                order_size_shares=5.0,
                max_side_exposure_shares=5.0,
            )
        )

    def test_both_sides_can_quote_again_after_match(self) -> None:
        self.assertTrue(
            maker_updown_5m.next_fill_within_side_exposure(
                side_shares=5.0,
                opposite_side_shares=5.0,
                order_size_shares=5.0,
                max_side_exposure_shares=5.0,
            )
        )


class FairValueTests(unittest.TestCase):
    def test_normal_cdf_midpoint(self) -> None:
        self.assertAlmostEqual(maker_updown_5m.normal_cdf(0.0), 0.5, places=8)


class MarketDiscoveryTests(unittest.TestCase):
    def test_started_market_is_allowed_after_startup(self) -> None:
        now = datetime(2026, 4, 18, 20, 5, 0, tzinfo=timezone.utc)
        started_market = {
            "id": "m1",
            "enableOrderBook": True,
            "endDate": "2026-04-18T20:10:00Z",
            "eventStartTime": "2026-04-18T20:00:00Z",
            "clobTokenIds": '["1","2"]',
            "outcomes": '["Up","Down"]',
            "conditionId": "cond-1",
            "slug": "eth-updown-5m-1",
            "question": "Started market",
        }
        future_market = {
            "id": "m2",
            "enableOrderBook": True,
            "endDate": "2026-04-18T20:15:00Z",
            "eventStartTime": "2026-04-18T20:10:00Z",
            "clobTokenIds": '["3","4"]',
            "outcomes": '["Up","Down"]',
            "conditionId": "cond-2",
            "slug": "eth-updown-5m-2",
            "question": "Future market",
        }
        events = [{
            "id": "e1",
            "title": "Started market",
            "tags": [{"slug": "up-or-down"}, {"slug": "5M"}, {"slug": "recurring"}],
            "markets": [started_market, future_market],
        }]

        original_http_json = maker_updown_5m.http_json
        original_utc_now = maker_updown_5m.utc_now
        try:
            maker_updown_5m.http_json = lambda *args, **kwargs: events
            maker_updown_5m.utc_now = lambda: now
            market = maker_updown_5m.discover_next_market("ethereum", set(), require_future_start=False)
        finally:
            maker_updown_5m.http_json = original_http_json
            maker_updown_5m.utc_now = original_utc_now

        self.assertIsNotNone(market)
        self.assertEqual(market.market_id, "m1")

    def test_bitcoin_uses_bitcoin_tag_slug(self) -> None:
        captured = {}

        def fake_http_json(path, *, params=None, timeout=20):
            captured["params"] = params
            return []

        original_http_json = maker_updown_5m.http_json
        try:
            maker_updown_5m.http_json = fake_http_json
            maker_updown_5m.discover_next_market("bitcoin", set(), require_future_start=True)
        finally:
            maker_updown_5m.http_json = original_http_json

        self.assertEqual(captured["params"]["tag_slug"], "bitcoin")

    def test_find_market_start_quote_uses_first_quote_at_or_after_start(self) -> None:
        market = maker_updown_5m.MarketWindow(
            asset="bitcoin",
            event_id="e1",
            market_id="m1",
            condition_id="c1",
            title="t",
            slug="s",
            token_ids=("up", "down"),
            outcome_labels=("Up", "Down"),
            window_start=datetime(2026, 4, 18, 20, 0, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 4, 18, 20, 5, 0, tzinfo=timezone.utc),
        )
        quotes = [
            maker_updown_5m.ChainlinkQuote(timestamp=market.window_start - timedelta(seconds=1), price=100.0),
            maker_updown_5m.ChainlinkQuote(timestamp=market.window_start, price=101.0),
            maker_updown_5m.ChainlinkQuote(timestamp=market.window_start + timedelta(seconds=1), price=102.0),
        ]
        original_fetch = maker_updown_5m.fetch_chainlink_quotes
        try:
            maker_updown_5m.fetch_chainlink_quotes = lambda *_args, **_kwargs: quotes
            quote = maker_updown_5m.find_market_start_quote(asset="bitcoin", market=market)
        finally:
            maker_updown_5m.fetch_chainlink_quotes = original_fetch

        self.assertIsNotNone(quote)
        self.assertEqual(quote.price, 101.0)

    def test_select_boundary_quote_prefers_last_before_when_no_exact_match(self) -> None:
        boundary = datetime(2026, 4, 18, 20, 0, 0, tzinfo=timezone.utc)
        quotes = [
            maker_updown_5m.ChainlinkQuote(timestamp=boundary - timedelta(seconds=2), price=99.0),
            maker_updown_5m.ChainlinkQuote(timestamp=boundary - timedelta(milliseconds=500), price=100.0),
            maker_updown_5m.ChainlinkQuote(timestamp=boundary + timedelta(milliseconds=400), price=101.0),
        ]
        quote = maker_updown_5m.select_boundary_quote(quotes, boundary)
        self.assertIsNotNone(quote)
        self.assertEqual(quote.price, 100.0)


class WarmupTests(unittest.TestCase):
    def test_trading_active_after_warmup_only(self) -> None:
        market = maker_updown_5m.MarketWindow(
            asset="ethereum",
            event_id="e1",
            market_id="m1",
            condition_id="c1",
            title="t",
            slug="s",
            token_ids=("up", "down"),
            outcome_labels=("Up", "Down"),
            window_start=datetime(2026, 4, 18, 20, 0, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 4, 18, 20, 5, 0, tzinfo=timezone.utc),
        )
        quoter = maker_updown_5m.MakerQuoter(
            market=market,
            client=object(),
            proxy_address="0x0",
            target_alpha=0.3,
            alpha_band_half_width=0.05,
            order_size_shares=5.0,
            max_market_committed_usd=10.0,
            max_side_exposure_shares=5.0,
            no_trade_first_seconds=30.0,
            no_trade_last_seconds=20.0,
        )
        self.assertFalse(quoter.trading_active(market.window_start + timedelta(seconds=29)))
        self.assertTrue(quoter.trading_active(market.window_start + timedelta(seconds=30)))
        self.assertTrue(quoter.trading_active(market.window_end - timedelta(seconds=20)))
        self.assertFalse(quoter.trading_active(market.window_end - timedelta(seconds=19)))

    def test_late_live_tick_does_not_seed_start_price(self) -> None:
        market = maker_updown_5m.MarketWindow(
            asset="ethereum",
            event_id="e1",
            market_id="m1",
            condition_id="c1",
            title="t",
            slug="s",
            token_ids=("up", "down"),
            outcome_labels=("Up", "Down"),
            window_start=datetime(2026, 4, 18, 20, 0, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 4, 18, 20, 5, 0, tzinfo=timezone.utc),
        )
        quoter = maker_updown_5m.MakerQuoter(
            market=market,
            client=object(),
            proxy_address="0x0",
            target_alpha=0.3,
            alpha_band_half_width=0.05,
            order_size_shares=5.0,
            max_market_committed_usd=10.0,
            max_side_exposure_shares=5.0,
            no_trade_first_seconds=30.0,
            no_trade_last_seconds=20.0,
        )
        quoter.on_underlying_price(
            timestamp=market.window_start + timedelta(seconds=5),
            price=2500.0,
        )
        self.assertEqual(quoter.start_underlying_price, 2500.0)
        self.assertIn("RTDS first >= open", quoter.start_underlying_price_source)

    def test_buffered_ticks_seed_from_last_pre_open_tick(self) -> None:
        market = maker_updown_5m.MarketWindow(
            asset="ethereum",
            event_id="e1",
            market_id="m1",
            condition_id="c1",
            title="t",
            slug="s",
            token_ids=("up", "down"),
            outcome_labels=("Up", "Down"),
            window_start=datetime(2026, 4, 18, 20, 0, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 4, 18, 20, 5, 0, tzinfo=timezone.utc),
        )
        quoter = maker_updown_5m.MakerQuoter(
            market=market,
            client=object(),
            proxy_address="0x0",
            target_alpha=0.3,
            alpha_band_half_width=0.05,
            order_size_shares=5.0,
            max_market_committed_usd=10.0,
            max_side_exposure_shares=5.0,
            no_trade_first_seconds=30.0,
            no_trade_last_seconds=20.0,
        )
        quoter.on_underlying_price(
            timestamp=market.window_start - timedelta(milliseconds=400),
            price=2499.0,
        )
        self.assertIsNone(quoter.start_underlying_price)
        quoter.on_underlying_price(
            timestamp=market.window_start + timedelta(milliseconds=600),
            price=2501.0,
        )
        self.assertEqual(quoter.start_underlying_price, 2499.0)
        self.assertIn("RTDS last <= open", quoter.start_underlying_price_source)

    def test_websocket_updates_trigger_sync_event(self) -> None:
        market = maker_updown_5m.MarketWindow(
            asset="ethereum",
            event_id="e1",
            market_id="m1",
            condition_id="c1",
            title="t",
            slug="s",
            token_ids=("up", "down"),
            outcome_labels=("Up", "Down"),
            window_start=datetime(2026, 4, 18, 20, 0, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 4, 18, 20, 5, 0, tzinfo=timezone.utc),
        )
        quoter = maker_updown_5m.MakerQuoter(
            market=market,
            client=object(),
            proxy_address="0x0",
            target_alpha=0.3,
            alpha_band_half_width=0.05,
            order_size_shares=5.0,
            max_market_committed_usd=10.0,
            max_side_exposure_shares=5.0,
            no_trade_first_seconds=30.0,
            no_trade_last_seconds=20.0,
        )
        sync_event = maker_updown_5m.asyncio.Event()
        quoter.attach_sync_event(sync_event)
        quoter.on_underlying_price(
            timestamp=datetime(2026, 4, 18, 20, 0, 1, tzinfo=timezone.utc),
            price=2500.0,
        )
        self.assertTrue(sync_event.is_set())


if __name__ == "__main__":
    unittest.main()
