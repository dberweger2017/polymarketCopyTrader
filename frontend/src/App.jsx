import { useEffect, useMemo, useState } from "react";

const POLL_MS = 3000;
const CHART_RANGES = [
  { key: "5m", label: "5m" },
  { key: "10m", label: "10m" },
  { key: "15m", label: "15m" },
  { key: "30m", label: "30m" },
  { key: "1h", label: "1h" },
  { key: "3h", label: "3h" },
  { key: "6h", label: "6h" },
  { key: "1d", label: "1d" },
  { key: "7d", label: "7d" },
  { key: "since_start", label: "Since Start" },
];

const currency = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function formatMoney(value) {
  return currency.format(value ?? 0);
}

function formatSignedMoney(value) {
  const amount = value ?? 0;
  const prefix = amount >= 0 ? "+" : "-";
  return `${prefix}${formatMoney(Math.abs(amount))}`;
}

function formatDecimal(value, digits = 4) {
  return Number(value ?? 0).toFixed(digits);
}

function formatPercent(value, digits = 2) {
  return `${(Number(value ?? 0) * 100).toFixed(digits)}%`;
}

function formatTime(isoString) {
  if (!isoString) return "-";
  return new Date(isoString).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatDateTime(isoString) {
  if (!isoString) return "-";
  return new Date(isoString).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatDuration(seconds) {
  const total = Math.max(Math.floor(seconds ?? 0), 0);
  const minutes = Math.floor(total / 60);
  const secs = total % 60;
  if (minutes >= 60) {
    const hours = Math.floor(minutes / 60);
    return `${hours}h ${String(minutes % 60).padStart(2, "0")}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${String(secs).padStart(2, "0")}s`;
  }
  return `${secs}s`;
}

function formatChartTimestamp(isoString, rangeKey) {
  if (!isoString) return "-";
  const date = new Date(isoString);
  const options =
    rangeKey === "1d" || rangeKey === "7d" || rangeKey === "since_start"
      ? { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }
      : { hour: "2-digit", minute: "2-digit" };
  return date.toLocaleString([], options);
}

function toneClass(value) {
  if (value > 0) return "positive";
  if (value < 0) return "negative";
  return "neutral";
}

function shortWallet(wallet) {
  if (!wallet || wallet.length <= 14) return wallet;
  return `${wallet.slice(0, 8)}...${wallet.slice(-6)}`;
}

function seriesLabel(wallet) {
  return wallet === "all" ? "All Traders" : shortWallet(wallet);
}

function rangeLabel(rangeKey) {
  return CHART_RANGES.find((range) => range.key === rangeKey)?.label ?? rangeKey;
}

function buildLinePoints(history, width, height, padding) {
  if (!history.length) return [];
  const min = Math.min(...history);
  const max = Math.max(...history);
  const span = Math.max(max - min, 0.000001);

  return history.map((value, index) => {
    const x = padding + (index / Math.max(history.length - 1, 1)) * (width - padding * 2);
    const y = height - padding - ((value - min) / span) * (height - padding * 2);
    return { x, y };
  });
}

function toSvgPoints(points) {
  return points.map((point) => `${point.x},${point.y}`).join(" ");
}

function StatCard({ label, value, tone = "neutral", breakdown = [], detail = "" }) {
  return (
    <div className={`stat-card ${breakdown.length ? "stat-card-has-tooltip" : ""}`}>
      {breakdown.length ? (
        <span className="stat-info-badge" aria-hidden="true" title="Hover for breakdown">
          i
        </span>
      ) : null}
      <span className="stat-label">{label}</span>
      <span className={`stat-value ${tone}`}>{value}</span>
      {detail ? <span className="stat-detail">{detail}</span> : null}
      {breakdown.length ? (
        <div className="stat-tooltip">
          <div className="stat-tooltip-title">{label} by Trader</div>
          {breakdown.map((item) => (
            <div key={item.wallet} className="stat-tooltip-row">
              <div className="stat-tooltip-wallet">
                <span className="stat-tooltip-dot" />
                <span>{shortWallet(item.wallet)}</span>
              </div>
              <span className={`stat-tooltip-value ${item.tone}`}>{item.value}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function LineChart({ title, subtitle, samples, formatter = formatMoney, tone = "teal" }) {
  const width = 900;
  const height = 220;
  const padding = 22;
  const history = samples.map((sample) => sample.value);
  const points = useMemo(() => buildLinePoints(history, width, height, padding), [history]);
  const min = history.length ? Math.min(...history) : 0;
  const max = history.length ? Math.max(...history) : 0;
  const first = history.length ? history[0] : 0;
  const last = history.length ? history.at(-1) : 0;
  const change = history.length > 1 ? last - first : 0;
  const linePoints = points.length ? toSvgPoints(points) : "";
  const areaPoints =
    points.length > 0
      ? `${padding},${height - padding} ${linePoints} ${width - padding},${height - padding}`
      : "";
  const lastPoint = points.length ? points[points.length - 1] : null;

  return (
    <section className="panel chart-panel">
      <div className="panel-header">
        <div className="chart-title-block">
          <h2>{title}</h2>
          <p>{subtitle}</p>
        </div>
        <div className="chart-meta">
          <span className="chart-stat-chip">Samples {history.length}</span>
          <span className="chart-stat-chip">
            Range {formatter(min)} to {formatter(max)}
          </span>
          <span className={`chart-stat-chip ${toneClass(change)}`}>Change {formatter(change)}</span>
        </div>
      </div>
      <div className="chart-shell">
        <div className="chart-axis">
          <span>{formatter(max)}</span>
          <span>{formatter((max + min) / 2)}</span>
          <span>{formatter(min)}</span>
        </div>
        <svg viewBox={`0 0 ${width} ${height}`} className="chart-svg" preserveAspectRatio="none">
          <defs>
            <linearGradient id={`lineFill-${tone}`} x1="0%" x2="0%" y1="0%" y2="100%">
              <stop offset="0%" stopColor={tone === "amber" ? "rgba(249, 115, 22, 0.34)" : "rgba(9, 122, 120, 0.34)"} />
              <stop offset="100%" stopColor="rgba(9, 122, 120, 0.02)" />
            </linearGradient>
            <linearGradient id={`lineStroke-${tone}`} x1="0%" x2="100%" y1="0%" y2="0%">
              <stop offset="0%" stopColor={tone === "amber" ? "#f97316" : "#0f766e"} />
              <stop offset="100%" stopColor={tone === "amber" ? "#fb7185" : "#f97316"} />
            </linearGradient>
          </defs>
          <rect x="0" y="0" width={width} height={height} rx="18" className="chart-bg" />
          {[0, 0.5, 1].map((marker) => (
            <line
              key={marker}
              x1={padding}
              y1={padding + marker * (height - padding * 2)}
              x2={width - padding}
              y2={padding + marker * (height - padding * 2)}
              className="chart-grid"
            />
          ))}
          {points.length ? (
            <>
              <polyline points={areaPoints} className="chart-area" fill={`url(#lineFill-${tone})`} />
              <polyline points={linePoints} className="chart-line" stroke={`url(#lineStroke-${tone})`} />
              {lastPoint ? <circle cx={lastPoint.x} cy={lastPoint.y} r="6" className="chart-point chart-point-end" /> : null}
            </>
          ) : null}
        </svg>
      </div>
      <div className="chart-footer">
        <span className="chart-footer-pill">Start {formatter(first)}</span>
        <span className="chart-footer-pill">Last {formatter(last)}</span>
        <span className="chart-footer-pill">
          From {samples.length ? formatDateTime(samples[0].timestamp) : "-"}
        </span>
        <span className="chart-footer-pill">
          To {samples.length ? formatDateTime(samples[samples.length - 1].timestamp) : "-"}
        </span>
      </div>
    </section>
  );
}

function EquityChart({
  chartData,
  chartError,
  chartLoading,
  selectedRange,
  selectedSeries,
  onSelectRange,
  onSelectSeries,
  walletPerformance,
}) {
  const samples = chartData?.samples ?? [];
  const selectedPerformance =
    selectedSeries === "all"
      ? null
      : walletPerformance.find((entry) => entry.wallet === selectedSeries) ?? null;
  const width = 900;
  const height = 220;
  const padding = 22;
  const history = samples.map((sample) => sample.equity);
  const points = useMemo(() => buildLinePoints(history, width, height, padding), [history]);
  const min = history.length ? Math.min(...history) : 0;
  const max = history.length ? Math.max(...history) : 0;
  const first = history.length ? history[0] : 0;
  const last = history.length ? history.at(-1) : 0;
  const change = history.length > 1 ? last - first : 0;
  const linePoints = points.length ? toSvgPoints(points) : "";
  const areaPoints =
    points.length > 0
      ? `${padding},${height - padding} ${linePoints} ${width - padding},${height - padding}`
      : "";
  const lastPoint = points.length ? points[points.length - 1] : null;

  return (
    <section className="panel chart-panel">
      <div className="panel-header">
        <div className="chart-title-block">
          <h2>Equity Curve</h2>
          <p>
            Rolling mark-to-market history for {seriesLabel(selectedSeries)} across{" "}
            {rangeLabel(selectedRange).toLowerCase()}
          </p>
        </div>
      </div>
      <div className="chart-toolbar">
        <div className="chart-controls">
          <label className="chart-select-label">
            <span>Profitability View</span>
            <select value={selectedSeries} onChange={(event) => onSelectSeries(event.target.value)}>
              <option value="all">All Traders</option>
              {walletPerformance.map((entry) => (
                <option key={entry.wallet} value={entry.wallet}>
                  {shortWallet(entry.wallet)}
                </option>
              ))}
            </select>
          </label>
          <div className="chart-range-group" aria-label="Chart time range">
            {CHART_RANGES.map((range) => (
              <button
                key={range.key}
                type="button"
                className={`chart-range-pill ${selectedRange === range.key ? "chart-range-pill-active" : ""}`}
                onClick={() => onSelectRange(range.key)}
              >
                {range.label}
              </button>
            ))}
          </div>
        </div>
        <div className="chart-toolbar-summary">
          <span className="chart-toolbar-pill">Series {seriesLabel(selectedSeries)}</span>
          <span className="chart-toolbar-pill">Window {rangeLabel(selectedRange)}</span>
          <span className="chart-toolbar-pill">Range {formatMoney(min)} to {formatMoney(max)}</span>
          <span className={`chart-toolbar-pill ${toneClass(change)}`}>Change {formatSignedMoney(change)}</span>
          {chartLoading ? <span className="chart-toolbar-pill neutral">Updating</span> : null}
          {chartError ? <span className="chart-toolbar-pill negative">Chart {chartError}</span> : null}
        </div>
      </div>
      <div className="chart-shell">
        <div className="chart-axis">
          <span>{formatMoney(max)}</span>
          <span>{formatMoney((max + min) / 2)}</span>
          <span>{formatMoney(min)}</span>
        </div>
        <svg viewBox={`0 0 ${width} ${height}`} className="chart-svg" preserveAspectRatio="none">
          <defs>
            <linearGradient id="copyLineFill" x1="0%" x2="0%" y1="0%" y2="100%">
              <stop offset="0%" stopColor="rgba(9, 122, 120, 0.34)" />
              <stop offset="100%" stopColor="rgba(9, 122, 120, 0.02)" />
            </linearGradient>
            <linearGradient id="copyLineStroke" x1="0%" x2="100%" y1="0%" y2="0%">
              <stop offset="0%" stopColor="#0f766e" />
              <stop offset="100%" stopColor="#f97316" />
            </linearGradient>
          </defs>
          <rect x="0" y="0" width={width} height={height} rx="18" className="chart-bg" />
          {[0, 0.5, 1].map((marker) => (
            <line
              key={marker}
              x1={padding}
              y1={padding + marker * (height - padding * 2)}
              x2={width - padding}
              y2={padding + marker * (height - padding * 2)}
              className="chart-grid"
            />
          ))}
          {points.length ? (
            <>
              <polyline points={areaPoints} className="chart-area" fill="url(#copyLineFill)" />
              <polyline points={linePoints} className="chart-line" stroke="url(#copyLineStroke)" />
              {lastPoint ? <circle cx={lastPoint.x} cy={lastPoint.y} r="6" className="chart-point chart-point-end" /> : null}
            </>
          ) : null}
        </svg>
      </div>
      <div className="chart-footer">
        <span className="chart-footer-pill">Start {formatMoney(first)}</span>
        <span className="chart-footer-pill">Last {formatMoney(last)}</span>
        <span className="chart-footer-pill">
          From {samples.length ? formatChartTimestamp(samples[0].timestamp, selectedRange) : "-"}
        </span>
        <span className="chart-footer-pill">
          To {samples.length ? formatChartTimestamp(samples[samples.length - 1].timestamp, selectedRange) : "-"}
        </span>
      </div>
      {selectedPerformance ? (
        <div className="subpanel-grid">
          <div className="subpanel-card">
            <span className="subpanel-label">Trader Cash</span>
            <strong>{formatMoney(selectedPerformance.cash)}</strong>
          </div>
          <div className="subpanel-card">
            <span className="subpanel-label">Trader Equity</span>
            <strong>{formatMoney(selectedPerformance.equity)}</strong>
          </div>
          <div className="subpanel-card">
            <span className="subpanel-label">Open Positions</span>
            <strong>{selectedPerformance.open_positions}</strong>
          </div>
          <div className="subpanel-card">
            <span className="subpanel-label">Unrealized</span>
            <strong className={toneClass(selectedPerformance.unrealized_pnl)}>
              {formatSignedMoney(selectedPerformance.unrealized_pnl)}
            </strong>
          </div>
        </div>
      ) : null}
    </section>
  );
}

function CopyPositionsTable({ positions }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2>Open Positions</h2>
          <p>Current paper inventory</p>
        </div>
        <span className="badge">{positions.length} open</span>
      </div>
      <div className="table-shell">
        <table>
          <thead>
            <tr>
              <th>Outcome</th>
              <th>Market</th>
              <th>Shares</th>
              <th>Avg</th>
              <th>Mark</th>
              <th>Value</th>
              <th>uPnL</th>
            </tr>
          </thead>
          <tbody>
            {positions.length ? (
              positions.map((position) => (
                <tr key={position.asset_id}>
                  <td>{position.outcome}</td>
                  <td>{position.title}</td>
                  <td>{formatDecimal(position.shares)}</td>
                  <td>{formatDecimal(position.avg_entry)}</td>
                  <td>{formatDecimal(position.mark)}</td>
                  <td>{formatMoney(position.value)}</td>
                  <td className={toneClass(position.unrealized_pnl)}>{formatSignedMoney(position.unrealized_pnl)}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan="7" className="empty-state">
                  No open positions yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ActivityFeed({ events, filters, subtitle }) {
  const [selectedFilter, setSelectedFilter] = useState(filters[0].key);
  const filteredEvents =
    selectedFilter === "all"
      ? events
      : events.filter((event) => (event.kind ?? "").toLowerCase() === selectedFilter);

  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2>Recent Activity</h2>
          <p>{subtitle}</p>
        </div>
        <div className="event-filter-row">
          {filters.map((filter) => (
            <button
              key={filter.key}
              type="button"
              className={`event-filter-pill ${selectedFilter === filter.key ? "event-filter-pill-active" : ""}`}
              onClick={() => setSelectedFilter(filter.key)}
            >
              {filter.label}
            </button>
          ))}
        </div>
      </div>
      <div className="event-list">
        {filteredEvents.length ? (
          filteredEvents.map((event, index) => (
            <article key={`${event.timestamp ?? "event"}-${index}`} className={`event event-${(event.kind ?? "status").toLowerCase()}`}>
              <div className="event-meta">
                <span>{event.timestamp ?? "-"}</span>
                <span>{event.kind ?? "STATUS"}</span>
              </div>
              <p>{event.message}</p>
            </article>
          ))
        ) : (
          <div className="empty-feed">No events for this filter yet.</div>
        )}
      </div>
    </section>
  );
}

function CopyBotDashboard({ data, chartData, chartError, chartLoading, selectedRange, selectedSeries, onSelectRange, onSelectSeries }) {
  if (!data) {
    return (
      <div className="loading-card">
        <span className="badge">Booting</span>
        <h1>Polymarket Copy Bot</h1>
        <p>Waiting for backend state...</p>
      </div>
    );
  }

  const { summary, config, positions, events, wallet_performance: walletPerformance } = data;
  const equityBreakdown = walletPerformance.map((entry) => ({
    wallet: entry.wallet,
    value: formatMoney(entry.equity),
    tone: "neutral",
  }));
  const realizedBreakdown = walletPerformance.map((entry) => ({
    wallet: entry.wallet,
    value: formatSignedMoney(entry.realized_pnl),
    tone: toneClass(entry.realized_pnl),
  }));

  return (
    <>
      <div className="hero">
        <div className="hero-copy">
          <span className="kicker">Paper Trading Control Room</span>
          <h1>Polymarket Copy Bot</h1>
          <p>
            Following {config.tracked_wallets.length} wallets with {formatMoney(config.copy_notional_usd)} per copied
            trade and slippage capped at {formatMoney(config.max_slippage)} or{" "}
            {formatPercent(config.relative_slippage_rate)} of copied price.
          </p>
        </div>
        <div className="hero-status">
          <span className={`status-pill status-${summary.status.toLowerCase()}`}>{summary.status}</span>
          <span className="status-detail">Last poll {formatTime(summary.last_poll_at)}</span>
          <span className="status-detail">Last trade {formatTime(summary.last_trade_at)}</span>
        </div>
      </div>

      <section className="wallet-strip panel">
        <div className="wallet-strip-header">
          <h2>Tracked Wallets</h2>
          <span className="badge">{summary.seeded_count} seeded trades ignored</span>
        </div>
        <div className="wallet-pill-row">
          {config.tracked_wallets.map((wallet) => (
            <span key={wallet} className="wallet-pill">
              {shortWallet(wallet)}
            </span>
          ))}
        </div>
        {config.public_url ? (
          <div className="public-link-card">
            <span className="public-link-label">Phone Link</span>
            <a className="public-link-anchor" href={config.public_url} target="_blank" rel="noreferrer">
              {config.public_url}
            </a>
          </div>
        ) : null}
        <p className="latest-line">Latest: {summary.latest_action}</p>
        {summary.last_error ? <p className="error-line">{summary.last_error}</p> : null}
      </section>

      <section className="stats-grid">
        <StatCard label="Cash" value={formatMoney(summary.cash)} />
        <StatCard label="Equity" value={formatMoney(summary.equity)} tone="positive" breakdown={equityBreakdown} />
        <StatCard label="Realized" value={formatSignedMoney(summary.realized_pnl)} tone={toneClass(summary.realized_pnl)} breakdown={realizedBreakdown} />
        <StatCard label="Unrealized" value={formatSignedMoney(summary.unrealized_pnl)} tone={toneClass(summary.unrealized_pnl)} />
        <StatCard label="Open Positions" value={String(summary.open_positions)} />
        <StatCard label="Heartbeat" value={`${config.heartbeat_seconds.toFixed(0)}s`} detail={`Polling every ${config.poll_interval_seconds.toFixed(1)}s`} />
      </section>

      <EquityChart
        chartData={chartData}
        chartError={chartError}
        chartLoading={chartLoading}
        selectedRange={selectedRange}
        selectedSeries={selectedSeries}
        onSelectRange={onSelectRange}
        onSelectSeries={onSelectSeries}
        walletPerformance={walletPerformance}
      />
      <CopyPositionsTable positions={positions} />
      <ActivityFeed
        events={events}
        filters={[
          { key: "all", label: "All" },
          { key: "copied", label: "Copied" },
          { key: "skipped", label: "Skipped" },
          { key: "status", label: "Status" },
          { key: "error", label: "Error" },
        ]}
        subtitle="Latest copy decisions and heartbeats"
      />
    </>
  );
}

function OptionsPositionsTable({ positions }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2>Live Inventory</h2>
          <p>Liquidation-aware marks for each open option leg.</p>
        </div>
        <span className="badge">{positions.length} open</span>
      </div>
      <div className="table-shell">
        <table>
          <thead>
            <tr>
              <th>Token</th>
              <th>Shares</th>
              <th>Avg Cost</th>
              <th>Bid</th>
              <th>Ask</th>
              <th>Exit Px</th>
              <th>Exit Value</th>
              <th>uPnL</th>
            </tr>
          </thead>
          <tbody>
            {positions.length ? (
              positions.map((position) => (
                <tr key={position.asset_id}>
                  <td>{position.label}</td>
                  <td>{formatDecimal(position.shares)}</td>
                  <td>{formatDecimal(position.avg_cost)}</td>
                  <td>{position.best_bid == null ? "-" : formatDecimal(position.best_bid)}</td>
                  <td>{position.best_ask == null ? "-" : formatDecimal(position.best_ask)}</td>
                  <td>{formatDecimal(position.liquidation_price)}</td>
                  <td>{formatMoney(position.liquidation_value)}</td>
                  <td className={toneClass(position.unrealized_pnl)}>{formatSignedMoney(position.unrealized_pnl)}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan="8" className="empty-state">
                  No open positions in the active market.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function OptionsBooksTable({ books }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2>Book vs Fair</h2>
          <p>Bid-side exit math and live alpha for the current market.</p>
        </div>
        <span className="badge">{books.length} outcomes</span>
      </div>
      <div className="table-shell">
        <table>
          <thead>
            <tr>
              <th>Token</th>
              <th>Bid</th>
              <th>Ask</th>
              <th>Mid</th>
              <th>Fair</th>
              <th>Buy Alpha</th>
              <th>Sell Alpha</th>
              <th>Shares</th>
            </tr>
          </thead>
          <tbody>
            {books.map((book) => (
              <tr key={book.asset_id}>
                <td>{book.label}</td>
                <td>{book.best_bid == null ? "-" : formatDecimal(book.best_bid)}</td>
                <td>{book.best_ask == null ? "-" : formatDecimal(book.best_ask)}</td>
                <td>{book.mid == null ? "-" : formatDecimal(book.mid)}</td>
                <td>{book.fair_value == null ? "-" : formatDecimal(book.fair_value)}</td>
                <td className={toneClass(book.buy_alpha_net)}>{book.buy_alpha_net == null ? "-" : formatDecimal(book.buy_alpha_net)}</td>
                <td className={toneClass(book.sell_alpha_net)}>{book.sell_alpha_net == null ? "-" : formatDecimal(book.sell_alpha_net)}</td>
                <td>{formatDecimal(book.shares)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SessionList({ sessions }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2>Recent Sessions</h2>
          <p>Latest saved 5-minute markets discovered from snapshot files.</p>
        </div>
        <span className="badge">{sessions.length} tracked</span>
      </div>
      <div className="event-list">
        {sessions.length ? (
          sessions.map((session) => (
            <article key={session.session_file} className="event event-status">
              <div className="event-meta">
                <span>{formatDateTime(session.updated_at_utc)}</span>
                <span>{session.status}</span>
              </div>
              <p>
                <strong>{session.title}</strong> · {session.asset} · balance {formatMoney(session.balance)} · trades{" "}
                {session.trade_count}
              </p>
            </article>
          ))
        ) : (
          <div className="empty-feed">No completed sessions yet.</div>
        )}
      </div>
    </section>
  );
}

function OptionsDashboard({ state, error }) {
  if (!state) {
    return (
      <div className="loading-card">
        <span className="badge">Booting</span>
        <h1>Polymarket 5m Options</h1>
        <p>{error || "Waiting for snapshot state..."}</p>
      </div>
    );
  }

  if (!state.available || !state.current) {
    return (
      <div className="loading-card">
        <span className="badge">Snapshots Needed</span>
        <h1>Polymarket 5m Options</h1>
        <p>{state.message || error || "Run the collector with --write-csv to populate this dashboard."}</p>
        <p className="loading-subtle">Watching {state.data_dir}</p>
      </div>
    );
  }

  const { current, recent_sessions: recentSessions } = state;
  const { market, summary, positions, books, events, charts } = current;
  const eventFeed = events.map((message, index) => ({
    timestamp: formatTime(current.updated_at_utc),
    kind: index === 0 ? "STATUS" : "NOTE",
    message,
  }));

  return (
    <>
      <div className="hero">
        <div className="hero-copy">
          <span className="kicker">Recurring 5-Minute Markets</span>
          <h1>Polymarket Options Session</h1>
          <p>
            Live monitor for the current {market.asset} Up/Down market. Balance now reflects cash plus estimated exit
            proceeds at the bid side, so it stays consistent throughout the session.
          </p>
        </div>
        <div className="hero-status">
          <span className={`status-pill status-${current.status.toLowerCase()}`}>{current.status}</span>
          <span className="status-detail">Window closes {formatDateTime(market.window_end_utc)}</span>
          <span className="status-detail">Updated {formatTime(current.updated_at_utc)}</span>
        </div>
      </div>

      <section className="wallet-strip panel">
        <div className="wallet-strip-header">
          <div>
            <h2>{market.title}</h2>
            <p className="latest-line">
              {market.asset.toUpperCase()} · {market.slug}
            </p>
          </div>
          <span className="badge">{formatDuration(summary.time_left_seconds)} left</span>
        </div>
        <div className="wallet-pill-row">
          {market.outcome_labels.map((label) => (
            <span key={label} className="wallet-pill">
              {label}
            </span>
          ))}
        </div>
        <p className="latest-line">Snapshot file: {current.session_file}</p>
        <p className="latest-line">Data dir: {state.data_dir}</p>
      </section>

      <section className="stats-grid">
        <StatCard label="Balance" value={formatMoney(summary.balance)} tone="positive" detail="cash + sell-now value" />
        <StatCard label="Cash" value={formatMoney(summary.cash)} detail="uninvested balance" />
        <StatCard label="Open Value" value={formatMoney(summary.liquidation_value)} detail="bid-side liquidation estimate" />
        <StatCard label="Realized" value={formatSignedMoney(summary.realized_pnl)} tone={toneClass(summary.realized_pnl)} />
        <StatCard label="Unrealized" value={formatSignedMoney(summary.unrealized_pnl)} tone={toneClass(summary.unrealized_pnl)} />
        <StatCard label="Exposure" value={formatMoney(summary.open_cost_basis)} detail={`cap ${formatMoney(summary.max_market_exposure_usd)}`} />
        <StatCard label="Remaining Cap" value={formatMoney(summary.remaining_buy_budget)} />
        <StatCard label="Trades" value={String(summary.trade_count)} />
        <StatCard label="Fair Up" value={summary.fair_up == null ? "-" : formatPercent(summary.fair_up, 1)} />
        <StatCard label="Underlying" value={summary.latest_underlying_price == null ? "-" : formatMoney(summary.latest_underlying_price)} detail={summary.start_underlying_price == null ? "start ref pending" : `start ${formatMoney(summary.start_underlying_price)}`} />
        <StatCard label="Entry Threshold" value={formatDecimal(Math.max(summary.min_alpha_net ?? 0, 0.2), 3)} detail={`trade size ${formatDecimal(summary.paper_trade_shares, 2)} sh`} />
        <StatCard label="Max Position" value={formatDecimal(summary.max_position_shares, 2)} detail={summary.initial_trade_taken ? "first entry used" : "first entry armed"} />
      </section>

      <LineChart
        title="Liquidation Balance"
        subtitle="Cash plus estimated bid-side exit proceeds for all open inventory."
        samples={charts.balance}
      />
      <div className="split-panel-grid">
        <LineChart
          title="Fair Up Probability"
          subtitle="Model probability of the Up outcome over the live session."
          samples={charts.fair_up}
          formatter={(value) => `${(value * 100).toFixed(1)}%`}
          tone="amber"
        />
        <LineChart
          title="Net Ask Alpha"
          subtitle="Fair minus ask minus taker fee for the Up token."
          samples={charts.up_alpha}
          formatter={(value) => `${value >= 0 ? "+" : ""}${Number(value ?? 0).toFixed(3)}`}
          tone="amber"
        />
      </div>

      <OptionsPositionsTable positions={positions} />
      <OptionsBooksTable books={books} />
      <div className="split-panel-grid">
        <ActivityFeed
          events={eventFeed}
          filters={[
            { key: "all", label: "All" },
            { key: "status", label: "Status" },
            { key: "note", label: "Notes" },
          ]}
          subtitle="Latest paper-trader notes from the active session"
        />
        <SessionList sessions={recentSessions} />
      </div>
    </>
  );
}

export default function App() {
  const [mode, setMode] = useState("options");
  const [copyData, setCopyData] = useState(null);
  const [copyError, setCopyError] = useState("");
  const [optionsData, setOptionsData] = useState(null);
  const [optionsError, setOptionsError] = useState("");
  const [selectedSeries, setSelectedSeries] = useState("all");
  const [selectedRange, setSelectedRange] = useState("since_start");
  const [chartData, setChartData] = useState({ samples: [] });
  const [chartError, setChartError] = useState("");
  const [chartLoading, setChartLoading] = useState(false);

  useEffect(() => {
    let active = true;

    async function loadCopyState() {
      try {
        const response = await fetch("/api/state");
        if (!response.ok) throw new Error(`API returned ${response.status}`);
        const payload = await response.json();
        if (active) {
          setCopyData(payload);
          setCopyError("");
        }
      } catch (err) {
        if (active) {
          setCopyError(err instanceof Error ? err.message : "Unknown error");
        }
      }
    }

    async function loadOptionsState() {
      try {
        const response = await fetch("/api/options/state");
        if (!response.ok) throw new Error(`API returned ${response.status}`);
        const payload = await response.json();
        if (active) {
          setOptionsData(payload);
          setOptionsError("");
        }
      } catch (err) {
        if (active) {
          setOptionsError(err instanceof Error ? err.message : "Unknown error");
        }
      }
    }

    loadCopyState();
    loadOptionsState();
    const timer = setInterval(() => {
      loadCopyState();
      loadOptionsState();
    }, POLL_MS);

    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  const walletPerformance = copyData?.wallet_performance ?? [];
  const availableSeries = new Set(["all", ...walletPerformance.map((entry) => entry.wallet)]);
  const activeSeries = availableSeries.has(selectedSeries) ? selectedSeries : "all";

  useEffect(() => {
    if (!copyData) return undefined;

    let active = true;

    async function loadChart(showLoading) {
      if (showLoading && active) setChartLoading(true);
      try {
        const params = new URLSearchParams({ series: activeSeries, window: selectedRange });
        const response = await fetch(`/api/chart?${params.toString()}`);
        if (!response.ok) throw new Error(`API returned ${response.status}`);
        const payload = await response.json();
        if (active) {
          setChartData(payload);
          setChartError("");
        }
      } catch (err) {
        if (active) {
          setChartError(err instanceof Error ? err.message : "Unknown error");
        }
      } finally {
        if (active) setChartLoading(false);
      }
    }

    loadChart(true);
    const timer = setInterval(() => loadChart(false), POLL_MS);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [activeSeries, copyData, selectedRange]);

  return (
    <main className="app-shell">
      <section className="mode-switch panel">
        <div>
          <span className="kicker">Control Room</span>
          <h2>Choose A Desk</h2>
          <p className="latest-line">Use the same dev server for the copy bot and the 5-minute options session monitor.</p>
        </div>
        <div className="mode-switch-row">
          <button
            type="button"
            className={`mode-pill ${mode === "options" ? "mode-pill-active" : ""}`}
            onClick={() => setMode("options")}
          >
            5m Options
          </button>
          <button
            type="button"
            className={`mode-pill ${mode === "copy" ? "mode-pill-active" : ""}`}
            onClick={() => setMode("copy")}
          >
            Copy Bot
          </button>
        </div>
      </section>

      {mode === "copy" ? (
        <CopyBotDashboard
          data={copyData}
          error={copyError}
          chartData={chartData}
          chartError={chartError}
          chartLoading={chartLoading}
          selectedRange={selectedRange}
          selectedSeries={activeSeries}
          onSelectRange={setSelectedRange}
          onSelectSeries={setSelectedSeries}
        />
      ) : (
        <OptionsDashboard state={optionsData} error={optionsError} />
      )}
    </main>
  );
}
