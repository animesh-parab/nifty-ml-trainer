import { useState, useEffect } from "react"
import axios from "axios"
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, BarChart, Bar, Cell
} from "recharts"
import { TrendingUp, TrendingDown, Minus, Activity, Brain, BarChart2, Clock } from "lucide-react"

const API = "http://127.0.0.1:8000"

const SIGNAL_COLORS = {
  UP:       "#22c55e",
  DOWN:     "#ef4444",
  SIDEWAYS: "#f59e0b"
}

function SignalBadge({ signal, confidence }) {
  const icon = signal === "UP" ? "↑" : signal === "DOWN" ? "↓" : "→"
  return (
    <span style={{
      display:      "inline-flex",
      alignItems:   "center",
      gap:          "6px",
      padding:      "6px 14px",
      borderRadius: "6px",
      background:   SIGNAL_COLORS[signal] + "22",
      border:       `1px solid ${SIGNAL_COLORS[signal]}`,
      color:        SIGNAL_COLORS[signal],
      fontWeight:   "700",
      fontSize:     "13px",
    }}>
      {icon} {signal} {confidence}%
    </span>
  )
}

function Card({ title, children, style }) {
  return (
    <div style={{
      background:   "#1e2130",
      borderRadius: "12px",
      padding:      "20px",
      border:       "1px solid #2d3148",
      ...style
    }}>
      {title && (
        <div style={{
          fontSize:      "11px",
          color:         "#6b7280",
          fontWeight:    "700",
          marginBottom:  "16px",
          textTransform: "uppercase",
          letterSpacing: "0.08em"
        }}>
          {title}
        </div>
      )}
      {children}
    </div>
  )
}

function MetricRow({ label, value, color }) {
  return (
    <div style={{
      display:        "flex",
      justifyContent: "space-between",
      alignItems:     "center",
      padding:        "8px 0",
      borderBottom:   "1px solid #2d3148"
    }}>
      <span style={{ color: "#9ca3af", fontSize: "13px" }}>{label}</span>
      <span style={{ color: color || "#f9fafb", fontWeight: "600", fontSize: "13px" }}>{value}</span>
    </div>
  )
}

function TradePanel({ signal, context }) {
  if (!signal || !context) return null
  const s5  = signal.signals[5]
  const s15 = signal.signals[15]
  const s30 = signal.signals[30]

  const combined  = +(s5.confidence * 0.40 + s15.confidence * 0.35 + s30.confidence * 0.25).toFixed(1)
  const upVotes   = [s5, s15, s30].filter(s => s.signal === "UP").length
  const downVotes = [s5, s15, s30].filter(s => s.signal === "DOWN").length
  const direction = s5.signal !== "SIDEWAYS"
    ? (upVotes > downVotes ? "UP" : "DOWN")
    : null

  const actionable = direction !== null && combined >= 60

  if (!actionable) {
    const readyFrames = [
      { label: "5-min",  ...s5  },
      { label: "15-min", ...s15 },
      { label: "30-min", ...s30 },
    ].filter(s => s.confidence >= 60 && s.signal !== "SIDEWAYS")

    return (
      <Card title="Trade Signal">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "14px" }}>
          <span style={{ color: "#f59e0b", fontWeight: "700", fontSize: "12px" }}>⏸ WAITING</span>
          <span style={{ color: combined >= 60 ? "#22c55e" : "#6b7280", fontSize: "12px", fontWeight: "600" }}>
            {combined}% combined
          </span>
        </div>
        {readyFrames.length > 0 && readyFrames.map(f => (
          <MetricRow key={f.label} label={f.label}
            value={`${f.signal} (${f.confidence}%)`}
            color={SIGNAL_COLORS[f.signal]} />
        ))}
        {readyFrames.length === 0 && (
          <div style={{ color: "#6b7280", fontSize: "12px", marginBottom: "8px" }}>
            No timeframe >= 60% yet
          </div>
        )}
        <div style={{ marginTop: "14px", fontSize: "11px", color: "#6b7280", lineHeight: "1.6" }}>
          Needs: 5-min UP/DOWN · Combined >= 60%
        </div>
      </Card>
    )
  }

  const isLong    = direction === "UP"
  const color     = isLong ? "#22c55e" : "#ef4444"
  const alignment = upVotes === 3 ? "Triple ✓" : upVotes === 2 || downVotes === 2 ? "Double ✓" : "Single"
  const exits     = signal.exits
  const entry     = context.price
  const atr       = context.atr_14

  return (
    <Card title="Trade Signal" style={{ border: `1px solid ${color}` }}>
      <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "16px" }}>
        <span style={{
          background: color, color: "#000",
          fontWeight: "800", fontSize: "14px",
          padding: "6px 16px", borderRadius: "6px"
        }}>
          {isLong ? "▲ BUY" : "▼ SELL"}
        </span>
        <span style={{
          background: color + "22", border: `1px solid ${color}`,
          color: color, fontWeight: "700", fontSize: "12px",
          padding: "4px 10px", borderRadius: "6px"
        }}>
          {combined}% · {alignment}
        </span>
      </div>

      <MetricRow label="Entry" value={`₹${entry?.toLocaleString()}`} />

      {exits ? (
        <>
          <MetricRow label="Stoploss" value={`₹${exits.stoploss} (-${exits.sl_pts} pts)`} color="#ef4444" />
          <div style={{ fontSize: "10px", color: "#6b7280", textAlign: "right", marginTop: "-4px", marginBottom: "6px" }}>{exits.sl_reason}</div>
          <MetricRow label="T1 — 50% exit" value={`₹${exits.t1} (+${exits.t1_pts} pts)  R/R ${exits.rr_t1}`} color="#22c55e" />
          <div style={{ fontSize: "10px", color: "#6b7280", textAlign: "right", marginTop: "-4px", marginBottom: "6px" }}>{exits.t1_reason}</div>
          <MetricRow label="T2 — 30% exit" value={`₹${exits.t2} (+${exits.t2_pts} pts)  R/R ${exits.rr_t2}`} color="#22c55e" />
          <div style={{ fontSize: "10px", color: "#6b7280", textAlign: "right", marginTop: "-4px", marginBottom: "6px" }}>{exits.t2_reason}</div>
          <MetricRow label="T3 — 20% exit" value={`₹${exits.t3} (+${exits.t3_pts} pts)  R/R ${exits.rr_t3}`} color="#22c55e" />
          <div style={{ fontSize: "10px", color: "#6b7280", textAlign: "right", marginTop: "-4px", marginBottom: "6px" }}>{exits.t3_reason}</div>
        </>
      ) : (
        (() => {
          const target   = isLong ? +(entry + atr * 1.5).toFixed(2) : +(entry - atr * 1.5).toFixed(2)
          const stoploss = isLong ? +(entry - atr * 0.8).toFixed(2) : +(entry + atr * 0.8).toFixed(2)
          const tPts     = Math.abs(target - entry).toFixed(1)
          const slPts    = Math.abs(stoploss - entry).toFixed(1)
          return (
            <>
              <MetricRow label="Target"   value={`₹${target} (+${tPts} pts)`}   color="#22c55e" />
              <MetricRow label="Stoploss" value={`₹${stoploss} (-${slPts} pts)`} color="#ef4444" />
              <MetricRow label="R/R"      value={(tPts / slPts).toFixed(2)}       color="#6366f1" />
            </>
          )
        })()
      )}

      <MetricRow label="ATR (14)" value={atr?.toFixed(2)} />
      <div style={{ fontSize: "11px", color: "#6b7280", margin: "12px 0 6px" }}>TIMEFRAME BREAKDOWN</div>
      {[{ label: "5-min", ...s5 }, { label: "15-min", ...s15 }, { label: "30-min", ...s30 }].map(f => (
        <MetricRow key={f.label} label={f.label}
          value={f.confidence >= 60 ? `${f.signal} (${f.confidence}%)` : `${f.signal} — weak`}
          color={f.confidence >= 60 ? SIGNAL_COLORS[f.signal] : "#6b7280"} />
      ))}
      <div style={{ marginTop: "12px", fontSize: "11px", color: "#6b7280" }}>
        ⚠️ Educational only. Not financial advice.
      </div>
    </Card>
  )
}

function TradeTracker() {
  const [trades, setTrades]       = useState([])
  const [summary, setSummary]     = useState({})
  const [currentPrice, setPrice]  = useState(0)
  const [activeTab, setActiveTab] = useState("active")
  const [loading, setLoading]     = useState(true)

  const fetchTrades = async () => {
    try {
      const res = await axios.get(`${API}/trades/live`)
      setTrades(res.data.trades || [])
      setSummary(res.data.summary || {})
      setPrice(res.data.current_price || 0)
      setLoading(false)
    } catch (err) {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchTrades()
    const interval = setInterval(fetchTrades, 30000)
    return () => clearInterval(interval)
  }, [])

  const statusColor = (status) => {
    if (!status) return "#6b7280"
    if (status.includes("T3"))  return "#6366f1"
    if (status.includes("T2"))  return "#22c55e"
    if (status.includes("T1"))  return "#86efac"
    if (status === "ACTIVE")    return "#f59e0b"
    if (status.includes("SL") || status === "LOSS") return "#ef4444"
    if (status.includes("WIN")) return "#22c55e"
    if (status === "EXPIRED")   return "#6b7280"
    return "#6b7280"
  }

  const statusLabel = (status) => {
    if (!status) return "—"
    if (status === "ACTIVE")    return "🟡 ACTIVE"
    if (status === "T1_HIT")    return "🟢 T1 HIT"
    if (status === "T2_HIT")    return "🟢 T2 HIT"
    if (status === "T3_HIT")    return "💜 T3 HIT"
    if (status === "SL_HIT")    return "🔴 SL HIT"
    if (status.includes("WIN")) return "✅ WIN"
    if (status.includes("LOSS")) return "❌ LOSS"
    if (status === "EXPIRED")   return "⏰ EXPIRED"
    return status
  }

  const filteredTrades = [...trades].filter(t => {
    if (activeTab === "active") return t.outcome === "PENDING"
    if (activeTab === "today")  return t.is_today
    return true
  }).reverse()

  const tabs = [
    { id: "active", label: `Active (${summary.pending || 0})` },
    { id: "today",  label: `Today (${summary.today || 0})` },
    { id: "all",    label: `All (${summary.total || 0})` },
  ]

  return (
    <Card title="Trade Tracker" style={{ gridColumn: "1 / -1" }}>
      <div style={{ display: "flex", gap: "24px", marginBottom: "16px", flexWrap: "wrap", alignItems: "center" }}>
        <span style={{ fontSize: "12px", color: "#9ca3af" }}>
          Current: <strong style={{ color: "#f9fafb" }}>₹{currentPrice?.toLocaleString()}</strong>
        </span>
        <span style={{ fontSize: "12px", color: "#22c55e" }}>Wins: <strong>{summary.wins || 0}</strong></span>
        <span style={{ fontSize: "12px", color: "#ef4444" }}>Losses: <strong>{summary.losses || 0}</strong></span>
        <span style={{ fontSize: "12px", color: "#6366f1" }}>Win Rate: <strong>{summary.win_rate || 0}%</strong></span>
        <span style={{ fontSize: "11px", color: "#6b7280", marginLeft: "auto" }}>⏱ Updates every 30s</span>
      </div>

      <div style={{ display: "flex", gap: "8px", marginBottom: "16px" }}>
        {tabs.map(t => (
          <button key={t.id} onClick={() => setActiveTab(t.id)} style={{
            padding: "5px 14px", borderRadius: "6px", border: "none",
            cursor: "pointer", fontSize: "12px", fontWeight: "600",
            background: activeTab === t.id ? "#6366f1" : "#2d3148",
            color:      activeTab === t.id ? "#fff"    : "#9ca3af",
          }}>
            {t.label}
          </button>
        ))}
      </div>

      {loading ? (
        <div style={{ color: "#6b7280", padding: "20px", textAlign: "center" }}>Loading trades...</div>
      ) : filteredTrades.length === 0 ? (
        <div style={{ color: "#6b7280", padding: "20px", textAlign: "center" }}>No trades in this view</div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px" }}>
            <thead>
              <tr style={{ color: "#6b7280" }}>
                <th style={{ textAlign: "left",   padding: "8px" }}>Time (IST)</th>
                <th style={{ textAlign: "center", padding: "8px" }}>Signal</th>
                <th style={{ textAlign: "right",  padding: "8px" }}>Entry</th>
                <th style={{ textAlign: "right",  padding: "8px" }}>SL</th>
                <th style={{ textAlign: "right",  padding: "8px" }}>T1</th>
                <th style={{ textAlign: "right",  padding: "8px" }}>T2</th>
                <th style={{ textAlign: "right",  padding: "8px" }}>T3</th>
                <th style={{ textAlign: "right",  padding: "8px" }}>Conf%</th>
                <th style={{ textAlign: "center", padding: "8px" }}>Status</th>
              </tr>
            </thead>
            <tbody>
              {filteredTrades.map((trade, i) => {
                const ts  = new Date(trade.timestamp)
const ist = ts
                const status = trade.live_status || trade.outcome
                return (
                  <tr key={i} style={{
                    borderBottom: "1px solid #2d3148",
                    background:   i % 2 === 0 ? "#161824" : "transparent"
                  }}>
                    <td style={{ padding: "8px", color: "#9ca3af" }}>
                      {ist.toLocaleString("en-IN", {
                        month: "short", day: "numeric",
                        hour: "2-digit", minute: "2-digit"
                      })}
                    </td>
                    <td style={{ textAlign: "center", padding: "8px" }}>
                      <span style={{ color: SIGNAL_COLORS[trade.signal], fontWeight: "700" }}>
                        {trade.signal === "UP" ? "↑" : "↓"} {trade.signal}
                      </span>
                    </td>
                    <td style={{ textAlign: "right", padding: "8px" }}>
                      ₹{parseFloat(trade.entry_price).toFixed(1)}
                    </td>
                    <td style={{ textAlign: "right", padding: "8px", color: "#ef4444" }}>
                      {trade.stoploss ? parseFloat(trade.stoploss).toFixed(1) : "—"}
                    </td>
                    <td style={{ textAlign: "right", padding: "8px", color: "#86efac" }}>
                      {trade.t1 ? parseFloat(trade.t1).toFixed(1) : "—"}
                    </td>
                    <td style={{ textAlign: "right", padding: "8px", color: "#22c55e" }}>
                      {trade.t2 ? parseFloat(trade.t2).toFixed(1) : "—"}
                    </td>
                    <td style={{ textAlign: "right", padding: "8px", color: "#6366f1" }}>
                      {trade.t3 ? parseFloat(trade.t3).toFixed(1) : "—"}
                    </td>
                    <td style={{ textAlign: "right", padding: "8px" }}>
                      {trade.confidence}%
                    </td>
                    <td style={{ textAlign: "center", padding: "8px" }}>
                      <span style={{ color: statusColor(status), fontWeight: "700", fontSize: "11px" }}>
                        {statusLabel(status)}
                      </span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  )
}

export default function App() {
  const [tab,       setTab]       = useState("live")
  const [signal,    setSignal]    = useState(null)
  const [context,   setContext]   = useState(null)
  const [history,   setHistory]   = useState([])
  const [analysis,  setAnalysis]  = useState(null)
  const [backtest,  setBacktest]  = useState(null)
  const [loading,   setLoading]   = useState(true)
  const [aiLoading, setAiLoading] = useState(false)
  const [lastUpdate,setLastUpdate]= useState(null)
  const [error,     setError]     = useState(null)

  const fetchLiveData = async () => {
    try {
      setError(null)
      const [sigRes, ctxRes, histRes] = await Promise.all([
        axios.get(`${API}/signal/latest`),
        axios.get(`${API}/market/context`),
        axios.get(`${API}/signal/history`),
      ])
      setSignal(sigRes.data)
      setContext(ctxRes.data)
      setHistory([...histRes.data.history].reverse())
      setLastUpdate(new Date().toLocaleTimeString("en-IN"))
      setLoading(false)
    } catch (err) {
      setError("API connection failed. Is uvicorn running?")
      setLoading(false)
    }
  }

  const fetchAiAnalysis = async () => {
    setAiLoading(true)
    try {
      const res = await axios.get(`${API}/ai/analysis`)
      setAnalysis(res.data)
    } catch (err) {
      setAnalysis({ analysis: "AI analysis failed. Check Groq API key." })
    }
    setAiLoading(false)
  }

  const fetchBacktest = async () => {
    try {
      const res = await axios.get(`${API}/backtest/results`)
      setBacktest(res.data)
    } catch (err) {
      console.error("Backtest fetch failed:", err)
    }
  }

  useEffect(() => {
    fetchLiveData()
    fetchBacktest()
    const interval = setInterval(fetchLiveData, 60000)
    return () => clearInterval(interval)
  }, [])

  const vixLabel = (regime) => {
    if (regime === 0) return { label: "Low",    color: "#22c55e" }
    if (regime === 1) return { label: "Medium", color: "#f59e0b" }
    return                    { label: "High",  color: "#ef4444" }
  }

  return (
    <div style={{
      minHeight:  "100vh",
      background: "#0f1117",
      color:      "#f9fafb",
      fontFamily: "'Inter', sans-serif",
      padding:    "24px 32px",
      boxSizing:  "border-box",
      width:      "100%",
      maxWidth:   "100%",
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "24px" }}>
        <div>
          <h1 style={{ fontSize: "22px", fontWeight: "700", margin: 0 }}>🇮🇳 Nifty ML Trainer</h1>
          <p style={{ color: "#6b7280", fontSize: "12px", margin: "4px 0 0" }}>
            LightGBM · Multi-timeframe · Groq AI Analysis
          </p>
        </div>
        <div style={{ display: "flex", gap: "10px", alignItems: "center" }}>
          {lastUpdate && <span style={{ color: "#6b7280", fontSize: "12px" }}>⏱ {lastUpdate}</span>}
          <button onClick={fetchLiveData} style={{
            background: "#2d3148", border: "none", borderRadius: "8px",
            color: "#f9fafb", padding: "8px 16px", cursor: "pointer", fontSize: "13px"
          }}>↻ Refresh</button>
        </div>
      </div>

      <div style={{ display: "flex", gap: "8px", marginBottom: "24px" }}>
        {[
          { id: "live",     label: "Live Signals"    },
          { id: "backtest", label: "Backtest Report" },
          { id: "ai",       label: "AI Analysis"     },
        ].map(t => (
          <button key={t.id} onClick={() => { setTab(t.id); if (t.id === "ai" && !analysis) fetchAiAnalysis() }} style={{
            padding: "8px 20px", borderRadius: "8px", border: "none", cursor: "pointer",
            fontSize: "13px", fontWeight: "600",
            background: tab === t.id ? "#6366f1" : "#1e2130",
            color:      tab === t.id ? "#fff"    : "#9ca3af",
          }}>
            {t.label}
          </button>
        ))}
      </div>

      {error && (
        <div style={{
          background: "#ef444422", border: "1px solid #ef4444",
          borderRadius: "8px", padding: "12px 16px",
          color: "#ef4444", marginBottom: "16px", fontSize: "13px"
        }}>⚠ {error}</div>
      )}

      {loading ? (
        <div style={{ textAlign: "center", padding: "80px", color: "#6b7280" }}>Loading...</div>
      ) : (
        <>
          {tab === "live" && (
            <div style={{
              display: "grid", gridTemplateColumns: "1fr 1fr 1fr",
              gap: "16px", width: "100%", boxSizing: "border-box"
            }}>
              <Card title="Current Signals">
                <div style={{ display: "flex", flexDirection: "column", gap: "14px" }}>
                  {signal && Object.entries(signal.signals).map(([w, s]) => (
                    <div key={w} style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <span style={{ color: "#9ca3af", fontSize: "13px" }}>{w}-min</span>
                      <SignalBadge signal={s.signal} confidence={s.confidence} />
                    </div>
                  ))}
                </div>
              </Card>

              <Card title="Market Context">
                {context && <>
                  <MetricRow label="Price"    value={`₹${context.price?.toLocaleString()}`} />
                  <MetricRow label="RSI (14)" value={context.rsi_14}
                    color={context.rsi_14 > 70 ? "#ef4444" : context.rsi_14 < 30 ? "#22c55e" : "#f9fafb"} />
                  <MetricRow label="ATR (14)" value={context.atr_14} />
                  <MetricRow label="EMA 9"    value={context.ema_9?.toLocaleString()} />
                  <MetricRow label="EMA 21"   value={context.ema_21?.toLocaleString()} />
                  <MetricRow label="VIX"
                    value={`${context.vix} (${vixLabel(context.vix_regime).label})`}
                    color={vixLabel(context.vix_regime).color} />
                </>}
              </Card>

              <TradePanel signal={signal} context={context} />

              

              <TradeTracker />
            </div>
          )}

          {tab === "backtest" && (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px", width: "100%" }}>
              {!backtest ? (
                <div style={{ gridColumn: "1/-1", textAlign: "center", padding: "60px", color: "#6b7280" }}>Loading backtest results...</div>
              ) : <>
                <Card title="Backtest Summary (Jul 2025 – Jan 2026 · Unseen Data)">
                  <MetricRow label="Total Trades"  value={backtest.summary.total_trades} />
                  <MetricRow label="Win Rate"       value={`${backtest.summary.win_rate}%`}        color="#22c55e" />
                  <MetricRow label="Total P&L"      value={`${backtest.summary.total_pnl_pts} pts`} color={backtest.summary.total_pnl_pts > 0 ? "#22c55e" : "#ef4444"} />
                  <MetricRow label="Total P&L (₹)"  value={`₹${Number(backtest.summary.total_pnl_inr).toLocaleString()}`} color={backtest.summary.total_pnl_inr > 0 ? "#22c55e" : "#ef4444"} />
                  <MetricRow label="Avg Win"         value={`${backtest.summary.avg_win_pts} pts`}  color="#22c55e" />
                  <MetricRow label="Avg Loss"        value={`${backtest.summary.avg_loss_pts} pts`} color="#ef4444" />
                  <MetricRow label="Risk/Reward"     value={backtest.summary.risk_reward} />
                  <MetricRow label="Sharpe Ratio"    value={backtest.summary.sharpe_ratio} />
                  <MetricRow label="Max Drawdown"    value={`${backtest.summary.max_drawdown_pts} pts`} color="#ef4444" />
                </Card>
                <Card title="Monthly P&L (pts)">
                  <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={Object.entries(backtest.monthly_pnl).map(([k, v]) => ({ month: k, pnl: v }))}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                      <XAxis dataKey="month" tick={{ fill: "#6b7280", fontSize: 10 }} tickFormatter={v => v.slice(5)} />
                      <YAxis tick={{ fill: "#6b7280", fontSize: 10 }} />
                      <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2d3148" }} labelStyle={{ color: "#f9fafb" }} />
                      <Bar dataKey="pnl" radius={[4, 4, 0, 0]}>
                        {Object.entries(backtest.monthly_pnl).map(([k, v], i) => (<Cell key={i} fill={v > 0 ? "#22c55e" : "#ef4444"} />))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </Card>
                <Card title="Cumulative P&L" style={{ gridColumn: "1 / -1" }}>
                  <ResponsiveContainer width="100%" height={240}>
                    <LineChart data={backtest.cumulative_series.filter((_, i) => i % 5 === 0)}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                      <XAxis dataKey="timestamp" tick={{ fill: "#6b7280", fontSize: 10 }} tickFormatter={v => new Date(v).toLocaleDateString("en-IN", { month: "short", year: "2-digit" })} />
                      <YAxis tick={{ fill: "#6b7280", fontSize: 10 }} />
                      <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2d3148" }} formatter={v => [`${Number(v).toFixed(1)} pts`, "Cumulative P&L"]} />
                      <Line type="monotone" dataKey="cumulative_pnl" stroke="#6366f1" dot={false} strokeWidth={2} />
                    </LineChart>
                  </ResponsiveContainer>
                </Card>
                <Card title="Drawdown" style={{ gridColumn: "1 / -1" }}>
                  <ResponsiveContainer width="100%" height={150}>
                    <LineChart data={backtest.drawdown_series.filter((_, i) => i % 5 === 0)}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                      <XAxis dataKey="timestamp" tick={{ fill: "#6b7280", fontSize: 10 }} tickFormatter={v => new Date(v).toLocaleDateString("en-IN", { month: "short", year: "2-digit" })} />
                      <YAxis tick={{ fill: "#6b7280", fontSize: 10 }} />
                      <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2d3148" }} formatter={v => [`${Number(v).toFixed(1)} pts`, "Drawdown"]} />
                      <Line type="monotone" dataKey="drawdown" stroke="#ef4444" dot={false} strokeWidth={1.5} />
                    </LineChart>
                  </ResponsiveContainer>
                </Card>
              </>}
            </div>
          )}

          {tab === "ai" && (
            <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: "16px", width: "100%" }}>
              <Card title="AI Analysis · Groq Llama 3.3 70B" style={{ gridColumn: "1 / -1" }}>
                {aiLoading ? (
                  <div style={{ color: "#6b7280", textAlign: "center", padding: "40px" }}>Analysing market conditions...</div>
                ) : analysis ? (
                  <div>
                    <div style={{ background: "#161824", borderRadius: "8px", padding: "16px", lineHeight: "1.8", color: "#e5e7eb", fontSize: "14px", marginBottom: "16px" }}>
                      {analysis.analysis}
                    </div>
                    <button onClick={fetchAiAnalysis} style={{ background: "#6366f1", border: "none", borderRadius: "8px", color: "#fff", padding: "8px 20px", cursor: "pointer", fontSize: "13px" }}>
                      ↻ Refresh Analysis
                    </button>
                  </div>
                ) : null}
              </Card>
              {signal && (
                <Card title="Current Signals">
                  {Object.entries(signal.signals).map(([w, s]) => (
                    <div key={w} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "12px" }}>
                      <span style={{ color: "#9ca3af" }}>{w}-min</span>
                      <SignalBadge signal={s.signal} confidence={s.confidence} />
                    </div>
                  ))}
                </Card>
              )}
              {context && (
                <Card title="Market Context">
                  <MetricRow label="Price" value={`₹${context.price?.toLocaleString()}`} />
                  <MetricRow label="RSI"   value={context.rsi_14} />
                  <MetricRow label="ATR"   value={context.atr_14} />
                  <MetricRow label="VIX"   value={context.vix} />
                </Card>
              )}
            </div>
          )}
        </>
      )}
    </div>
  )
}
