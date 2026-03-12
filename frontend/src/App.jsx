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
  const s5 = signal.signals[5]
  if (!s5 || s5.signal === "SIDEWAYS" || s5.confidence < 60) {
    return (
      <Card title="Trade Signal">
        <div style={{ color: "#6b7280", textAlign: "center", padding: "20px", fontSize: "13px" }}>
          No actionable signal — waiting for high confidence directional move
        </div>
      </Card>
    )
  }
  const isLong    = s5.signal === "UP"
  const entry     = context.price
  const atr       = context.atr_14
  const target    = isLong ? +(entry + atr * 1.5).toFixed(2) : +(entry - atr * 1.5).toFixed(2)
  const stoploss  = isLong ? +(entry - atr * 0.8).toFixed(2) : +(entry + atr * 0.8).toFixed(2)
  const targetPts = Math.abs(target - entry).toFixed(1)
  const slPts     = Math.abs(stoploss - entry).toFixed(1)
  const color     = isLong ? "#22c55e" : "#ef4444"

  return (
    <Card title="Trade Signal" style={{ border: `1px solid ${color}` }}>
      <div style={{
        display:      "flex",
        alignItems:   "center",
        gap:          "10px",
        marginBottom: "16px"
      }}>
        <span style={{
          background:   color,
          color:        "#000",
          fontWeight:   "800",
          fontSize:     "14px",
          padding:      "6px 16px",
          borderRadius: "6px"
        }}>
          {isLong ? "▲ BUY" : "▼ SELL"}
        </span>
        <span style={{ color: "#9ca3af", fontSize: "13px" }}>
          Confidence: <strong style={{ color }}>{s5.confidence}%</strong>
        </span>
      </div>
      <MetricRow label="Entry Price"  value={`₹${entry?.toLocaleString()}`} />
      <MetricRow label="Target"       value={`₹${target?.toLocaleString()} (+${targetPts} pts)`} color="#22c55e" />
      <MetricRow label="Stoploss"     value={`₹${stoploss?.toLocaleString()} (-${slPts} pts)`}   color="#ef4444" />
      <MetricRow label="R/R Ratio"    value={(targetPts / slPts).toFixed(2)} color="#6366f1" />
      <div style={{ marginTop: "12px", fontSize: "11px", color: "#6b7280" }}>
        ⚠️ For educational purposes only. Not financial advice.
      </div>
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
    }}>
      {/* Header */}
      <div style={{
        display:        "flex",
        justifyContent: "space-between",
        alignItems:     "center",
        marginBottom:   "24px"
      }}>
        <div>
          <h1 style={{ fontSize: "22px", fontWeight: "700", margin: 0 }}>
            🇮🇳 Nifty ML Trainer
          </h1>
          <p style={{ color: "#6b7280", fontSize: "12px", margin: "4px 0 0" }}>
            LightGBM · Multi-timeframe · Groq AI Analysis
          </p>
        </div>
        <div style={{ display: "flex", gap: "10px", alignItems: "center" }}>
          {lastUpdate && (
            <span style={{ color: "#6b7280", fontSize: "12px" }}>
              ⏱ {lastUpdate}
            </span>
          )}
          <button onClick={fetchLiveData} style={{
            background: "#2d3148", border: "none", borderRadius: "8px",
            color: "#f9fafb", padding: "8px 16px", cursor: "pointer", fontSize: "13px"
          }}>
            ↻ Refresh
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: "8px", marginBottom: "24px" }}>
        {[
          { id: "live",     label: "Live Signals"    },
          { id: "backtest", label: "Backtest Report" },
          { id: "ai",       label: "AI Analysis"     },
        ].map(t => (
          <button key={t.id} onClick={() => {
            setTab(t.id)
            if (t.id === "ai" && !analysis) fetchAiAnalysis()
          }} style={{
            padding:      "8px 20px",
            borderRadius: "8px",
            border:       "none",
            cursor:       "pointer",
            fontSize:     "13px",
            fontWeight:   "600",
            background:   tab === t.id ? "#6366f1" : "#1e2130",
            color:        tab === t.id ? "#fff"    : "#9ca3af",
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
        }}>
          ⚠ {error}
        </div>
      )}

      {loading ? (
        <div style={{ textAlign: "center", padding: "80px", color: "#6b7280" }}>
          Loading...
        </div>
      ) : (
        <>
          {/* ── LIVE SIGNALS TAB ── */}
          {tab === "live" && (
            <div style={{
              display:             "grid",
              gridTemplateColumns: "1fr 1fr 1fr",
              gap:                 "16px",
              width:               "100%"
            }}>
              {/* Current Signals */}
              <Card title="Current Signals">
                <div style={{ display: "flex", flexDirection: "column", gap: "14px" }}>
                  {signal && Object.entries(signal.signals).map(([w, s]) => (
                    <div key={w} style={{
                      display: "flex", justifyContent: "space-between", alignItems: "center"
                    }}>
                      <span style={{ color: "#9ca3af", fontSize: "13px" }}>{w}-min</span>
                      <SignalBadge signal={s.signal} confidence={s.confidence} />
                    </div>
                  ))}
                </div>
              </Card>

              {/* Market Context */}
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

              {/* Trade Signal Panel */}
              <TradePanel signal={signal} context={context} />

              {/* Signal History — full width */}
              <Card title="Signal History (Last 50)" style={{ gridColumn: "1 / -1" }}>
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px" }}>
                    <thead>
                      <tr style={{ color: "#6b7280" }}>
                        <th style={{ textAlign: "left",   padding: "8px" }}>Time</th>
                        <th style={{ textAlign: "center", padding: "8px" }}>5-min</th>
                        <th style={{ textAlign: "center", padding: "8px" }}>15-min</th>
                        <th style={{ textAlign: "center", padding: "8px" }}>30-min</th>
                        <th style={{ textAlign: "right",  padding: "8px" }}>RSI</th>
                        <th style={{ textAlign: "right",  padding: "8px" }}>VIX</th>
                      </tr>
                    </thead>
                    <tbody>
                      {history.map((row, i) => (
                        <tr key={i} style={{
                          borderBottom: "1px solid #2d3148",
                          background:   i % 2 === 0 ? "#161824" : "transparent"
                        }}>
                          <td style={{ padding: "8px", color: "#9ca3af" }}>
                            {new Date(row.timestamp).toLocaleString("en-IN", {
                              month: "short", day: "numeric",
                              hour: "2-digit", minute: "2-digit"
                            })}
                          </td>
                          {[5, 15, 30].map(w => (
                            <td key={w} style={{ textAlign: "center", padding: "8px" }}>
                              <span style={{
                                color:      SIGNAL_COLORS[row.signals[w]?.signal],
                                fontWeight: "600"
                              }}>
                                {row.signals[w]?.signal}
                              </span>
                            </td>
                          ))}
                          <td style={{ textAlign: "right", padding: "8px" }}>{row.rsi_14}</td>
                          <td style={{ textAlign: "right", padding: "8px" }}>{row.vix}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Card>
            </div>
          )}

          {/* ── BACKTEST TAB ── */}
          {tab === "backtest" && (
            <div style={{
              display:             "grid",
              gridTemplateColumns: "1fr 1fr",
              gap:                 "16px",
              width:               "100%"
            }}>
              {!backtest ? (
                <div style={{ gridColumn: "1/-1", textAlign: "center", padding: "60px", color: "#6b7280" }}>
                  Loading backtest results...
                </div>
              ) : <>
                {/* Summary */}
                <Card title="Backtest Summary (Jul 2025 – Jan 2026 · Unseen Data)">
                  <MetricRow label="Total Trades"  value={backtest.summary.total_trades} />
                  <MetricRow label="Win Rate"       value={`${backtest.summary.win_rate}%`}        color="#22c55e" />
                  <MetricRow label="Total P&L"      value={`${backtest.summary.total_pnl_pts} pts`}
                    color={backtest.summary.total_pnl_pts > 0 ? "#22c55e" : "#ef4444"} />
                  <MetricRow label="Total P&L (₹)"  value={`₹${Number(backtest.summary.total_pnl_inr).toLocaleString()}`}
                    color={backtest.summary.total_pnl_inr > 0 ? "#22c55e" : "#ef4444"} />
                  <MetricRow label="Avg Win"         value={`${backtest.summary.avg_win_pts} pts`}  color="#22c55e" />
                  <MetricRow label="Avg Loss"        value={`${backtest.summary.avg_loss_pts} pts`} color="#ef4444" />
                  <MetricRow label="Risk/Reward"     value={backtest.summary.risk_reward} />
                  <MetricRow label="Sharpe Ratio"    value={backtest.summary.sharpe_ratio} />
                  <MetricRow label="Max Drawdown"    value={`${backtest.summary.max_drawdown_pts} pts`} color="#ef4444" />
                </Card>

                {/* Monthly P&L Bar Chart */}
                <Card title="Monthly P&L (pts)">
                  <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={Object.entries(backtest.monthly_pnl).map(([k, v]) => ({ month: k, pnl: v }))}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                      <XAxis dataKey="month" tick={{ fill: "#6b7280", fontSize: 10 }}
                        tickFormatter={v => v.slice(5)} />
                      <YAxis tick={{ fill: "#6b7280", fontSize: 10 }} />
                      <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2d3148" }}
                        labelStyle={{ color: "#f9fafb" }} />
                      <Bar dataKey="pnl" radius={[4, 4, 0, 0]}>
                        {Object.entries(backtest.monthly_pnl).map(([k, v], i) => (
                          <Cell key={i} fill={v > 0 ? "#22c55e" : "#ef4444"} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </Card>

                {/* Cumulative P&L */}
                <Card title="Cumulative P&L" style={{ gridColumn: "1 / -1" }}>
                  <ResponsiveContainer width="100%" height={240}>
                    <LineChart data={backtest.cumulative_series.filter((_, i) => i % 5 === 0)}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                      <XAxis dataKey="timestamp" tick={{ fill: "#6b7280", fontSize: 10 }}
                        tickFormatter={v => new Date(v).toLocaleDateString("en-IN", {
                          month: "short", year: "2-digit"
                        })} />
                      <YAxis tick={{ fill: "#6b7280", fontSize: 10 }} />
                      <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2d3148" }}
                        formatter={v => [`${Number(v).toFixed(1)} pts`, "Cumulative P&L"]} />
                      <Line type="monotone" dataKey="cumulative_pnl"
                        stroke="#6366f1" dot={false} strokeWidth={2} />
                    </LineChart>
                  </ResponsiveContainer>
                </Card>

                {/* Drawdown */}
                <Card title="Drawdown" style={{ gridColumn: "1 / -1" }}>
                  <ResponsiveContainer width="100%" height={150}>
                    <LineChart data={backtest.drawdown_series.filter((_, i) => i % 5 === 0)}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                      <XAxis dataKey="timestamp" tick={{ fill: "#6b7280", fontSize: 10 }}
                        tickFormatter={v => new Date(v).toLocaleDateString("en-IN", {
                          month: "short", year: "2-digit"
                        })} />
                      <YAxis tick={{ fill: "#6b7280", fontSize: 10 }} />
                      <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2d3148" }}
                        formatter={v => [`${Number(v).toFixed(1)} pts`, "Drawdown"]} />
                      <Line type="monotone" dataKey="drawdown"
                        stroke="#ef4444" dot={false} strokeWidth={1.5} />
                    </LineChart>
                  </ResponsiveContainer>
                </Card>
              </>}
            </div>
          )}

          {/* ── AI ANALYSIS TAB ── */}
          {tab === "ai" && (
            <div style={{
              display:             "grid",
              gridTemplateColumns: "2fr 1fr",
              gap:                 "16px",
              width:               "100%"
            }}>
              <Card title="AI Analysis · Groq Llama 3.3 70B" style={{ gridColumn: "1 / -1" }}>
                {aiLoading ? (
                  <div style={{ color: "#6b7280", textAlign: "center", padding: "40px" }}>
                    Analysing market conditions...
                  </div>
                ) : analysis ? (
                  <div>
                    <div style={{
                      background: "#161824", borderRadius: "8px",
                      padding: "16px", lineHeight: "1.8",
                      color: "#e5e7eb", fontSize: "14px", marginBottom: "16px"
                    }}>
                      {analysis.analysis}
                    </div>
                    <button onClick={fetchAiAnalysis} style={{
                      background: "#6366f1", border: "none", borderRadius: "8px",
                      color: "#fff", padding: "8px 20px", cursor: "pointer", fontSize: "13px"
                    }}>
                      ↻ Refresh Analysis
                    </button>
                  </div>
                ) : null}
              </Card>

              {signal && (
                <Card title="Current Signals">
                  {Object.entries(signal.signals).map(([w, s]) => (
                    <div key={w} style={{
                      display: "flex", justifyContent: "space-between",
                      alignItems: "center", marginBottom: "12px"
                    }}>
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