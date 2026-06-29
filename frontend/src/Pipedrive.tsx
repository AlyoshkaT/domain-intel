import { useState, useEffect, useCallback, useMemo } from "react"
import { type Lang } from "./i18n"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

interface StatusRow {
  domain: string
  status_pipedrive: string
  paid_m1: boolean
  paid_m2: boolean
  paid_m3: boolean
  status_fact: string
  risk: string
  last_paid_at: string | null
  won_deals: number
  total_deals: number
  total_paid_value: number
  currency: string
  org_name: string
  computed_at: string
}

// Bilingual labels — the app is UA/EN.
const L = (lang: Lang) => ({
  title: lang === "ua" ? "Pipedrive — статус відносин" : "Pipedrive — relationship status",
  subtitle: lang === "ua"
    ? "Поточний статус кожного домену за історією оплат (won-угоди по місяцях)"
    : "Per-domain status derived from payment history (won deals by month)",
  fact: lang === "ua" ? "Статус ФАКТ" : "Status FACT",
  allFact: lang === "ua" ? "Усі статуси" : "All statuses",
  risk: lang === "ua" ? "Ризик" : "Risk",
  allRisk: lang === "ua" ? "Усі" : "All",
  search: lang === "ua" ? "Пошук за доменом / організацією…" : "Search by domain / organization…",
  sync: lang === "ua" ? "↻ Синхронізувати" : "↻ Sync",
  syncing: lang === "ua" ? "Синхронізація…" : "Syncing…",
  load: lang === "ua" ? "Оновити" : "Reload",
  loading: lang === "ua" ? "Завантаження…" : "Loading…",
  empty: lang === "ua" ? "Немає даних. Натисни «Синхронізувати», щоб витягти з Pipedrive." : "No data. Click Sync to pull from Pipedrive.",
  noMatch: lang === "ua" ? "Нічого не знайдено за фільтром." : "Nothing matches the filter.",
  count: (a: string, b: string) => lang === "ua" ? `${a} з ${b}` : `${a} of ${b}`,
  thDomain: lang === "ua" ? "Домен" : "Domain",
  thOrg: lang === "ua" ? "Організація" : "Organization",
  thPd: "Pipedrive",
  thFact: lang === "ua" ? "ФАКТ" : "FACT",
  thRisk: lang === "ua" ? "Ризик" : "Risk",
  thLast: lang === "ua" ? "Остання оплата" : "Last paid",
  thWon: lang === "ua" ? "Оплат" : "Payments",
  thValue: lang === "ua" ? "Сума оплат" : "Paid total",
  months: lang === "ua" ? "Цей міс / −1 / −2" : "This mo / −1 / −2",
})

const FACT_COLORS: Record<string, string> = {
  Won: "#22c55e", Open: "#3b82f6", Lost: "#ef4444",
}
const RISK_COLORS: Record<string, string> = {
  Alarm: "#f59e0b", Churn: "#a855f7",
}

function Badge({ text, color }: { text: string; color: string }) {
  return (
    <span className="service-tag"
      style={{ background: color + "22", color, border: `1px solid ${color}44` }}>
      {text}
    </span>
  )
}

export default function PipedrivePage({ lang, can }: { lang: Lang; can: (p: string) => boolean }) {
  const tx = L(lang)
  const [rows, setRows] = useState<StatusRow[]>([])
  const [loading, setLoading] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [error, setError] = useState("")
  const [factFilter, setFactFilter] = useState("")
  const [riskFilter, setRiskFilter] = useState("")
  const [search, setSearch] = useState("")

  const load = useCallback(async () => {
    setLoading(true); setError("")
    try {
      const d = await apiFetch("/api/pipedrive/status")
      setRows(d.rows || [])
    } catch (e: any) { setError(e.message) } finally { setLoading(false) }
  }, [])

  useEffect(() => { load() }, [load])

  const sync = useCallback(async () => {
    setSyncing(true); setError("")
    try {
      const r = await apiFetch("/api/pipedrive/sync", { method: "POST" })
      if (r.status === "error") setError(r.error || "sync failed")
      await load()
    } catch (e: any) { setError(e.message) } finally { setSyncing(false) }
  }, [load])

  const summary = useMemo(() => {
    const s: Record<string, number> = { Won: 0, Open: 0, Lost: 0, Alarm: 0, Churn: 0 }
    for (const r of rows) {
      if (r.status_fact in s) s[r.status_fact]++
      if (r.risk) s[r.risk]++
    }
    return s
  }, [rows])

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    return rows.filter(r =>
      (!factFilter || r.status_fact === factFilter) &&
      (!riskFilter || r.risk === riskFilter) &&
      (!q || r.domain.includes(q) || (r.org_name || "").toLowerCase().includes(q))
    )
  }, [rows, factFilter, riskFilter, search])

  const exportCSV = useCallback(() => {
    const cols = ["domain", "org_name", "status_pipedrive", "status_fact", "risk",
      "paid_m1", "paid_m2", "paid_m3", "last_paid_at", "won_deals", "total_deals",
      "total_paid_value", "currency", "computed_at"]
    const esc = (v: any) => `"${String(v ?? "").replace(/"/g, '""')}"`
    const csv = [cols.join(","), ...filtered.map(r => cols.map(c => esc((r as any)[c])).join(","))].join("\n")
    const a = document.createElement("a")
    a.href = URL.createObjectURL(new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8" }))
    a.download = `pipedrive_status_${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
  }, [filtered])

  const selStyle: React.CSSProperties = {
    padding: "5px 8px", background: "var(--bg-2)", border: "1px solid var(--border)",
    borderRadius: "var(--radius)", color: "var(--text)", fontSize: 12, outline: "none", minWidth: 120,
  }
  const dot = (v: boolean) => (
    <span title={v ? "paid" : "—"} style={{
      display: "inline-block", width: 11, height: 11, borderRadius: "50%",
      background: v ? "#22c55e" : "var(--border)", margin: "0 2px",
    }} />
  )

  return (
    <div className="page page-wide">
      <div className="page-header">
        <h1 className="page-title">{tx.title}</h1>
        <span style={{ fontSize: 12, color: "var(--text-3)" }}>{tx.subtitle}</span>
      </div>

      {/* Summary cards */}
      <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 12 }}>
        {(["Won", "Open", "Lost"] as const).map(k => (
          <div key={k} className="card" style={{ padding: "10px 16px", minWidth: 90, cursor: "pointer", borderColor: factFilter === k ? FACT_COLORS[k] : undefined }}
            onClick={() => setFactFilter(factFilter === k ? "" : k)}>
            <div style={{ fontSize: 11, color: "var(--text-3)" }}>{k}</div>
            <div style={{ fontSize: 22, fontWeight: 700, color: FACT_COLORS[k] }}>{summary[k].toLocaleString()}</div>
          </div>
        ))}
        {(["Alarm", "Churn"] as const).map(k => (
          <div key={k} className="card" style={{ padding: "10px 16px", minWidth: 90, cursor: "pointer", borderColor: riskFilter === k ? RISK_COLORS[k] : undefined }}
            onClick={() => setRiskFilter(riskFilter === k ? "" : k)}>
            <div style={{ fontSize: 11, color: "var(--text-3)" }}>{k}</div>
            <div style={{ fontSize: 22, fontWeight: 700, color: RISK_COLORS[k] }}>{summary[k].toLocaleString()}</div>
          </div>
        ))}
      </div>

      {/* Filters */}
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="tech-filters" style={{ flexWrap: "wrap", gap: 10, alignItems: "flex-end" }}>
          <div className="tech-filter-group">
            <label className="tech-filter-label">{tx.fact}</label>
            <select style={selStyle} value={factFilter} onChange={e => setFactFilter(e.target.value)}>
              <option value="">{tx.allFact}</option>
              {["Won", "Open", "Lost"].map(f => <option key={f} value={f}>{f}</option>)}
            </select>
          </div>
          <div className="tech-filter-group">
            <label className="tech-filter-label">{tx.risk}</label>
            <select style={selStyle} value={riskFilter} onChange={e => setRiskFilter(e.target.value)}>
              <option value="">{tx.allRisk}</option>
              {["Alarm", "Churn"].map(f => <option key={f} value={f}>{f}</option>)}
            </select>
          </div>
          <button className="btn-search" onClick={load} disabled={loading}>
            {loading ? tx.loading : "🔄"} {tx.load}
          </button>
          {can("admin") && (
            <button className="btn-search" onClick={sync} disabled={syncing} style={{ marginLeft: "auto" }}>
              {syncing ? tx.syncing : tx.sync}
            </button>
          )}
        </div>
        {error && <div style={{ marginTop: 8, color: "var(--danger)", fontSize: 12 }}>⚠ {error}</div>}
      </div>

      {/* Table controls */}
      {rows.length > 0 && (
        <div className="filter-row" style={{ marginBottom: 8 }}>
          <input className="filter-input" placeholder={tx.search} value={search} onChange={e => setSearch(e.target.value)} />
          <span className="filter-count">{tx.count(filtered.length.toLocaleString(), rows.length.toLocaleString())}</span>
          <button className="btn-export" onClick={exportCSV}>↓ CSV</button>
        </div>
      )}

      {loading && <div className="loading-center"><span className="spinner-lg" /></div>}

      {!loading && filtered.length > 0 && (
        <div className="table-wrap table-fixed-height">
          <table className="results-table">
            <thead>
              <tr>
                <th>{tx.thDomain}</th>
                <th>{tx.thOrg}</th>
                <th>{tx.thPd}</th>
                <th>{tx.thFact}</th>
                <th>{tx.thRisk}</th>
                <th title={tx.months}>{tx.months}</th>
                <th>{tx.thLast}</th>
                <th style={{ textAlign: "right" }}>{tx.thWon}</th>
                <th style={{ textAlign: "right" }}>{tx.thValue}</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => (
                <tr key={i}>
                  <td className="td-domain"><a href={`https://${r.domain}`} target="_blank" rel="noopener">{r.domain}</a></td>
                  <td style={{ fontSize: 12, color: "var(--text-2)" }}>{r.org_name || "—"}</td>
                  <td style={{ fontSize: 11, color: "var(--text-3)" }}>{r.status_pipedrive}</td>
                  <td><Badge text={r.status_fact} color={FACT_COLORS[r.status_fact] || "#6b7280"} /></td>
                  <td>{r.risk ? <Badge text={r.risk} color={RISK_COLORS[r.risk] || "#6b7280"} /> : ""}</td>
                  <td style={{ whiteSpace: "nowrap" }}>{dot(r.paid_m1)}{dot(r.paid_m2)}{dot(r.paid_m3)}</td>
                  <td style={{ fontFamily: "var(--mono)", fontSize: 11, whiteSpace: "nowrap" }}>{r.last_paid_at || "—"}</td>
                  <td style={{ textAlign: "right", fontFamily: "var(--mono)", fontSize: 11 }}>{r.won_deals}</td>
                  <td style={{ textAlign: "right", fontFamily: "var(--mono)", fontSize: 11, whiteSpace: "nowrap" }}>
                    {r.total_paid_value ? r.total_paid_value.toLocaleString() : "0"} {r.currency}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && rows.length === 0 && <div className="empty-state">{tx.empty}</div>}
      {!loading && rows.length > 0 && filtered.length === 0 && <div className="empty-state">{tx.noMatch}</div>}
    </div>
  )
}
