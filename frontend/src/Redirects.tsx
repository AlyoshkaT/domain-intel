import { useState, useEffect, useCallback, useMemo } from "react"
import { t, type Lang } from "./i18n"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

interface Redirect {
  original: string
  resolved: string
  type: string
  detected_at: string
  job_id: string
}

interface JobOption { job_id: string; first_seen: string }

const TYPE_LABELS: Record<string, string> = {
  www: "www",
  subdomain: "Subdomain",
  http_redirect: "HTTP redirect",
  known: "Known",
}

const TYPE_COLORS: Record<string, string> = {
  www: "#3b82f6",
  subdomain: "#a855f7",
  http_redirect: "#f59e0b",
  known: "#6b7280",
}

export default function RedirectsPage({ lang }: { lang: Lang }) {
  const [search, setSearch] = useState("")
  const [typeFilter, setTypeFilter] = useState("")
  const [jobFilter, setJobFilter] = useState("")
  const [dateFrom, setDateFrom] = useState("")
  const [dateTo, setDateTo] = useState("")
  const [loading, setLoading] = useState(false)
  const [rows, setRows] = useState<Redirect[]>([])
  const [jobs, setJobs] = useState<JobOption[]>([])
  const [error, setError] = useState("")
  const [localSearch, setLocalSearch] = useState("")

  // Load job list for dropdown
  useEffect(() => {
    apiFetch("/api/redirects/jobs").then(d => setJobs(d.jobs || [])).catch(() => {})
  }, [])

  const load = useCallback(async () => {
    setLoading(true)
    setError("")
    try {
      const params = new URLSearchParams()
      if (typeFilter) params.set("type", typeFilter)
      if (jobFilter) params.set("job_id", jobFilter)
      if (dateFrom) params.set("date_from", dateFrom)
      if (dateTo) params.set("date_to", dateTo)
      params.set("limit", "5000")
      const data = await apiFetch(`/api/redirects?${params}`)
      setRows(data.redirects || [])
      if (data.error) setError(data.error)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [typeFilter, jobFilter, dateFrom, dateTo])

  // Auto-load on mount
  useEffect(() => { load() }, [])

  const filtered = useMemo(() => {
    if (!localSearch) return rows
    const q = localSearch.toLowerCase()
    return rows.filter(r =>
      r.original.toLowerCase().includes(q) ||
      r.resolved.toLowerCase().includes(q) ||
      r.job_id.toLowerCase().includes(q)
    )
  }, [rows, localSearch])

  const exportCSV = useCallback(() => {
    const cols = ["original", "resolved", "type", "detected_at", "job_id"]
    const csv = [
      cols.join(","),
      ...filtered.map(r => cols.map(c => `"${String((r as any)[c] || "").replace(/"/g, '""')}"`).join(","))
    ].join("\n")
    const a = document.createElement("a")
    a.href = URL.createObjectURL(new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8" }))
    a.download = `redirects_${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
  }, [filtered])

  // Unique types in loaded data
  const availableTypes = useMemo(() => [...new Set(rows.map(r => r.type))].sort(), [rows])

  const selStyle: React.CSSProperties = {
    padding: "5px 8px",
    background: "var(--bg-2)",
    border: "1px solid var(--border)",
    borderRadius: "var(--radius)",
    color: "var(--text)",
    fontSize: 12,
    outline: "none",
    minWidth: 120,
  }

  return (
    <div className="page page-wide">
      <div className="page-header">
        <h1 className="page-title">{t('redir_title', lang)}</h1>
        <span style={{ fontSize: 12, color: "var(--text-3)" }}>
          {t('redir_subtitle', lang)}
        </span>
      </div>

      {/* Filters */}
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="tech-filters" style={{ flexWrap: "wrap", gap: 10 }}>
          <div className="tech-filter-group">
            <label className="tech-filter-label">{t('redir_type', lang)}</label>
            <select style={selStyle} value={typeFilter} onChange={e => setTypeFilter(e.target.value)}>
              <option value="">{t('redir_all_types', lang)}</option>
              {availableTypes.map(tp => (
                <option key={tp} value={tp}>{TYPE_LABELS[tp] || tp}</option>
              ))}
            </select>
          </div>

          <div className="tech-filter-group">
            <label className="tech-filter-label">{t('redir_job', lang)}</label>
            <select style={{ ...selStyle, maxWidth: 160 }} value={jobFilter} onChange={e => setJobFilter(e.target.value)}>
              <option value="">{t('redir_all_jobs', lang)}</option>
              {jobs.map(j => (
                <option key={j.job_id} value={j.job_id}>
                  {j.job_id.slice(0, 8)}… ({j.first_seen.slice(0, 10)})
                </option>
              ))}
            </select>
          </div>

          <div className="tech-filter-group">
            <label className="tech-filter-label">{t('redir_from', lang)}</label>
            <input
              type="date"
              style={selStyle}
              value={dateFrom}
              onChange={e => setDateFrom(e.target.value)}
            />
          </div>

          <div className="tech-filter-group">
            <label className="tech-filter-label">{t('redir_to', lang)}</label>
            <input
              type="date"
              style={selStyle}
              value={dateTo}
              onChange={e => setDateTo(e.target.value)}
            />
          </div>

          <button className="btn-search" onClick={load} disabled={loading}>
            {loading ? t('redir_loading', lang) : "🔍"} {t('redir_load', lang)}
          </button>
        </div>
        {error && <div style={{ marginTop: 8, color: "var(--danger)", fontSize: 12 }}>⚠ {error}</div>}
      </div>

      {/* Table controls */}
      {rows.length > 0 && (
        <div className="filter-row" style={{ marginBottom: 8 }}>
          <input
            className="filter-input"
            placeholder={t('redir_search_ph', lang)}
            value={localSearch}
            onChange={e => setLocalSearch(e.target.value)}
          />
          <span className="filter-count">{t('redir_count', lang)(filtered.length.toLocaleString(), rows.length.toLocaleString())}</span>
          <button className="btn-export" onClick={exportCSV}>↓ CSV</button>
        </div>
      )}

      {/* Table */}
      {loading && <div className="loading-center"><span className="spinner-lg" /></div>}

      {!loading && filtered.length > 0 && (
        <div className="table-wrap table-fixed-height">
          <table className="results-table">
            <thead>
              <tr>
                <th>Original</th>
                <th>Resolved</th>
                <th>{t('redir_type', lang)}</th>
                <th>Detected</th>
                <th>Job ID</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => (
                <tr key={i}>
                  <td className="td-domain">
                    <a href={`https://${r.original}`} target="_blank" rel="noopener">{r.original}</a>
                  </td>
                  <td className="td-domain">
                    <a href={`https://${r.resolved}`} target="_blank" rel="noopener">{r.resolved}</a>
                  </td>
                  <td>
                    <span
                      className="service-tag"
                      style={{ background: (TYPE_COLORS[r.type] || "#6b7280") + "22", color: TYPE_COLORS[r.type] || "var(--text-3)", border: `1px solid ${TYPE_COLORS[r.type] || "var(--border)"}44` }}
                    >
                      {TYPE_LABELS[r.type] || r.type}
                    </span>
                  </td>
                  <td style={{ fontFamily: "var(--mono)", fontSize: 11, whiteSpace: "nowrap" }}>{r.detected_at}</td>
                  <td style={{ fontFamily: "var(--mono)", fontSize: 11 }}>
                    <span title={r.job_id}>{r.job_id ? r.job_id.slice(0, 8) + "…" : "—"}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && rows.length === 0 && (
        <div className="empty-state">{t('redir_empty', lang)}</div>
      )}
      {!loading && rows.length > 0 && filtered.length === 0 && (
        <div className="empty-state">{t('redir_no_match', lang)}</div>
      )}
    </div>
  )
}
