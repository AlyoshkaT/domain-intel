import { useState, useEffect, useCallback } from "react"
import ExplorerPage from "./Explorer"
import "./index.css"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

interface Job {
  job_id: string; status: string; total_domains: number
  processed_domains: number; failed_domains: number
  services: string[]; filename: string; created_at: string
  updated_at: string; error_message?: string
}
interface Result {
  domain: string; status: string; sw_visits?: number
  cms_list?: string; ai_category?: string; ai_is_ecommerce?: string
  ai_industry?: string; bw_vertical?: string; sw_category?: string
  sw_subcategory?: string; sw_description?: string; sw_title?: string
  company_name?: string; sw_primary_region?: string
  sw_primary_region_pct?: number; error_detail?: string
}
interface Credits { similarweb?: number | null; builtwith?: number | null }

function cell(v?: string | null) { return v && v.trim() ? v : "—" }

// ─── Credits Dialog ───────────────────────────────────────────────────────────
function CreditsDialog({ credits, services, domainCount, onConfirm, onCancel }: {
  credits: Credits; services: string[]; domainCount: number
  onConfirm: () => void; onCancel: () => void
}) {
  const bwNeeded = services.includes("builtwith") ? domainCount : 0
  const swNeeded = services.includes("similarweb") ? domainCount : 0
  const bwOk = credits.builtwith == null || credits.builtwith >= bwNeeded
  const swOk = credits.similarweb == null || credits.similarweb >= swNeeded
  const canProceed = bwOk && swOk
  return (
    <div className="dialog-overlay">
      <div className="dialog">
        <div className="dialog-title">{canProceed ? "Підтвердження запуску" : "⛔ Обробка неможлива"}</div>
        <div className="dialog-body">
          {services.includes("builtwith") && (
            <div className={`dialog-row ${!bwOk ? "dialog-row-error" : ""}`}>
              <span className="dialog-service">BuiltWith</span>
              <span className="dialog-stat">Залишок: <b>{credits.builtwith?.toLocaleString() ?? "?"}</b></span>
              <span className="dialog-stat">До обробки: <b>{bwNeeded.toLocaleString()}</b></span>
              {!bwOk && <span className="dialog-warn">Недостатньо</span>}
            </div>
          )}
          {services.includes("similarweb") && (
            <div className={`dialog-row ${!swOk ? "dialog-row-error" : ""}`}>
              <span className="dialog-service">SimilarWeb</span>
              <span className="dialog-stat">Залишок: <b>{credits.similarweb?.toLocaleString() ?? "?"}</b></span>
              <span className="dialog-stat">До обробки: <b>{swNeeded.toLocaleString()}</b></span>
              {!swOk && <span className="dialog-warn">Недостатньо</span>}
            </div>
          )}
          {!canProceed && <div className="dialog-error-msg">ОБРОБКА НЕМОЖЛИВА — ЗА НЕСТАЧЕЮ СПЛАЧЕНИХ ЗАПИТІВ</div>}
          {canProceed && <div className="dialog-question">Ви впевнені що хочете запустити обробку?</div>}
        </div>
        <div className="dialog-actions">
          {canProceed
            ? <><button className="dialog-btn dialog-btn-confirm" onClick={onConfirm}>Так</button>
                <button className="dialog-btn dialog-btn-cancel" onClick={onCancel}>Ні</button></>
            : <button className="dialog-btn dialog-btn-cancel" onClick={onCancel}>Зрозуміло</button>}
        </div>
      </div>
    </div>
  )
}

function ServiceToggle({ id, label, sublabel, checked, onChange }: {
  id: string; label: string; sublabel?: string; checked: boolean; onChange: () => void
}) {
  return (
    <button onClick={onChange} className={`service-toggle ${checked ? "active" : ""}`}>
      <div className="service-toggle-dot" />
      <div><div className="service-toggle-label">{label}</div>
        {sublabel && <div className="service-toggle-sub">{sublabel}</div>}</div>
    </button>
  )
}

function CreditsBar({ credits, onRefresh }: { credits: Credits; onRefresh: () => void }) {
  return (
    <div className="credits-bar">
      <div className="credits-item">
        <span className="credits-label">SimilarWeb</span>
        <span className="credits-sep">–</span>
        <span className="credits-value">{credits.similarweb != null ? credits.similarweb.toLocaleString() : "—"}</span>
      </div>
      <div className="credits-divider" />
      <div className="credits-item">
        <span className="credits-label">BuiltWith</span>
        <span className="credits-sep">–</span>
        <span className="credits-value">{credits.builtwith != null ? credits.builtwith.toLocaleString() : "—"}</span>
      </div>
      <button className="credits-refresh" onClick={onRefresh} title="Оновити">↻</button>
    </div>
  )
}

function StatusBadge({ status }: { status: string }) {
  const map: Record<string, string> = {
    pending: "badge-pending", running: "badge-running", completed: "badge-completed",
    completed_with_errors: "badge-warn", failed: "badge-failed", cancelled: "badge-cancelled",
  }
  return <span className={`badge ${map[status] || "badge-pending"}`}>{status}</span>
}

function ProgressBar({ value, total }: { value: number; total: number }) {
  const pct = total > 0 ? Math.round((value / total) * 100) : 0
  return (
    <div className="progress-wrap">
      <div className="progress-track"><div className="progress-fill" style={{ width: `${pct}%` }} /></div>
      <span className="progress-label">{value}/{total} ({pct}%)</span>
    </div>
  )
}

// ─── Job status line ──────────────────────────────────────────────────────────
function JobStatusLine({ job }: { job: Job }) {
  const done = (job.processed_domains || 0) + (job.failed_domains || 0)
  const total = job.total_domains || 0
  const pct = total > 0 ? Math.round((done / total) * 100) : 0

  const stageLabel = () => {
    if (job.status === "pending") return "⏳ Очікування запуску..."
    if (job.status === "running") return `🔄 Обробка: ${done.toLocaleString()} / ${total.toLocaleString()} (${pct}%)`
    if (job.status === "completed") return `✅ Завершено: ${total.toLocaleString()} доменів`
    if (job.status === "completed_with_errors") return `⚠️ Завершено з помилками: ${job.failed_domains} помилок з ${total.toLocaleString()}`
    if (job.status === "failed") return `❌ Помилка: ${job.error_message || ""}`
    if (job.status === "cancelled") return "🚫 Скасовано"
    return job.status
  }

  return (
    <div className="job-status-line">
      <span className="job-status-text">{stageLabel()}</span>
      {(job.status === "running" || job.status === "pending") && (
        <div className="job-status-services">
          {job.services?.map(s => <span key={s} className="service-tag">{s}</span>)}
        </div>
      )}
    </div>
  )
}

// ─── Page: New Job ─────────────────────────────────────────────────────────────
function NewJobPage({ onJobCreated }: { onJobCreated: (id: string) => void }) {
  const [services, setServices] = useState<string[]>(["similarweb", "builtwith"])
  const [file, setFile] = useState<File | null>(null)
  const [manualDomains, setManualDomains] = useState("")
  const [dragging, setDragging] = useState(false)
  const [forceRefresh, setForceRefresh] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState("")
  const [credits, setCredits] = useState<Credits>({})
  const [showDialog, setShowDialog] = useState(false)
  const [domainCount, setDomainCount] = useState(0)

  const loadCredits = useCallback(async () => {
    try { setCredits(await apiFetch("/api/credits")) } catch {}
  }, [])
  useEffect(() => { loadCredits() }, [loadCredits])

  const toggle = (id: string) => setServices(p => p.includes(id) ? p.filter(s => s !== id) : [...p, id])

  const handleFile = (f: File) => {
    if (f.name.match(/\.(csv|xlsx|xls|txt)$/i)) { setFile(f); setManualDomains(""); setError("") }
    else setError("Підтримуються: CSV, XLSX, TXT")
  }
  const handleManualChange = (v: string) => { setManualDomains(v); if (v.trim()) setFile(null) }
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault(); setDragging(false)
    if (e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0])
  }

  const handleSubmitClick = () => {
    if (!file && !manualDomains.trim()) return setError("Завантажте файл або введіть домени")
    if (services.length === 0) return setError("Виберіть хоча б один сервіс")
    const cnt = manualDomains.trim() ? manualDomains.split(/\r?\n/).filter(s => s.trim()).length : 0
    setDomainCount(cnt); setShowDialog(true)
  }

  const handleConfirm = async () => {
    setShowDialog(false); setLoading(true); setError("")
    try {
      const fd = new FormData()
      let sendFile = file
      if (!sendFile && manualDomains.trim()) {
        const content = manualDomains.split(/\r?\n/).map(s => s.trim()).filter(Boolean).join('\n')
        sendFile = new File([new Blob([content])], 'domains.txt', { type: 'text/plain' })
      }
      if (!sendFile) throw new Error('No domains')
      fd.append("file", sendFile)
      fd.append("services", JSON.stringify(services))
      fd.append("force_refresh", String(forceRefresh))
      const res = await fetch("/api/jobs", { method: "POST", body: fd })
      if (!res.ok) { const e = await res.json().catch(() => ({ detail: "Error" })); throw new Error(e.detail) }
      const data = await res.json()
      onJobCreated(data.job_id)
    } catch (e: any) { setError(e.message) }
    finally { setLoading(false) }
  }

  // П.1 — WhatCMS removed
  const serviceOptions = [
    { id: "similarweb", label: "SimilarWeb", sublabel: "трафік · категорія · регіон" },
    { id: "builtwith",  label: "BuiltWith",  sublabel: "CMS · пошук · email маркетинг" },
    { id: "ai",         label: "Claude AI",  sublabel: "категорія · галузь · e-comm" },
  ]

  return (
    <div className="page">
      {showDialog && <CreditsDialog credits={credits} services={services} domainCount={domainCount} onConfirm={handleConfirm} onCancel={() => setShowDialog(false)} />}
      <div className="page-header">
        <h1 className="page-title">Новий аналіз</h1>
        <CreditsBar credits={credits} onRefresh={async () => { await apiFetch("/api/credits/refresh", { method: "POST" }); await loadCredits() }} />
      </div>
      <div className="card">
        <div className="card-section-title">Сервіси аналізу</div>
        <div className="services-grid">
          {serviceOptions.map(s => <ServiceToggle key={s.id} id={s.id} label={s.label} sublabel={s.sublabel} checked={services.includes(s.id)} onChange={() => toggle(s.id)} />)}
        </div>
      </div>
      <div className="card">
        <div className="card-section-title">Вставити домени вручну</div>
        <textarea className="manual-domains" placeholder={"один домен на рядок\nexample.com\nshop.example.ua"}
          value={manualDomains} onChange={e => handleManualChange(e.target.value)} rows={5} />
        {manualDomains.trim() && <div className="domain-count-hint">{manualDomains.split(/\r?\n/).filter(s => s.trim()).length} доменів</div>}
      </div>
      <div className="card">
        <div className="card-section-title">Або завантажити файл</div>
        <div className={`dropzone ${dragging ? "dragging" : ""} ${file ? "has-file" : ""}`}
          onDragOver={e => { e.preventDefault(); setDragging(true) }}
          onDragLeave={() => setDragging(false)} onDrop={handleDrop}
          onClick={() => document.getElementById("file-input")?.click()}>
          <input id="file-input" type="file" accept=".csv,.xlsx,.xls,.txt" style={{ display: "none" }}
            onChange={e => e.target.files?.[0] && handleFile(e.target.files[0])} />
          {file
            ? <div className="dropzone-file"><span className="dropzone-icon">📄</span><span className="dropzone-filename">{file.name}</span><span className="dropzone-filesize">{(file.size / 1024).toFixed(0)} KB</span></div>
            : <div className="dropzone-empty"><span className="dropzone-icon">⬆</span><span>Перетягніть CSV або XLSX файл</span><span className="dropzone-hint">або натисніть для вибору</span></div>}
        </div>
      </div>
      <div className="force-refresh-row">
        <button className={`force-toggle ${forceRefresh ? "active" : ""}`} onClick={() => setForceRefresh(!forceRefresh)}>
          <div className="force-toggle-indicator" />
          <div><div className="force-toggle-label">Ігнорувати кеш</div><div className="force-toggle-sub">Примусово оновити дані навіть якщо є в кеші</div></div>
        </button>
      </div>
      {error && <div className="error-msg">{error}</div>}
      <button className={`btn-primary ${loading ? "loading" : ""}`} onClick={handleSubmitClick}
        disabled={loading || (!file && !manualDomains.trim()) || services.length === 0}>
        {loading ? <span className="spinner" /> : null}{loading ? "Запускаємо..." : "Запустити аналіз"}
      </button>
    </div>
  )
}

// ─── Page: Jobs ────────────────────────────────────────────────────────────────
function JobsPage({ onSelect }: { onSelect: (id: string) => void }) {
  const [jobs, setJobs] = useState<Job[]>([])
  const [loading, setLoading] = useState(true)
  const load = useCallback(async () => {
    try { const d = await apiFetch("/api/jobs"); setJobs(d.jobs) } catch {} finally { setLoading(false) }
  }, [])
  useEffect(() => { load(); const iv = setInterval(load, 3000); return () => clearInterval(iv) }, [load])
  const hasRunning = jobs.some(j => j.status === "running" || j.status === "pending")

  return (
    <div className="page">
      <div className="page-header">
        <h1 className="page-title">Job-и</h1>
        {hasRunning && <div className="live-indicator"><span />Live</div>}
      </div>
      {loading ? <div className="loading-center"><span className="spinner-lg" /></div>
        : jobs.length === 0 ? <div className="empty-state">Ще немає job-ів.</div>
        : (
          <div className="jobs-list">
            {jobs.map(job => (
              <div key={job.job_id} className="job-card" onClick={() => onSelect(job.job_id)}>
                <div className="job-card-header">
                  <div>
                    <div className="job-filename">{job.filename}</div>
                    <div className="job-meta">
                      <span className="job-id">{job.job_id.slice(0, 8)}</span>
                      <span className="job-date">{new Date(job.created_at).toLocaleString("uk-UA")}</span>
                    </div>
                  </div>
                  <StatusBadge status={job.status} />
                </div>
                <JobStatusLine job={job} />
                <ProgressBar value={(job.processed_domains || 0) + (job.failed_domains || 0)} total={job.total_domains || 0} />
              </div>
            ))}
          </div>
        )}
    </div>
  )
}

// ─── Page: Results ─────────────────────────────────────────────────────────────
const COLUMNS = [
  { key: "domain", label: "Domain", w: "160px" }, { key: "sw_visits", label: "Traffic", w: "110px" },
  { key: "cms_list", label: "CMS", w: "120px" }, { key: "ai_category", label: "AI Category", w: "140px" },
  { key: "ai_is_ecommerce", label: "AI Ecomm", w: "90px" }, { key: "ai_industry", label: "AI Industry", w: "150px" },
  { key: "bw_vertical", label: "Industry BW", w: "120px" }, { key: "sw_category", label: "Category SW", w: "160px" },
  { key: "sw_subcategory", label: "Subcategory SW", w: "160px" }, { key: "sw_description", label: "Description", w: "200px" },
  { key: "sw_title", label: "Title", w: "160px" }, { key: "company_name", label: "Company", w: "140px" },
  { key: "sw_primary_region", label: "Region", w: "80px" }, { key: "sw_primary_region_pct", label: "Region %", w: "80px" },
]

function ResultsPage({ jobId, onBack }: { jobId: string; onBack: () => void }) {
  const [job, setJob] = useState<Job | null>(null)
  const [results, setResults] = useState<Result[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState("")

  const loadData = useCallback(async () => {
    try {
      const [j, r] = await Promise.all([apiFetch(`/api/jobs/${jobId}`), apiFetch(`/api/jobs/${jobId}/results`)])
      setJob(j); setResults(r.results)
    } catch {} finally { setLoading(false) }
  }, [jobId])

  useEffect(() => {
    loadData()
    const iv = setInterval(() => { if (job?.status === "running" || job?.status === "pending") loadData() }, 3000)
    return () => clearInterval(iv)
  }, [loadData, job?.status])

  const filtered = results.filter(r =>
    !filter || r.domain.includes(filter.toLowerCase()) ||
    (r.sw_category || "").toLowerCase().includes(filter.toLowerCase()) ||
    (r.ai_industry || "").toLowerCase().includes(filter.toLowerCase()) ||
    (r.cms_list || "").toLowerCase().includes(filter.toLowerCase())
  )

  const renderCell = (r: Result, key: string): string => {
    if (key === "sw_visits") return r.sw_visits ? r.sw_visits.toLocaleString("en-US") : "—"
    if (key === "sw_primary_region_pct") return r.sw_primary_region_pct != null ? `${r.sw_primary_region_pct}%` : "—"
    return cell((r as any)[key])
  }

  if (loading) return <div className="loading-center"><span className="spinner-lg" /></div>

  return (
    <div className="page page-wide">
      <div className="page-header">
        <div className="back-btn" onClick={onBack}>← Назад</div>
        <h1 className="page-title">{job?.filename}</h1>
        <div className="page-header-actions">
          {job && <StatusBadge status={job.status} />}
          <button className="btn-export" onClick={() => window.open(`/api/jobs/${jobId}/export/csv`, "_blank")}>CSV</button>
          <button className="btn-export" onClick={() => window.open(`/api/jobs/${jobId}/export/xlsx`, "_blank")}>XLSX</button>
        </div>
      </div>
      {job && <JobStatusLine job={job} />}
      {job && (job.status === "running" || job.status === "pending") && (
        <div className="progress-banner">
          <ProgressBar value={(job.processed_domains || 0) + (job.failed_domains || 0)} total={job.total_domains || 0} />
        </div>
      )}
      <div className="filter-row">
        <input className="filter-input" placeholder="Фільтр по домену, CMS, категорії..."
          value={filter} onChange={e => setFilter(e.target.value)} />
        <span className="filter-count">{filtered.length} доменів</span>
      </div>
      <div className="table-wrap">
        <table className="results-table">
          <thead><tr>{COLUMNS.map(c => <th key={c.key} style={{ minWidth: c.w }}>{c.label}</th>)}</tr></thead>
          <tbody>
            {filtered.map(r => (
              <tr key={r.domain} className={r.status === "error" ? "row-error" : ""}>
                {COLUMNS.map(c => (
                  <td key={c.key}
                    className={c.key === "domain" ? "td-domain" : c.key === "sw_visits" ? "td-traffic" : c.key === "sw_description" ? "td-desc" : ""}
                    title={c.key === "sw_description" ? cell((r as any)[c.key]) : undefined}>
                    {c.key === "domain"
                      ? <a href={`https://${r.domain}`} target="_blank" rel="noopener">{r.domain}</a>
                      : renderCell(r, c.key)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {filtered.length === 0 && <div className="empty-state">Результатів ще немає або нічого не знайдено</div>}
      </div>
    </div>
  )
}

// ─── App ───────────────────────────────────────────────────────────────────────
type View = "new" | "jobs" | "results" | "explorer"

export default function App() {
  const [view, setView] = useState<View>("jobs")
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null)
  const [dark, setDark] = useState(() => {
    const s = localStorage.getItem("theme")
    if (s) return s === "dark"
    return window.matchMedia("(prefers-color-scheme: dark)").matches
  })

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", dark ? "dark" : "light")
    localStorage.setItem("theme", dark ? "dark" : "light")
  }, [dark])

  const handleJobCreated = (id: string) => { setSelectedJobId(id); setView("results") }
  const handleSelectJob = (id: string) => { setSelectedJobId(id); setView("results") }

  return (
    <div className="app">
      <nav className="nav">
        <div className="nav-brand">
          <div className="nav-logo">◈</div>
          <span className="nav-title">Domain Intel</span>
        </div>
        <div className="nav-links">
          <button className={`nav-link ${view === "new" ? "active" : ""}`} onClick={() => setView("new")}>+ Новий</button>
          <button className={`nav-link ${view === "jobs" ? "active" : ""}`} onClick={() => setView("jobs")}>Job-и</button>
          <button className={`nav-link ${view === "explorer" ? "active" : ""}`} onClick={() => setView("explorer")}>Explorer</button>
        </div>
        <div className="nav-right">
          <button className="theme-toggle" onClick={() => setDark(!dark)} title="Змінити тему">{dark ? "☀" : "☾"}</button>
        </div>
      </nav>
      <main className="main">
        {view === "new" && <NewJobPage onJobCreated={handleJobCreated} />}
        {view === "jobs" && <JobsPage onSelect={handleSelectJob} />}
        {view === "results" && selectedJobId && <ResultsPage jobId={selectedJobId} onBack={() => setView("jobs")} />}
        {view === "explorer" && <ExplorerPage />}
      </main>
    </div>
  )
}
