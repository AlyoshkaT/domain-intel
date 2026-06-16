import { useState, useEffect, useCallback, useRef } from "react"
import ExplorerPage from "./Explorer"
import TechnologiesPage from "./Technologies"
import RedirectsPage from "./Redirects"
import SetupPage from "./Setup"
import { t, type Lang } from "./i18n"
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
function CreditsDialog({ credits, services, domainCount, onConfirm, onCancel, lang }: {
  credits: Credits; services: string[]; domainCount: number
  onConfirm: () => void; onCancel: () => void; lang: Lang
}) {
  const bwNeeded = services.includes("builtwith") ? domainCount : 0
  const swNeeded = services.includes("similarweb") ? domainCount : 0
  const bwOk = credits.builtwith == null || credits.builtwith >= bwNeeded
  const swOk = credits.similarweb == null || credits.similarweb >= swNeeded
  const canProceed = bwOk && swOk
  return (
    <div className="dialog-overlay">
      <div className="dialog">
        <div className="dialog-title">{canProceed ? t('dialog_confirm', lang) : t('dialog_unavailable', lang)}</div>
        <div className="dialog-body">
          {services.includes("builtwith") && (
            <div className={`dialog-row ${!bwOk ? "dialog-row-error" : ""}`}>
              <span className="dialog-service">BuiltWith</span>
              <span className="dialog-stat">{t('dialog_remaining', lang)}: <b>{credits.builtwith?.toLocaleString() ?? "?"}</b></span>
              <span className="dialog-stat">{t('dialog_to_process', lang)}: <b>{bwNeeded.toLocaleString()}</b></span>
              {!bwOk && <span className="dialog-warn">{t('dialog_insufficient', lang)}</span>}
            </div>
          )}
          {services.includes("similarweb") && (
            <div className={`dialog-row ${!swOk ? "dialog-row-error" : ""}`}>
              <span className="dialog-service">SimilarWeb</span>
              <span className="dialog-stat">{t('dialog_remaining', lang)}: <b>{credits.similarweb?.toLocaleString() ?? "?"}</b></span>
              <span className="dialog-stat">{t('dialog_to_process', lang)}: <b>{swNeeded.toLocaleString()}</b></span>
              {!swOk && <span className="dialog-warn">{t('dialog_insufficient', lang)}</span>}
            </div>
          )}
          {!canProceed && <div className="dialog-error-msg">{t('dialog_no_credits', lang)}</div>}
          {canProceed && <div className="dialog-question">{t('dialog_sure', lang)}</div>}
        </div>
        <div className="dialog-actions">
          {canProceed
            ? <><button className="dialog-btn dialog-btn-confirm" onClick={onConfirm}>{t('dialog_yes', lang)}</button>
                <button className="dialog-btn dialog-btn-cancel" onClick={onCancel}>{t('dialog_no', lang)}</button></>
            : <button className="dialog-btn dialog-btn-cancel" onClick={onCancel}>{t('dialog_ok', lang)}</button>}
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
      <button className="credits-refresh" onClick={onRefresh} title="Refresh">↻</button>
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
function JobStatusLine({ job, lang }: { job: Job; lang: Lang }) {
  const done = (job.processed_domains || 0) + (job.failed_domains || 0)
  const total = job.total_domains || 0
  const pct = total > 0 ? Math.round((done / total) * 100) : 0

  const stageLabel = () => {
    if (job.status === "pending") return t('status_pending', lang)
    if (job.status === "running") return t('status_running', lang)(done.toLocaleString(), total.toLocaleString(), String(pct))
    if (job.status === "completed") return t('status_completed', lang)(total.toLocaleString())
    if (job.status === "completed_with_errors") return t('status_errors', lang)(String(job.failed_domains), total.toLocaleString())
    if (job.status === "failed") return `${t('status_failed', lang)}: ${job.error_message || ""}`
    if (job.status === "cancelled") return t('status_cancelled', lang)
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
function NewJobPage({ onJobCreated, lang }: { onJobCreated: (id: string) => void; lang: Lang }) {
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
    else setError(t('new_err_format', lang))
  }
  const handleManualChange = (v: string) => { setManualDomains(v); if (v.trim()) setFile(null) }
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault(); setDragging(false)
    if (e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0])
  }

  const handleSubmitClick = () => {
    if (!file && !manualDomains.trim()) return setError(t('new_err_no_file', lang))
    if (services.length === 0) return setError(t('new_err_no_service', lang))
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
    { id: "similarweb", label: "SimilarWeb", sublabel: t('new_sw_sub', lang) },
    { id: "builtwith",  label: "BuiltWith",  sublabel: t('new_bw_sub', lang) },
    { id: "ai",         label: "Claude AI",  sublabel: t('new_ai_sub', lang) },
  ]

  return (
    <div className="page">
      {showDialog && <CreditsDialog credits={credits} services={services} domainCount={domainCount} onConfirm={handleConfirm} onCancel={() => setShowDialog(false)} lang={lang} />}
      <div className="page-header">
        <h1 className="page-title">{t('new_title', lang)}</h1>
        <CreditsBar credits={credits} onRefresh={async () => { await apiFetch("/api/credits/refresh", { method: "POST" }); await loadCredits() }} />
      </div>
      <div className="card">
        <div className="card-section-title">{t('new_services_title', lang)}</div>
        <div className="services-grid">
          {serviceOptions.map(s => <ServiceToggle key={s.id} id={s.id} label={s.label} sublabel={s.sublabel} checked={services.includes(s.id)} onChange={() => toggle(s.id)} />)}
        </div>
      </div>
      <div className="card">
        <div className="card-section-title">{t('new_manual_title', lang)}</div>
        <textarea className="manual-domains" placeholder={t('new_manual_placeholder', lang)}
          value={manualDomains} onChange={e => handleManualChange(e.target.value)} rows={5} />
        {manualDomains.trim() && <div className="domain-count-hint">{t('new_manual_count', lang)(manualDomains.split(/\r?\n/).filter(s => s.trim()).length)}</div>}
      </div>
      <div className="card">
        <div className="card-section-title">{t('new_file_title', lang)}</div>
        <div className={`dropzone ${dragging ? "dragging" : ""} ${file ? "has-file" : ""}`}
          onDragOver={e => { e.preventDefault(); setDragging(true) }}
          onDragLeave={() => setDragging(false)} onDrop={handleDrop}
          onClick={() => document.getElementById("file-input")?.click()}>
          <input id="file-input" type="file" accept=".csv,.xlsx,.xls,.txt" style={{ display: "none" }}
            onChange={e => e.target.files?.[0] && handleFile(e.target.files[0])} />
          {file
            ? <div className="dropzone-file"><span className="dropzone-icon">📄</span><span className="dropzone-filename">{file.name}</span><span className="dropzone-filesize">{(file.size / 1024).toFixed(0)} KB</span></div>
            : <div className="dropzone-empty"><span className="dropzone-icon">⬆</span><span>{t('new_dropzone', lang)}</span><span className="dropzone-hint">{t('new_dropzone_hint', lang)}</span></div>}
        </div>
      </div>
      <div className="force-refresh-row">
        <button className={`force-toggle ${forceRefresh ? "active" : ""}`} onClick={() => setForceRefresh(!forceRefresh)}>
          <div className="force-toggle-indicator" />
          <div><div className="force-toggle-label">{t('new_ignore_cache', lang)}</div><div className="force-toggle-sub">{t('new_ignore_cache_desc', lang)}</div></div>
        </button>
      </div>
      {error && <div className="error-msg">{error}</div>}
      <button className={`btn-primary ${loading ? "loading" : ""}`} onClick={handleSubmitClick}
        disabled={loading || (!file && !manualDomains.trim()) || services.length === 0}>
        {loading ? <span className="spinner" /> : null}{loading ? t('new_submitting', lang) : t('new_submit', lang)}
      </button>
    </div>
  )
}

// ─── Page: Jobs ────────────────────────────────────────────────────────────────
function JobsPage({ onSelect, can, lang }: { onSelect: (id: string) => void; can: (p: string) => boolean; lang: Lang }) {
  const [jobs, setJobs] = useState<Job[]>([])
  const [loading, setLoading] = useState(true)
  const [acting, setActing] = useState<string | null>(null)
  const load = useCallback(async () => {
    try { const d = await apiFetch("/api/jobs"); setJobs(d.jobs) } catch {} finally { setLoading(false) }
  }, [])
  useEffect(() => { load(); const iv = setInterval(load, 3000); return () => clearInterval(iv) }, [load])
  const hasRunning = jobs.some(j => j.status === "running" || j.status === "pending")

  const handleCancel = async (e: React.MouseEvent, jobId: string) => {
    e.stopPropagation()
    if (!window.confirm(t('jobs_cancel_confirm', lang))) return
    setActing(jobId)
    try { await apiFetch(`/api/jobs/${jobId}/cancel`, { method: "POST" }); await load() } catch {}
    finally { setActing(null) }
  }

  const handleForce = async (e: React.MouseEvent, jobId: string) => {
    e.stopPropagation()
    if (!window.confirm(t('jobs_force_confirm', lang))) return
    setActing(jobId)
    try { await apiFetch(`/api/jobs/${jobId}/force_complete`, { method: "POST" }); await load() } catch {}
    finally { setActing(null) }
  }

  const handleRetry = async (e: React.MouseEvent, jobId: string) => {
    e.stopPropagation()
    setActing(jobId)
    try {
      const d = await apiFetch(`/api/jobs/${jobId}/retry_errors`, { method: "POST" })
      if (!d.count) { alert(t('jobs_retry_none', lang)); return }
      if (!window.confirm(t('jobs_retry_confirm', lang)(d.count))) return
      await load()
    } catch {}
    finally { setActing(null) }
  }

  const handleResume = async (e: React.MouseEvent, jobId: string, notProcessed: number) => {
    e.stopPropagation()
    if (!window.confirm(t('jobs_resume_confirm', lang)(notProcessed, 0))) return
    setActing(jobId)
    try {
      const d = await apiFetch(`/api/jobs/${jobId}/resume`, { method: "POST" })
      if (d.remaining === 0) alert(t('jobs_resume_none', lang))
      await load()
    } catch (err: any) {
      alert(err.message || t('jobs_resume_no_list', lang))
    } finally { setActing(null) }
  }

  return (
    <div className="page">
      <div className="page-header">
        <h1 className="page-title">{t('jobs_title', lang)}</h1>
        {hasRunning && <div className="live-indicator"><span />Live</div>}
      </div>
      {loading ? <div className="loading-center"><span className="spinner-lg" /></div>
        : jobs.length === 0 ? <div className="empty-state">{t('jobs_empty', lang)}</div>
        : (
          <div className="jobs-list">
            {jobs.map(job => {
              const isActive = job.status === "running" || job.status === "pending"
              const isDone = ["completed", "completed_with_errors", "failed", "cancelled"].includes(job.status)
              const isActing = acting === job.job_id
              const totalDone = (job.processed_domains || 0) + (job.failed_domains || 0)
              const notProcessed = (job.total_domains || 0) - totalDone
              const hasErrors = (job.failed_domains || 0) > 0
              return (
                <div key={job.job_id} className="job-card" onClick={() => onSelect(job.job_id)}>
                  <div className="job-card-header">
                    <div>
                      <div className="job-filename">{job.filename}</div>
                      <div className="job-meta">
                        <span className="job-id">{job.job_id.slice(0, 8)}</span>
                        <span className="job-date">{new Date(job.created_at).toLocaleString("uk-UA")}</span>
                      </div>
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      {isActive && (
                        <>
                          <button className="btn-export" disabled={isActing}
                            style={{ color: "var(--danger)", borderColor: "var(--danger)", fontSize: 11 }}
                            onClick={e => handleCancel(e, job.job_id)}>
                            {isActing ? "…" : t('jobs_cancel', lang)}
                          </button>
                          {can("admin") && (
                            <button className="btn-export" disabled={isActing}
                              style={{ color: "var(--accent)", borderColor: "var(--accent)", fontSize: 11 }}
                              onClick={e => handleForce(e, job.job_id)}>
                              {isActing ? "…" : t('jobs_force', lang)}
                            </button>
                          )}
                        </>
                      )}
                      {isDone && notProcessed > 0 && (
                        <button className="btn-export" disabled={isActing}
                          style={{ color: "#22c55e", borderColor: "#22c55e", fontSize: 11 }}
                          onClick={e => handleResume(e, job.job_id, notProcessed)}>
                          {isActing ? "…" : t('jobs_resume', lang)}
                        </button>
                      )}
                      {isDone && hasErrors && (
                        <button className="btn-export" disabled={isActing}
                          style={{ color: "var(--warning, #f59e0b)", borderColor: "var(--warning, #f59e0b)", fontSize: 11 }}
                          onClick={e => handleRetry(e, job.job_id)}>
                          {isActing ? "…" : t('jobs_retry', lang)(job.failed_domains || 0)}
                        </button>
                      )}
                      <StatusBadge status={job.status} />
                    </div>
                  </div>
                  <JobStatusLine job={job} lang={lang} />
                  <ProgressBar value={totalDone} total={job.total_domains || 0} />
                  {isDone && notProcessed > 0 && (
                    <div style={{ fontSize: 11, color: "var(--text-3)", marginTop: 4 }}>
                      ⚠ {t('jobs_unprocessed', lang)(notProcessed)}
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}
    </div>
  )
}

// ─── Page: Results ─────────────────────────────────────────────────────────────
const COLUMNS = [
  { key: "domain", label: "Domain", w: "160px" }, { key: "sw_visits", label: "Traffic", w: "110px" },
  { key: "cms_list", label: "CMS", w: "120px" }, { key: "ems_list", label: "EMS", w: "120px" },
  { key: "ai_category", label: "AI Category", w: "140px" },
  { key: "ai_is_ecommerce", label: "AI Ecomm", w: "90px" }, { key: "ai_industry", label: "AI Industry", w: "150px" },
  { key: "bw_vertical", label: "Industry BW", w: "120px" }, { key: "sw_category", label: "Category SW", w: "160px" },
  { key: "sw_subcategory", label: "Subcategory SW", w: "160px" }, { key: "sw_description", label: "Description", w: "200px" },
  { key: "sw_title", label: "Title", w: "160px" }, { key: "company_name", label: "Company", w: "140px" },
  { key: "sw_primary_region", label: "Region", w: "80px" }, { key: "sw_primary_region_pct", label: "Region %", w: "80px" },
]

function ResultsPage({ jobId, onBack, can, lang }: { jobId: string; onBack: () => void; can: (p: string) => boolean; lang: Lang }) {
  const [job, setJob] = useState<Job | null>(null)
  const [results, setResults] = useState<Result[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState("")
  const [acting, setActing] = useState(false)
  const [sheetUrl, setSheetUrl] = useState<string | null>(null)

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

  // Poll for sheet URL after export is triggered
  const pollSheetUrl = useCallback(() => {
    let attempts = 0
    const iv = setInterval(async () => {
      attempts++
      try {
        const d = await apiFetch(`/api/jobs/${jobId}/export/sheets/url`)
        if (d.url) { setSheetUrl(d.url); clearInterval(iv) }
        if (d.error) { alert(t('jobs_sheets_export_err', lang)(d.error)); clearInterval(iv) }
      } catch {}
      if (attempts >= 20) clearInterval(iv) // stop after ~60s
    }, 3000)
  }, [jobId])

  const handleSheetsExport = useCallback(async (analytics: boolean) => {
    setActing(true)
    try {
      await apiFetch(`/api/jobs/${jobId}/export/sheets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ analytics })   // folder resolved server-side from user profile
      })
      alert(t('jobs_sheets_exporting', lang))
      pollSheetUrl()
    } catch {}
    finally { setActing(false) }
  }, [jobId, lang, pollSheetUrl])

  // Download via fetch→blob (no extra browser tab, like Explorer) instead of window.open
  const downloadFile = useCallback(async (kind: "csv" | "xlsx") => {
    try {
      const res = await fetch(`/api/jobs/${jobId}/export/${kind}`, { credentials: "same-origin" })
      if (!res.ok) throw new Error("export failed")
      const blob = await res.blob()
      const a = document.createElement("a")
      a.href = URL.createObjectURL(blob)
      a.download = `${(job?.filename || "results").replace(/\.[^.]+$/, "")}.${kind}`
      a.click()
      URL.revokeObjectURL(a.href)
    } catch { alert(`${kind.toUpperCase()} export error`) }
  }, [jobId, job])

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
        <div className="back-btn" onClick={onBack}>{t('results_back', lang)}</div>
        <h1 className="page-title">{job?.filename}</h1>
        <div className="page-header-actions">
          {job && <StatusBadge status={job.status} />}
          {job && (job.status === "running" || job.status === "pending") && (
            <>
              <button className="btn-export" disabled={acting}
                style={{ color: "var(--danger)", borderColor: "var(--danger)" }}
                onClick={async () => {
                  if (!window.confirm(t('jobs_cancel_confirm', lang))) return
                  setActing(true)
                  try { await apiFetch(`/api/jobs/${jobId}/cancel`, { method: "POST" }); await loadData() } catch {}
                  finally { setActing(false) }
                }}>
                {acting ? "…" : t('jobs_cancel', lang)}
              </button>
              {can("admin") && (
                <button className="btn-export" disabled={acting}
                  style={{ color: "var(--accent)", borderColor: "var(--accent)" }}
                  onClick={async () => {
                    if (!window.confirm(t('jobs_force_confirm', lang))) return
                    setActing(true)
                    try { await apiFetch(`/api/jobs/${jobId}/force_complete`, { method: "POST" }); await loadData() } catch {}
                    finally { setActing(false) }
                  }}>
                  {acting ? "…" : t('jobs_force', lang)}
                </button>
              )}
            </>
          )}
          {job && ["completed_with_errors","failed","cancelled"].includes(job.status) && (() => {
            const notProcessed = (job.total_domains||0) - (job.processed_domains||0) - (job.failed_domains||0)
            return notProcessed > 0 ? (
              <button className="btn-export" disabled={acting}
                style={{ color: "#22c55e", borderColor: "#22c55e" }}
                onClick={async () => {
                  if (!window.confirm(t('jobs_resume_confirm', lang)(notProcessed, (job.processed_domains||0)+(job.failed_domains||0)))) return
                  setActing(true)
                  try { await apiFetch(`/api/jobs/${jobId}/resume`, { method: "POST" }); await loadData() }
                  catch (err: any) { alert(err.message || t('jobs_resume_no_list', lang)) }
                  finally { setActing(false) }
                }}>
                {acting ? "…" : t('jobs_resume', lang)}
              </button>
            ) : null
          })()}
          {job && ["completed_with_errors","failed","cancelled"].includes(job.status) && (job.failed_domains || 0) > 0 && (
            <button className="btn-export" disabled={acting}
              style={{ color: "var(--warning, #f59e0b)", borderColor: "var(--warning, #f59e0b)" }}
              onClick={async () => {
                setActing(true)
                try {
                  const d = await apiFetch(`/api/jobs/${jobId}/retry_errors`, { method: "POST" })
                  if (!d.count) { alert(t('jobs_retry_none', lang)); return }
                  if (!window.confirm(t('jobs_retry_confirm', lang)(d.count))) return
                  await loadData()
                } catch {}
                finally { setActing(false) }
              }}>
              {acting ? "…" : t('jobs_retry', lang)(job.failed_domains || 0)}
            </button>
          )}
          {can("download") && <button className="btn-export" onClick={() => downloadFile("csv")}>CSV</button>}
          {can("download") && <button className="btn-export" onClick={() => downloadFile("xlsx")}>XLSX</button>}
          {can("sheets") && (
            <button className="btn-export" disabled={acting} onClick={() => handleSheetsExport(false)}>
              {acting ? "…" : t('jobs_sheets', lang)}
            </button>
          )}
          {can("sheets") && (
            <button className="btn-export" disabled={acting}
              onClick={() => handleSheetsExport(true)}
              title="Export with Analytics sheet (4 pivot tables: EMS / CMS / AI Category / oSearch)"
              style={{ borderColor: "#6366f1", color: "#6366f1" }}>
              {acting ? "…" : "↗ Sheets + Analytics"}
            </button>
          )}
          {sheetUrl && (
            <a className="btn-export" href={sheetUrl} target="_blank" rel="noreferrer"
               style={{ color: "#22c55e", borderColor: "#22c55e" }}>
              {t('jobs_sheets_open', lang)}
            </a>
          )}
          {can("jobs") && (
            <button className="btn-export" disabled={acting}
              style={{ color: "var(--accent)", borderColor: "var(--accent)" }}
              onClick={async () => {
                if (!window.confirm(t('jobs_sync_results_confirm', lang))) return
                setActing(true)
                try {
                  const d = await apiFetch(`/api/jobs/${jobId}/sync_from_results`, { method: "POST" })
                  alert(t('jobs_sync_results_done', lang)(d.total ?? 0, d.elapsed ?? 0))
                } catch {}
                finally { setActing(false) }
              }}>
              {acting ? "…" : t('jobs_sync_results', lang)}
            </button>
          )}
        </div>
      </div>
      {job && <JobStatusLine job={job} lang={lang} />}
      {job && (job.status === "running" || job.status === "pending") && (
        <div className="progress-banner">
          <ProgressBar value={(job.processed_domains || 0) + (job.failed_domains || 0)} total={job.total_domains || 0} />
        </div>
      )}
      <div className="filter-row">
        <input className="filter-input" placeholder={t('results_filter_ph', lang)}
          value={filter} onChange={e => setFilter(e.target.value)} />
        <span className="filter-count">{t('results_count', lang)(filtered.length)}</span>
      </div>
      <div className="table-wrap table-fixed-height">
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
        {filtered.length === 0 && <div className="empty-state">{t('results_empty', lang)}</div>}
      </div>
    </div>
  )
}

// ─── App ───────────────────────────────────────────────────────────────────────
// ── BQ Activity Indicator ────────────────────────────────────────────────────
type BqAct = { corp_r: boolean; corp_w: boolean; priv_r: boolean; priv_w: boolean }

function BqIndicator() {
  const [act, setAct] = useState<BqAct>({ corp_r: false, corp_w: false, priv_r: false, priv_w: false })
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    const poll = () => {
      fetch("/api/bq_activity")
        .then(r => r.ok ? r.json() : null)
        .then(d => { if (d) setAct(d) })
        .catch(() => {})
    }
    poll()
    timerRef.current = setInterval(poll, 800)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [])

  const led = (active: boolean, type: "r" | "w") => (
    <span className={`bq-led ${active ? (type === "r" ? "bq-led-r" : "bq-led-w") : ""}`}>
      {type === "r" ? "R" : "W"}
    </span>
  )

  return (
    <div className="bq-indicator" title="BigQuery activity: R=read, W=write">
      <div className="bq-row"><span className="bq-name">corp</span>{led(act.corp_r, "r")}{led(act.corp_w, "w")}</div>
      <div className="bq-row"><span className="bq-name">priv</span>{led(act.priv_r, "r")}{led(act.priv_w, "w")}</div>
    </div>
  )
}

type View = "new" | "jobs" | "results" | "explorer" | "technologies" | "redirects" | "setup"

export default function App() {
  const [view, setView] = useState<View>("new")
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null)
  const [techDomains, setTechDomains] = useState<string[]>([])
  // Always tracks current Explorer filtered domains (updated live as filters change)
  const [explorerFilteredDomains, setExplorerFilteredDomains] = useState<string[]>([])
  const [permissions, setPermissions] = useState<string[]>(["explorer", "jobs", "download", "sheets", "admin"])
  const [dark, setDark] = useState(() => {
    const s = localStorage.getItem("theme")
    if (s) return s === "dark"
    return window.matchMedia("(prefers-color-scheme: dark)").matches
  })
  const [lang, setLang] = useState<Lang>(() => (localStorage.getItem('lang') as Lang) ?? 'en')

  useEffect(() => { localStorage.setItem('lang', lang) }, [lang])

  useEffect(() => {
    apiFetch("/api/me").then(r => {
      if (r.permissions) {
        const perms: string[] = r.permissions
        setPermissions(perms)
        // Redirect to a valid default page
        if (!perms.includes("jobs")) {
          if (perms.includes("explorer")) setView("explorer")
          else if (perms.includes("admin")) setView("setup")
        }
      }
    }).catch(() => {})
  }, [])

  const can = (p: string) => permissions.includes(p)

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
          {can("jobs") && <button className={`nav-link ${view === "new" ? "active" : ""}`} onClick={() => setView("new")}>{t('nav_new', lang)}</button>}
          {can("jobs") && <button className={`nav-link ${view === "jobs" ? "active" : ""}`} onClick={() => setView("jobs")}>{t('nav_jobs', lang)}</button>}
          {can("explorer") && <button className={`nav-link ${view === "explorer" ? "active" : ""}`} onClick={() => setView("explorer")}>{t('nav_explorer', lang)}</button>}
          {can("explorer") && <button className={`nav-link ${view === "technologies" ? "active" : ""}`} onClick={() => { setTechDomains(explorerFilteredDomains); setView("technologies") }}>{t('nav_technologies', lang)}</button>}
          {can("explorer") && <button className={`nav-link ${view === "redirects" ? "active" : ""}`} onClick={() => setView("redirects")}>{t('nav_redirects', lang)}</button>}
          {can("admin") && <button className={`nav-link ${view === "setup" ? "active" : ""}`} onClick={() => setView("setup")}>{t('nav_setup', lang)}</button>}
        </div>
        <div className="nav-right">
          <BqIndicator />
          <button className="theme-toggle" onClick={() => setLang(l => l === 'en' ? 'ua' : 'en')} title="Language">
            {lang === 'en' ? 'UA' : 'EN'}
          </button>
          <button className="theme-toggle" onClick={() => setDark(!dark)} title={t('nav_theme', lang)}>{dark ? "☀" : "☾"}</button>
        </div>
      </nav>
      <main className="main">
        {view === "technologies" && <TechnologiesPage domains={techDomains} onBack={() => setView("explorer")} can={can} lang={lang} />}
        {view === "redirects" && <RedirectsPage lang={lang} />}
        {view === "setup" && <SetupPage lang={lang} />}
        {view === "new" && <NewJobPage onJobCreated={handleJobCreated} lang={lang} />}
        {view === "jobs" && <JobsPage onSelect={handleSelectJob} can={can} lang={lang} />}
        {view === "results" && selectedJobId && <ResultsPage jobId={selectedJobId} onBack={() => setView("jobs")} can={can} lang={lang} />}
        {view === "explorer" && <ExplorerPage
          onFilteredDomainsChange={setExplorerFilteredDomains}
          onNavigateToJobs={() => setView("jobs")}
          can={can} lang={lang} />}
      </main>
    </div>
  )
}
