import { useState, useEffect, useCallback, useRef } from "react"
import Dashboard, { TRAFFIC_GROUPS } from "./Dashboard"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

// ─── Types ────────────────────────────────────────────────────────────────────
interface FilterValue { value: string; count: number }
type TextFilterType  = "all" | "in" | "contains" | "not_contains" | "empty" | "not_empty"
type NumFilterType   = "all" | "gt" | "lt" | "between"
type MultiFilterType = "all" | "in" | "not_in" | "empty" | "not_empty"

interface TextFilter  { type: TextFilterType;  value: string; selected: string[] }
interface NumFilter   { type: NumFilterType;   value: string; min: string; max: string }
interface MultiFilter { type: MultiFilterType; selected: string[]; search: string }

type FilterState = {
  domain: TextFilter; cms_list: MultiFilter
  osearch: MultiFilter; ems_list: MultiFilter; ai_category: MultiFilter
  ai_is_ecommerce: MultiFilter; sw_category: MultiFilter; sw_primary_region: MultiFilter
  sw_visits: NumFilter; sw_primary_region_pct: NumFilter
}

const defaultText  = (): TextFilter  => ({ type: "all", value: "", selected: [] })
const defaultNum   = (): NumFilter   => ({ type: "all", value: "", min: "", max: "" })
const defaultMulti = (): MultiFilter => ({ type: "all", selected: [], search: "" })
const defaultFilters = (): FilterState => ({
  domain: defaultText(), cms_list: defaultMulti(),
  osearch: defaultMulti(), ems_list: defaultMulti(), ai_category: defaultMulti(),
  ai_is_ecommerce: defaultMulti(), sw_category: defaultMulti(), sw_primary_region: defaultMulti(),
  sw_visits: defaultNum(), sw_primary_region_pct: defaultNum(),
})

const MULTI_FIELDS = ["cms_list","osearch","ems_list","ai_category","ai_is_ecommerce","sw_category","sw_primary_region"]

// ─── Domain filter with multi-select ─────────────────────────────────────────
function DomainFilter({ filter, allValues, onChange }: {
  filter: TextFilter; allValues: FilterValue[]; onChange: (f: TextFilter) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const h = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false) }
    document.addEventListener("mousedown", h); return () => document.removeEventListener("mousedown", h)
  }, [])

  const suggestions = allValues.filter(v => filter.value && v.value.toLowerCase().includes(filter.value.toLowerCase())).slice(0, 15)

  const toggleDomain = (v: string) => {
    const sel = filter.selected.includes(v) ? filter.selected.filter(s => s !== v) : [...filter.selected, v]
    onChange({ ...filter, selected: sel, type: sel.length > 0 ? "in" : "all" })
  }

  return (
    <div className="flt-text" ref={ref}>
      <select className="flt-select-sm" value={filter.type}
        onChange={e => onChange({ ...filter, type: e.target.value as TextFilterType, selected: [], value: "" })}>
        <option value="all">Всі</option>
        <option value="in">Мультивибір</option>
        <option value="contains">Містить</option>
        <option value="not_contains">Не містить</option>
        <option value="empty">Порожнє</option>
        <option value="not_empty">Не порожнє</option>
      </select>

      {filter.type === "in" && (
        <div className="flt-dropdown-wrap">
          <button className={`flt-dropdown-btn ${filter.selected.length > 0 ? "active" : ""}`} onClick={() => setOpen(!open)}>
            <span>{filter.selected.length === 0 ? "Вибрати домени..." : `Вибрано: ${filter.selected.length}`}</span>
            <span className="flt-chevron">{open ? "▴" : "▾"}</span>
          </button>
          {open && (
            <div className="flt-dropdown">
              <input className="flt-search-input" placeholder="Пошук домену..."
                value={filter.value} onChange={e => onChange({ ...filter, value: e.target.value })} autoFocus />
              {filter.selected.length > 0 && (
                <div className="flt-clear-sel" onClick={() => onChange({ ...filter, selected: [], type: "all", value: "" })}>
                  ✕ Скинути вибір ({filter.selected.length})
                </div>
              )}
              <div className="flt-options">
                {filter.selected.map(v => (
                  <label key={v} className="flt-option flt-option-selected">
                    <input type="checkbox" checked onChange={() => toggleDomain(v)} />
                    <span className="flt-option-text">{v}</span>
                  </label>
                ))}
                {suggestions.filter(v => !filter.selected.includes(v.value)).map(v => (
                  <label key={v.value} className="flt-option">
                    <input type="checkbox" checked={false} onChange={() => toggleDomain(v.value)} />
                    <span className="flt-option-text">{v.value}</span>
                    <span className="flt-option-count">{v.count}</span>
                  </label>
                ))}
                {!filter.value && <div className="flt-loading">Введіть домен для пошуку</div>}
              </div>
            </div>
          )}
          {filter.selected.length > 0 && (
            <div className="flt-selected-tags">
              {filter.selected.slice(0, 2).map(v => <span key={v} className="flt-tag" onClick={() => toggleDomain(v)}>{v} ✕</span>)}
              {filter.selected.length > 2 && <span className="flt-tag flt-tag-more">+{filter.selected.length - 2}</span>}
            </div>
          )}
        </div>
      )}

      {(filter.type === "contains" || filter.type === "not_contains") && (
        <div style={{ position: "relative" }}>
          <input className="flt-num-input" placeholder="Текст або домен..."
            value={filter.value}
            onChange={e => { onChange({ ...filter, value: e.target.value }); setOpen(true) }}
            onFocus={() => setOpen(true)} />
          {open && filter.value && suggestions.length > 0 && (
            <div className="flt-dropdown" style={{ position: "absolute", top: "calc(100% + 4px)", left: 0, right: 0, zIndex: 300 }}>
              <div className="flt-options">
                {suggestions.map(v => (
                  <div key={v.value} className="flt-option" onClick={() => { onChange({ ...filter, value: v.value }); setOpen(false) }}>
                    <span className="flt-option-text">{v.value}</span>
                    <span className="flt-option-count">{v.count}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ─── Multi-select ─────────────────────────────────────────────────────────────
function MultiSelect({ field, filter, allValues, onChange }: {
  field: string; filter: MultiFilter; allValues: FilterValue[]; onChange: (f: MultiFilter) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const h = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false) }
    document.addEventListener("mousedown", h); return () => document.removeEventListener("mousedown", h)
  }, [])
  const filtered = allValues.filter(v => !filter.search || v.value.toLowerCase().includes(filter.search.toLowerCase()))
  const toggle = (v: string) => {
    const sel = filter.selected.includes(v) ? filter.selected.filter(s => s !== v) : [...filter.selected, v]
    onChange({ ...filter, selected: sel, type: sel.length > 0 ? "in" : "all" })
  }
  return (
    <div className="flt-multi" ref={ref}>
      <select className="flt-select-sm" value={filter.type}
        onChange={e => onChange({ ...filter, type: e.target.value as MultiFilterType, selected: [] })}>
        <option value="all">Всі</option><option value="in">Включити</option>
        <option value="not_in">Виключити</option><option value="empty">Порожнє</option>
        <option value="not_empty">Не порожнє</option>
      </select>
      {(filter.type === "in" || filter.type === "not_in") && (
        <div className="flt-dropdown-wrap">
          <button className={`flt-dropdown-btn ${filter.selected.length > 0 ? "active" : ""}`} onClick={() => setOpen(!open)}>
            <span>{filter.selected.length === 0 ? `Всі (${allValues.length})` : `Вибрано: ${filter.selected.length}`}</span>
            <span className="flt-chevron">{open ? "▴" : "▾"}</span>
          </button>
          {open && (
            <div className="flt-dropdown">
              <input className="flt-search-input" placeholder="Пошук..." value={filter.search}
                onChange={e => onChange({ ...filter, search: e.target.value })} autoFocus />
              {filter.selected.length > 0 && (
                <div className="flt-clear-sel" onClick={() => onChange({ ...filter, selected: [], type: "all", search: "" })}>
                  ✕ Скинути ({filter.selected.length})
                </div>
              )}
              <div className="flt-options">
                {filtered.length === 0 && <div className="flt-loading">Нічого</div>}
                {filtered.map(v => (
                  <label key={v.value} className="flt-option">
                    <input type="checkbox" checked={filter.selected.includes(v.value)} onChange={() => toggle(v.value)} />
                    <span className="flt-option-text">{v.value}</span>
                    <span className="flt-option-count">{v.count}</span>
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
      {filter.selected.length > 0 && (
        <div className="flt-selected-tags">
          {filter.selected.slice(0, 2).map(v => <span key={v} className="flt-tag" onClick={() => toggle(v)}>{v} ✕</span>)}
          {filter.selected.length > 2 && <span className="flt-tag flt-tag-more">+{filter.selected.length - 2}</span>}
        </div>
      )}
    </div>
  )
}

// ─── Numeric filter ───────────────────────────────────────────────────────────
function NumericFilter({ filter, onChange }: { filter: NumFilter; onChange: (f: NumFilter) => void }) {
  return (
    <div className="flt-num">
      <select className="flt-select-sm" value={filter.type} onChange={e => onChange({ ...filter, type: e.target.value as NumFilterType })}>
        <option value="all">Всі</option><option value="gt">Більше</option>
        <option value="lt">Менше</option><option value="between">Від — До</option>
      </select>
      {(filter.type === "gt" || filter.type === "lt") && (
        <input className="flt-num-input" type="number" placeholder="Значення"
          value={filter.value} onChange={e => onChange({ ...filter, value: e.target.value })} />
      )}
      {filter.type === "between" && (
        <div className="flt-between">
          <input className="flt-num-input" type="number" placeholder="Від" value={filter.min} onChange={e => onChange({ ...filter, min: e.target.value })} />
          <span className="flt-between-sep">—</span>
          <input className="flt-num-input" type="number" placeholder="До" value={filter.max} onChange={e => onChange({ ...filter, max: e.target.value })} />
        </div>
      )}
    </div>
  )
}

// ─── Sync button ──────────────────────────────────────────────────────────────
function SyncButton({ onSync }: { onSync: () => void }) {
  const [status, setStatus] = useState<any>({})
  const [syncing, setSyncing] = useState(false)
  const load = useCallback(async () => {
    try { const s = await apiFetch("/api/explore/sync/status"); setStatus(s); if (!s.running) setSyncing(false) } catch {}
  }, [])
  useEffect(() => { load(); const iv = setInterval(load, 2000); return () => clearInterval(iv) }, [load])
  const isRunning = syncing || status.running
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 3 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        {status.last_sync && !isRunning && (
          <span style={{ fontSize: 10, color: "var(--text-3)" }}>
            {new Date(status.last_sync).toLocaleString("uk-UA")} · {status.total_domains?.toLocaleString()} доменів
          </span>
        )}
        <button className="flt-reset-btn" disabled={isRunning}
          onClick={async () => { setSyncing(true); await apiFetch("/api/explore/refresh", { method: "POST" }); setTimeout(onSync, 5000) }}>
          {isRunning ? "⏳" : "↻"} {isRunning ? "Синхронізація..." : "Синхронізувати БД"}
        </button>
      </div>
      {isRunning && status.progress && <span style={{ fontSize: 10, color: "var(--accent)", fontFamily: "var(--mono)" }}>{status.progress}</span>}
      {status.error && <span style={{ fontSize: 10, color: "var(--red)" }}>❌ {status.error.slice(0, 80)}</span>}
    </div>
  )
}

// ─── Filter panel ─────────────────────────────────────────────────────────────
function FilterPanel({ filters, fieldValues, onChange, onSearch, loading, activeCount }: {
  filters: FilterState; fieldValues: Record<string, FilterValue[]>
  onChange: (f: FilterState) => void; onSearch: () => void; loading: boolean; activeCount: number
}) {
  const upd = (key: keyof FilterState, val: any) => onChange({ ...filters, [key]: val })
  const sections = [
    { key: "domain",                 label: "Domain",      type: "domain" },
    { key: "sw_visits",              label: "Traffic",     type: "num"    },
    { key: "cms_list",               label: "CMS",         type: "multi"  },
    { key: "osearch",                label: "oSearch",     type: "multi"  },
    { key: "ems_list",               label: "EMS",         type: "multi"  },
    { key: "ai_category",            label: "AI Category", type: "multi"  },
    { key: "ai_is_ecommerce",        label: "AI Ecomm",    type: "multi"  },
    { key: "sw_category",            label: "Category SW", type: "multi"  },
    { key: "sw_primary_region",      label: "Region",      type: "multi"  },
    { key: "sw_primary_region_pct",  label: "Region %",    type: "num"    },
  ] as const
  return (
    <div className="filter-panel">
      <div className="filter-panel-header">
        <span className="filter-panel-title">Фільтри</span>
        <button className="flt-reset-btn" onClick={() => onChange(defaultFilters())}>Скинути</button>
      </div>
      {sections.map(s => (
        <div key={s.key} className="filter-section">
          <div className="filter-section-label">{s.label}</div>
          {s.type === "domain" && <DomainFilter filter={filters.domain} allValues={fieldValues.domain || []} onChange={v => upd("domain", v)} />}
          {s.type === "num"    && <NumericFilter filter={filters[s.key] as NumFilter} onChange={v => upd(s.key, v)} />}
          {s.type === "multi"  && <MultiSelect field={s.key} filter={filters[s.key] as MultiFilter} allValues={fieldValues[s.key] || []} onChange={v => upd(s.key, v)} />}
        </div>
      ))}
      <button className="btn-primary explorer-search-btn" onClick={onSearch} disabled={loading}>
        {loading ? <span className="spinner" /> : "🔍"}
        {loading ? "Пошук..." : "Застосувати"}
        {activeCount > 0 && <span className="flt-count-badge">{activeCount}</span>}
      </button>
    </div>
  )
}

function cell(v?: string | null) { return v && v.trim() ? v : "—" }

interface ExploreResult {
  domain: string; sw_visits?: number; cms_list?: string; wcms_name?: string
  osearch?: string; ems_list?: string; ai_category?: string; ai_is_ecommerce?: string
  ai_industry?: string; sw_category?: string; sw_subcategory?: string
  sw_description?: string; sw_title?: string; company_name?: string
  sw_primary_region?: string; sw_primary_region_pct?: number
}

// ─── Main Explorer ─────────────────────────────────────────────────────────────
export default function ExplorerPage({ onViewTechnologies, onNavigateToJobs }: { onViewTechnologies?: (domains: string[]) => void; onNavigateToJobs?: () => void }) {
  const [filters, setFilters] = useState<FilterState>(defaultFilters())
  const [fieldValues, setFieldValues] = useState<Record<string, FilterValue[]>>({})
  const [allResults, setAllResults] = useState<ExploreResult[]>([])
  const [results, setResults] = useState<ExploreResult[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [stats, setStats] = useState<any>({})
  const [offset, setOffset] = useState(0)
  const [jumpPage, setJumpPage] = useState("")
  const PAGE = 100

  // All profiles in memory for reactive filter counting
  const [baseProfiles, setBaseProfiles] = useState<ExploreResult[]>([])

  // Compute available filter values from currently filtered profiles
  const computeFieldValues = useCallback((filtered: ExploreResult[]) => {
    const counts: Record<string, Map<string, number>> = {}
    const fields = [...MULTI_FIELDS, "domain"]
    fields.forEach(f => { counts[f] = new Map() })

    for (const p of filtered) {
      for (const field of fields) {
        const val = (p as any)[field]
        if (val && String(val).trim()) {
          counts[field].set(String(val), (counts[field].get(String(val)) || 0) + 1)
        }
      }
    }

    const newValues: Record<string, FilterValue[]> = {}
    for (const field of fields) {
      newValues[field] = [...counts[field].entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([value, count]) => ({ value, count }))
    }
    setFieldValues(newValues)
  }, [])

  useEffect(() => {
    apiFetch("/api/explore/stats").then(setStats).catch(() => {})
    doSearch(defaultFilters(), 0)
  }, [])

  // Recompute filter values when allResults change (reactive)
  useEffect(() => {
    if (allResults.length > 0) {
      computeFieldValues(allResults)
      if (baseProfiles.length === 0) setBaseProfiles(allResults)
    }
  }, [allResults])

  const buildPayload = (f: FilterState, off: number, limit = PAGE) => {
    const af: any = {}
    // Domain
    if (f.domain.type === "in" && f.domain.selected.length > 0)
      af.domain = { type: "in", values: f.domain.selected }
    else if ((f.domain.type === "contains" || f.domain.type === "not_contains") && f.domain.value)
      af.domain = { type: f.domain.type, value: f.domain.value }
    else if (f.domain.type === "empty" || f.domain.type === "not_empty")
      af.domain = { type: f.domain.type }
    // Multi
    for (const field of MULTI_FIELDS) {
      const flt = f[field as keyof FilterState] as MultiFilter
      if (flt.type === "empty" || flt.type === "not_empty") af[field] = { type: flt.type }
      else if ((flt.type === "in" || flt.type === "not_in") && flt.selected.length > 0)
        af[field] = { type: flt.type, values: flt.selected }
    }
    // Numeric
    for (const field of ["sw_visits", "sw_primary_region_pct"] as (keyof FilterState)[]) {
      const flt = f[field] as NumFilter
      if (flt.type === "gt" && flt.value) af[field] = { type: "gt", value: parseFloat(flt.value) }
      else if (flt.type === "lt" && flt.value) af[field] = { type: "lt", value: parseFloat(flt.value) }
      else if (flt.type === "between" && flt.min && flt.max)
        af[field] = { type: "between", min: parseFloat(flt.min), max: parseFloat(flt.max) }
    }
    return { filters: af, limit, offset: off }
  }

  const doSearch = async (f: FilterState, off: number) => {
    setLoading(true)
    try {
      const [allData, pageData] = await Promise.all([
        apiFetch("/api/explore/search", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(buildPayload(f, 0, 200000)) }),
        apiFetch("/api/explore/search", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(buildPayload(f, off, PAGE)) }),
      ])
      setAllResults(allData.results || [])
      setResults(pageData.results || [])
      setTotal(pageData.total || 0)
    } catch { setAllResults([]); setResults([]); setTotal(0) }
    finally { setLoading(false) }
  }

  const handleSearch = () => { setOffset(0); doSearch(filters, 0) }
  const handlePrev = () => { const o = Math.max(0, offset - PAGE); setOffset(o); doSearch(filters, o) }
  const handleNext = () => { const o = offset + PAGE; setOffset(o); doSearch(filters, o) }
  const handleJump = () => {
    const row = parseInt(jumpPage)
    if (!isNaN(row) && row > 0) {
      const o = Math.floor((row - 1) / PAGE) * PAGE
      setOffset(o); doSearch(filters, o); setJumpPage("")
    }
  }

  const exportCSV = () => {
    const cols = ["domain","sw_visits","cms_list","wcms_name","osearch","ems_list","ai_category","ai_is_ecommerce","ai_industry","sw_category","sw_subcategory","sw_primary_region","sw_primary_region_pct","sw_description","sw_title","company_name"]
    const rows = allResults.map(r => cols.map(h => { const v = (r as any)[h]; return v != null ? `"${String(v).replace(/"/g, '""')}"` : "" }).join(","))
    const csv = [cols.join(","), ...rows].join("\n")
    const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8" }))
    a.download = `explorer_${new Date().toISOString().slice(0, 10)}.csv`; a.click()
  }

  const exportXLSX = async () => {
    try {
      const res = await fetch("/api/explore/export/xlsx", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ results: allResults }) })
      if (!res.ok) throw new Error("Export failed")
      const blob = await res.blob()
      const a = document.createElement("a"); a.href = URL.createObjectURL(blob)
      a.download = `explorer_${new Date().toISOString().slice(0, 10)}.xlsx`; a.click()
    } catch { alert("XLSX export error") }
  }

  const activeCount = Object.values(filters).filter((f: any) =>
    (f.type && f.type !== "all") || (f.selected && f.selected.length > 0) || (f.value && f.value.trim())
  ).length

  const [refreshServices, setRefreshServices] = useState<string[]>([])
  const [refreshing, setRefreshing] = useState(false)
  const [refreshMsg, setRefreshMsg] = useState("")

  const toggleRefreshService = (s: string) =>
    setRefreshServices(p => p.includes(s) ? p.filter(x => x !== s) : [...p, s])

  // ─── Dashboard → filter ───────────────────────────────────────────────────
  const handleDashboardFilter = useCallback((field: string, label: string) => {
    const next = { ...filters }

    if (field === "sw_visits") {
      // Traffic groups → numeric filter
      if (label === "(порожнє)") return
      const group = TRAFFIC_GROUPS.find(g => g.label === label)
      if (!group) return
      const idx = TRAFFIC_GROUPS.indexOf(group)
      const nextGroup = TRAFFIC_GROUPS[idx - 1] // higher threshold group
      if (label === "Nano <10k") {
        next.sw_visits = { type: "lt", value: "10000", min: "", max: "" }
      } else if (!nextGroup) {
        next.sw_visits = { type: "gt", value: String(group.min), min: "", max: "" }
      } else {
        next.sw_visits = { type: "between", value: "", min: String(group.min), max: String(nextGroup.min) }
      }
    } else if (label === "(порожнє)") {
      const cur = next[field as keyof FilterState] as MultiFilter
      next[field as keyof FilterState] = { ...cur, type: "empty", selected: [] } as any
    } else {
      // Multi-select toggle
      const cur = next[field as keyof FilterState] as MultiFilter
      const sel = cur.selected.includes(label)
        ? cur.selected.filter(s => s !== label)
        : [...cur.selected, label]
      next[field as keyof FilterState] = { ...cur, selected: sel, type: sel.length > 0 ? "in" : "all" } as any
    }

    setFilters(next)
    setOffset(0)
    doSearch(next, 0)
  }, [filters, doSearch])

  const handleForceRefresh = async () => {
    if (!refreshServices.length || !allResults.length) return
    setRefreshing(true); setRefreshMsg("")
    try {
      const domains = allResults.map(r => r.domain).join("\n")
      const file = new File([domains], "domains_refresh.txt", { type: "text/plain" })
      const fd = new FormData()
      fd.append("file", file)
      fd.append("services", JSON.stringify(refreshServices))
      fd.append("force_refresh", "true")
      const res = await fetch("/api/jobs", { method: "POST", body: fd })
      if (!res.ok) throw new Error("Failed")
      setRefreshMsg(`Запущено оновлення ${allResults.length.toLocaleString()} доменів (${refreshServices.join(", ")})`)
      setTimeout(() => { if (onNavigateToJobs) onNavigateToJobs() }, 1500)
    } catch { setRefreshMsg("Помилка запуску") }
    finally { setRefreshing(false) }
  }

  return (
    <div className="explorer-layout">
      <aside className="explorer-sidebar">
        <FilterPanel filters={filters} fieldValues={fieldValues} onChange={setFilters}
          onSearch={handleSearch} loading={loading} activeCount={activeCount} />
      </aside>

      <main className="explorer-main">
        {/* Stats + Force Refresh */}
        <div style={{ display: "flex", alignItems: "stretch", gap: 8, marginBottom: 12 }}>
          <div className="stats-grid" style={{ flex: 1, margin: 0 }}>
            {[
              { label: "Доменів",    value: stats.total_domains },
              { label: "З трафіком", value: stats.with_traffic },
              { label: "З CMS",      value: stats.with_cms },
              { label: "З EMS",      value: stats.with_ems },
              { label: "З AI",       value: stats.with_ai },
            ].map(s => (
              <div key={s.label} className="stat-card">
                <div className="stat-label">{s.label}</div>
                <div className="stat-value">{s.value?.toLocaleString() || "—"}</div>
              </div>
            ))}
          </div>
          {allResults.length > 0 && (
            <div className="stat-card force-refresh-card">
              <div className="stat-label">↻ Оновити КЕШ</div>
              <div className="force-refresh-row">
                <div className="gran-btns">
                  {[{id:"builtwith",label:"BW"},{id:"similarweb",label:"SW"},{id:"ai",label:"AI"}].map(s => (
                    <button key={s.id}
                      className={`gran-btn${refreshServices.includes(s.id) ? " active" : ""}`}
                      onClick={() => toggleRefreshService(s.id)}>{s.label}</button>
                  ))}
                </div>
                <button className="btn-export"
                  onClick={handleForceRefresh}
                  disabled={refreshing || refreshServices.length === 0}>
                  {refreshing ? "⏳" : "↻"} {allResults.length.toLocaleString()}
                </button>
              </div>
              {refreshMsg && <div style={{ fontSize: 10, color: "var(--accent)", marginTop: 2 }}>{refreshMsg}</div>}
            </div>
          )}
        </div>

        {/* Dashboards */}
        {allResults.length > 0 && <Dashboard profiles={allResults} onFilter={handleDashboardFilter} />}

        {/* Results header */}
        <div className="explorer-results-header" style={{ marginTop: 16 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span className="explorer-total">
              {loading ? "Завантаження..." : `Знайдено: ${total.toLocaleString()} доменів`}
            </span>
            <SyncButton onSync={() => doSearch(filters, 0)} />
          </div>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            {allResults.length > 0 && (
              <>
                <button className="btn-export" onClick={exportCSV}>↓ CSV</button>
                <button className="btn-export" onClick={exportXLSX}>↓ XLSX</button>
                {onViewTechnologies && allResults.length > 0 && (
                  <button className="btn-export" style={{background:"var(--accent)",color:"white",borderColor:"var(--accent)"}}
                    onClick={() => onViewTechnologies(allResults.map(r => r.domain))}>
                    📊 Технології →
                  </button>
                )}
              </>
            )}
            {total > PAGE && (
              <div className="pagination">
                <button className="page-btn" onClick={handlePrev} disabled={offset === 0 || loading}>←</button>
                <span className="page-info">{offset + 1}–{Math.min(offset + PAGE, total)} / {total.toLocaleString()}</span>
                <button className="page-btn" onClick={handleNext} disabled={offset + PAGE >= total || loading}>→</button>
                <span style={{ color: "var(--text-3)", fontSize: 11 }}>…</span>
                <input className="page-jump-input" type="number" placeholder="№" value={jumpPage}
                  onChange={e => setJumpPage(e.target.value)}
                  onKeyDown={e => e.key === "Enter" && handleJump()} />
                <button className="page-btn" onClick={handleJump} disabled={!jumpPage}>↵</button>
              </div>
            )}
          </div>
        </div>

        {/* Table with fixed height and vertical scroll */}
        {loading && <div className="loading-center"><span className="spinner-lg" /></div>}
        {!loading && results.length > 0 && (
          <div className="table-wrap table-fixed-height" style={{ marginTop: 8 }}>
            <table className="results-table">
              <thead>
                <tr>
                  <th style={{ width: 40 }}>#</th>
                  <th>Domain</th><th>Traffic</th><th>CMS</th>
                  <th>oSearch</th><th>EMS</th><th>AI Category</th><th>AI Ecomm</th>
                  <th>AI Industry</th><th>Category SW</th><th>Subcategory</th>
                  <th>Description</th><th>Region</th><th>Region %</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r, i) => (
                  <tr key={`${r.domain}-${i}`}>
                    <td className="td-num">{offset + i + 1}</td>
                    <td className="td-domain"><a href={`https://${r.domain}`} target="_blank" rel="noopener">{r.domain}</a></td>
                    <td className="td-traffic">{r.sw_visits ? r.sw_visits.toLocaleString("en-US") : "—"}</td>
                    <td>{cell(r.cms_list)}</td>
                    <td>{cell(r.osearch)}</td><td>{cell(r.ems_list)}</td>
                    <td>{cell(r.ai_category)}</td><td>{cell(r.ai_is_ecommerce)}</td>
                    <td>{cell(r.ai_industry)}</td><td>{cell(r.sw_category)}</td>
                    <td>{cell(r.sw_subcategory)}</td>
                    <td className="td-desc" title={r.sw_description || ""}>{cell(r.sw_description)}</td>
                    <td>{cell(r.sw_primary_region)}</td>
                    <td>{r.sw_primary_region_pct != null ? `${r.sw_primary_region_pct}%` : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {!loading && results.length === 0 && <div className="empty-state">Нічого не знайдено.</div>}
      </main>
    </div>
  )
}
