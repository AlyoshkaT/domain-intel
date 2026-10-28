import { useState, useEffect, useCallback, useMemo, useRef, Fragment } from "react"
import { type Lang } from "./i18n"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

interface DealDetail {
  deal_id: number
  title: string; status: string; value: number; currency: string
  won_time: string | null; lost_time: string | null; add_time: string | null; tariff: string
}
interface StatusRow {
  domain: string
  status_pipedrive: string
  deals_status: string
  main_deal_id: number | null
  paid_m1: boolean; paid_m2: boolean; paid_m3: boolean
  status_fact: string
  risk: string
  last_paid_at: string | null
  last_contact_at: string | null
  won_deals: number; open_deals: number; lost_deals: number; total_deals: number
  total_paid_value: number
  currency: string
  paid_breakdown: string
  org_name: string
  manager: string
  tariff: string
  mrr: number | null
  mrr_monthly: string        // JSON: recent months [{month, mrr, ch:{chan:val}}], recent-first
  mrr_channels: string       // JSON: latest month per-channel {chan: val} (sums to mrr)
  deals_json: string
  computed_at: string
}
interface Point { month: string; won: number; open: number; lost: number }
interface MrrMonth { month: string; mrr: number; ch?: Record<string, number> }

// MRR direction columns, in the order the user asked for. `key` = the corp `Media`
// value stored in pipedrive_mrr_monthly.channel. SMS & Viber are OFF by default:
// they still count in the Σ column (full monthly MRR → status), but a toggle decides
// whether a channel is added to the money chart's summed line.
const CHANNELS: { key: string; label: string; short: string }[] = [
  { key: "ExtraFeatures", label: "CDP module", short: "CDP" },
  { key: "Email", label: "Email", short: "Email" },
  { key: "AppInbox", label: "App Inbox", short: "Inbox" },
  { key: "WebPush", label: "Web Push", short: "WebP" },
  { key: "InApp", label: "In App", short: "InApp" },
  { key: "MobPush", label: "Mob push", short: "MobP" },
  { key: "TelegramBot", label: "Telegram", short: "TG" },
  { key: "Widget", label: "Web widget", short: "Widget" },
  { key: "Sms", label: "SMS", short: "SMS" },
  { key: "Viber", label: "Viber", short: "Viber" },
]
const DEFAULT_OFF = new Set(["Sms", "Viber"])
const CH_KEY = "pd_mrr_channels_on"

const L = (lang: Lang) => ({
  title: lang === "ua" ? "Pipedrive — статус відносин" : "Pipedrive — relationship status",
  subtitle: lang === "ua"
    ? "Статус кожного домену за реальним щомісячним MRR (corpBQ). Pipedrive — довідкові колонки."
    : "Per-domain status from real monthly MRR (corpBQ). Pipedrive data is reference only.",
  fact: lang === "ua" ? "Статус ФАКТ" : "Status FACT",
  allFact: lang === "ua" ? "Усі статуси" : "All statuses",
  risk: lang === "ua" ? "Ризик" : "Risk",
  allRisk: lang === "ua" ? "Усі" : "All",
  period: lang === "ua" ? "Період" : "Period",
  from: lang === "ua" ? "від" : "from",
  to: lang === "ua" ? "до" : "to",
  search: lang === "ua" ? "Пошук за доменом / організацією…" : "Search by domain / organization…",
  sync: lang === "ua" ? "↻ Синхронізувати" : "↻ Sync",
  syncing: lang === "ua" ? "Синхронізація…" : "Syncing…",
  load: lang === "ua" ? "Застосувати" : "Apply",
  loading: lang === "ua" ? "Завантаження…" : "Loading…",
  empty: lang === "ua" ? "Немає даних. Натисни «Синхронізувати», щоб витягти з Pipedrive." : "No data. Click Sync to pull from Pipedrive.",
  noMatch: lang === "ua" ? "Нічого не знайдено за фільтром." : "Nothing matches the filter.",
  count: (a: string, b: string) => lang === "ua" ? `${a} з ${b}` : `${a} of ${b}`,
  chartTitle: lang === "ua" ? "Динаміка Won / Open / Lost (по місяцях)" : "Won / Open / Lost trend (monthly)",
  moneyTitle: lang === "ua" ? "MRR відфільтрованих компаній — сума за місяцями, $ (12 міс)" : "MRR of filtered companies — monthly total, $ (12 mo)",
  moneyNums: lang === "ua" ? "Цифри" : "Numbers",
  boardCount: lang === "ua" ? "Продажі по менеджерах — кількість" : "Sales by manager — count",
  boardMoney: lang === "ua" ? "Продажі по менеджерах — гроші (₴)" : "Sales by manager — money (₴)",
  boardNote: lang === "ua" ? "сума оплат, переважно UAH" : "paid total, mostly UAH",
  boardClients: lang === "ua" ? "клієнтів" : "clients",
  asOf: lang === "ua" ? "статус станом на" : "status as of",
  thDomain: lang === "ua" ? "Домен" : "Domain",
  thOrg: lang === "ua" ? "Організація" : "Organization",
  thManager: "MANAGER",
  thNum: "DEALS №",
  thTariff: "TARIFF",
  thMrr: lang === "ua" ? "Σ MRR (−2/−1/міс)" : "Σ MRR (−2/−1/mo)",
  chByChannel: lang === "ua" ? "MRR по каналах (поточний міс, $)" : "MRR by channel (current mo, $)",
  chTogglesNote: lang === "ua"
    ? "Канали на графіку (сірі — не входять у суму; Σ у таблиці завжди повна)"
    : "Channels on the chart (greyed = excluded from the sum; the table Σ is always full)",
  thPd: "DEALS Status",
  dealsStatus: "STATUS DEALS",
  thFact: lang === "ua" ? "ФАКТ" : "FACT",
  thRisk: lang === "ua" ? "Ризик" : "Risk",
  thLast: lang === "ua" ? "Остання оплата" : "Last paid",
  thContact: "LAST CONTACT",
  thValue: lang === "ua" ? "Сума оплат" : "Paid total",
  months: lang === "ua" ? "−2 / −1 / Цей міс" : "−2 / −1 / This mo",
  dealsHdr: lang === "ua" ? "Угоди домену:" : "Deals for this domain:",
})

const FACT_COLORS: Record<string, string> = { Won: "#22c55e", Open: "#3b82f6", Lost: "#ef4444", "—": "#6b7280" }
const RISK_COLORS: Record<string, string> = { Alarm: "#f59e0b", Churn: "#a855f7" }
const KIND_COLORS = { won: "#22c55e", open: "#3b82f6", lost: "#ef4444" }

// Compact money formatter: 14_350_000 → "14.3M", 1_200 → "1.2K".
function fmtMoney(n: number): string {
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M"
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K"
  return Math.round(n).toLocaleString()
}

function Badge({ text, color }: { text: string; color: string }) {
  return <span className="service-tag" style={{ background: color + "22", color, border: `1px solid ${color}44` }}>{text}</span>
}

// Simple grouped bar chart (SVG) — Won/Open/Lost per month.
function TrendChart({ data, title }: { data: Point[]; title: string }) {
  if (!data.length) return null
  const W = Math.max(640, data.length * 56), H = 220, pad = 28, base = H - pad
  const max = Math.max(1, ...data.flatMap(d => [d.won, d.open, d.lost]))
  const slot = (W - pad * 2) / data.length
  const bw = Math.min(12, slot / 4)
  const y = (v: number) => base - (v / max) * (base - 10)
  const kinds: (keyof typeof KIND_COLORS)[] = ["won", "open", "lost"]
  return (
    <div className="card" style={{ marginBottom: 12, overflowX: "auto" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
        <span style={{ fontSize: 12, fontWeight: 600, color: "var(--text-2)" }}>{title}</span>
        <span style={{ display: "flex", gap: 12, fontSize: 11 }}>
          {kinds.map(k => <span key={k} style={{ color: KIND_COLORS[k] }}>■ {k}</span>)}
        </span>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxHeight: 240 }}>
        <line x1={pad} y1={base} x2={W - pad} y2={base} stroke="var(--border)" />
        {data.map((d, i) => {
          const x0 = pad + i * slot + slot / 2
          return (
            <g key={d.month}>
              {kinds.map((k, j) => {
                const bx = x0 + (j - 1) * (bw + 2) - bw / 2
                return <rect key={k} x={bx} y={y(d[k])} width={bw} height={base - y(d[k])}
                  fill={KIND_COLORS[k]} rx={1}><title>{`${d.month} ${k}: ${d[k]}`}</title></rect>
              })}
              <text x={x0} y={H - 8} textAnchor="middle" fontSize={9} fill="var(--text-3)">{d.month.slice(2)}</text>
            </g>
          )
        })}
      </svg>
    </div>
  )
}

// Simple money bar chart (SVG) — total MRR of the filtered companies per month.
const MONEY_NUMS_KEY = "pd_money_nums"

function MoneyChart({ data, title, numsLabel }: { data: { month: string; mrr: number; domains: number }[]; title: string; numsLabel: string }) {
  // Show the MRR value above every bar at once (toggle), so the whole picture is
  // readable without hovering each bar. Choice is remembered per-browser.
  const [showNums, setShowNums] = useState<boolean>(() => {
    try { return localStorage.getItem(MONEY_NUMS_KEY) === "1" } catch { return false }
  })
  useEffect(() => { try { localStorage.setItem(MONEY_NUMS_KEY, showNums ? "1" : "0") } catch { /* ignore */ } }, [showNums])
  if (!data.length) return null
  const W = Math.max(640, data.length * 44), H = 200, pad = 28, base = H - pad
  const max = Math.max(1, ...data.map(d => d.mrr))
  const slot = (W - pad * 2) / data.length
  const bw = Math.min(22, slot * 0.6)
  const y = (v: number) => base - (v / max) * (base - 10)
  return (
    <div className="card" style={{ marginBottom: 12, overflowX: "auto" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, marginBottom: 6 }}>
        <div style={{ fontSize: 12, fontWeight: 600, color: "var(--text-2)" }}>{title}</div>
        <button onClick={() => setShowNums(v => !v)} title={numsLabel}
          style={{ flexShrink: 0, cursor: "pointer", fontSize: 11, padding: "2px 8px", borderRadius: 4,
            border: `1px solid ${showNums ? "#22c55e66" : "var(--border)"}`,
            background: showNums ? "#22c55e22" : "transparent",
            color: showNums ? "#16a34a" : "var(--text-3)", whiteSpace: "nowrap" }}>
          {showNums ? "●" : "○"} {numsLabel}
        </button>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxHeight: 220 }}>
        <line x1={pad} y1={base} x2={W - pad} y2={base} stroke="var(--border)" />
        {data.map((d) => {
          const x0 = pad + data.indexOf(d) * slot + slot / 2
          return (
            <g key={d.month}>
              <rect x={x0 - bw / 2} y={y(d.mrr)} width={bw} height={base - y(d.mrr)} fill="#22c55e" rx={2}>
                <title>{`${d.month}: ${d.mrr.toLocaleString()} · ${d.domains}`}</title>
              </rect>
              {showNums && d.mrr > 0 && (
                <text x={x0} y={Math.max(8, y(d.mrr) - 3)} textAnchor="middle" fontSize={8} fontWeight={600} fill="var(--text-2)">{fmtMoney(d.mrr)}</text>
              )}
              <text x={x0} y={H - 8} textAnchor="middle" fontSize={9} fill="var(--text-3)">{d.month.slice(2)}</text>
            </g>
          )
        })}
      </svg>
    </div>
  )
}

// One ranked, clickable manager board (by a chosen metric). Bars are relative
// to the top manager; clicking a row toggles the table filter.
function ManagerBoard({ title, note, stats, metric, fmt, active, onPick }: {
  title: string; note?: string
  stats: { manager: string; count: number; money: number; clients: number }[]
  metric: "count" | "money"; fmt: (n: number) => string
  active: string; onPick: (m: string) => void
}) {
  const ranked = [...stats].filter(s => s[metric] > 0).sort((a, b) => b[metric] - a[metric])
  const max = ranked.length ? ranked[0][metric] : 1
  const color = metric === "money" ? "#22c55e" : "#3b82f6"
  return (
    <div className="card" style={{ flex: 1, minWidth: 280 }}>
      <div style={{ fontSize: 12, fontWeight: 600, color: "var(--text-2)", marginBottom: 2 }}>{title}</div>
      {note && <div style={{ fontSize: 10, color: "var(--text-3)", marginBottom: 8 }}>{note}</div>}
      <div style={{ maxHeight: 230, overflowY: "auto", display: "flex", flexDirection: "column", gap: 3 }}>
        {ranked.map(s => {
          const on = active === s.manager
          return (
            <div key={s.manager} onClick={() => onPick(on ? "" : s.manager)}
              title={`${s.clients} ${"clients"}`}
              style={{ cursor: "pointer", padding: "3px 6px", borderRadius: 4,
                background: on ? color + "22" : "transparent", border: `1px solid ${on ? color + "66" : "transparent"}` }}>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, marginBottom: 2 }}>
                <span style={{ color: "var(--text-1)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: 170 }}>{s.manager}</span>
                <span style={{ fontFamily: "var(--mono)", color: "var(--text-2)" }}>{fmt(s[metric])}</span>
              </div>
              <div style={{ height: 5, background: "var(--bg-2)", borderRadius: 3, overflow: "hidden" }}>
                <div style={{ width: `${Math.max(2, (s[metric] / max) * 100)}%`, height: "100%", background: color }} />
              </div>
            </div>
          )
        })}
        {!ranked.length && <div style={{ fontSize: 12, color: "var(--text-3)" }}>—</div>}
      </div>
    </div>
  )
}

export default function PipedrivePage({ lang, can }: { lang: Lang; can: (p: string) => boolean }) {
  const tx = L(lang)
  const today = new Date().toISOString().slice(0, 10)
  const yearAgo = new Date(Date.now() - 365 * 864e5).toISOString().slice(0, 10)

  const [rows, setRows] = useState<StatusRow[]>([])
  const [company, setCompany] = useState("")
  const [series, setSeries] = useState<Point[]>([])
  const [loading, setLoading] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [error, setError] = useState("")
  const [factFilter, setFactFilter] = useState("")
  const [managerFilter, setManagerFilter] = useState("")
  const [dealsFilter, setDealsFilter] = useState("")
  const [riskFilter, setRiskFilter] = useState("")
  const [search, setSearch] = useState("")
  const [dateFrom, setDateFrom] = useState(yearAgo)
  const [dateTo, setDateTo] = useState(today)
  const [expanded, setExpanded] = useState<string | null>(null)
  const [sortCol, setSortCol] = useState<string>("last_contact_at")
  const [sortDir, setSortDir] = useState<1 | -1>(-1)  // newest contact first → oldest

  // Which channels are summed into the money chart (SMS/Viber off by default).
  // Persisted per-browser; does NOT affect the table Σ column or FACT/RISK status.
  const [chOn, setChOn] = useState<Record<string, boolean>>(() => {
    const d: Record<string, boolean> = {}
    for (const c of CHANNELS) d[c.key] = !DEFAULT_OFF.has(c.key)
    try { const s = JSON.parse(localStorage.getItem(CH_KEY) || "null"); if (s && typeof s === "object") return { ...d, ...s } } catch { /* ignore */ }
    return d
  })
  useEffect(() => { try { localStorage.setItem(CH_KEY, JSON.stringify(chOn)) } catch { /* ignore */ } }, [chOn])
  const enabledCh = useMemo(() => CHANNELS.filter(c => chOn[c.key]).map(c => c.key), [chOn])

  // Trend chart is server-computed; reload it for the current period + manager.
  const loadSeries = useCallback(async (manager: string) => {
    const mp = manager ? `&manager=${encodeURIComponent(manager)}` : ""
    const ts = await apiFetch(`/api/pipedrive/timeseries?date_from=${dateFrom}&date_to=${dateTo}${mp}`)
    setSeries(ts.series || [])
  }, [dateFrom, dateTo])

  // usePeriod=false → read the precomputed pipedrive_status table (fast, all domains,
  // status as of the last sync). usePeriod=true → recompute status as of the chosen
  // date / period (slower; triggered explicitly by the Apply button).
  const load = useCallback(async (usePeriod = false) => {
    setLoading(true); setError("")
    try {
      const p = new URLSearchParams()
      if (usePeriod) {
        if (dateTo) p.set("as_of", dateTo)
        if (dateFrom) p.set("date_from", dateFrom)
      }
      const [s] = await Promise.all([
        apiFetch(`/api/pipedrive/status?${p.toString()}`),
        loadSeries(managerFilter),
      ])
      setRows(s.rows || [])
      setCompany(s.company || "")
    } catch (e: any) { setError(e.message) } finally { setLoading(false) }
  }, [dateFrom, dateTo, managerFilter, loadSeries])

  useEffect(() => { load(false) }, [])  // initial: fast precomputed read; period applies via the button

  // Rebuild the trend chart whenever the selected manager changes (skip mount).
  const mounted = useRef(false)
  useEffect(() => {
    if (!mounted.current) { mounted.current = true; return }
    loadSeries(managerFilter).catch(() => {})
  }, [managerFilter, loadSeries])

  const sync = useCallback(async () => {
    setSyncing(true); setError("")
    try {
      const r = await apiFetch("/api/pipedrive/sync", { method: "POST" })
      if (r.status === "error") setError(r.error || "sync failed")
      await load()
    } catch (e: any) { setError(e.message) } finally { setSyncing(false) }
  }, [load])

  const summary = useMemo(() => {
    const s: Record<string, number> = { Won: 0, Lost: 0, "—": 0, Alarm: 0, Churn: 0 }
    for (const r of rows) { if (r.status_fact in s) s[r.status_fact]++; if (r.risk) s[r.risk]++ }
    return s
  }, [rows])

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    const out = rows.filter(r =>
      (!factFilter || r.status_fact === factFilter) &&
      (!dealsFilter || r.deals_status === dealsFilter) &&
      (!riskFilter || r.risk === riskFilter) &&
      (!managerFilter || r.manager === managerFilter) &&
      (!q || r.domain.includes(q) || (r.org_name || "").toLowerCase().includes(q))
    )
    const cmp = (a: any, b: any) => {
      const va = a[sortCol], vb = b[sortCol]
      if (va == null && vb == null) return 0
      if (va == null) return 1
      if (vb == null) return -1
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * sortDir
      return String(va).localeCompare(String(vb)) * sortDir
    }
    return [...out].sort(cmp)
  }, [rows, factFilter, dealsFilter, riskFilter, managerFilter, search, sortCol, sortDir])

  // Money chart: SUM of real MRR per month over the currently-filtered companies,
  // counting only the ENABLED channels (client-side from each row's recent-12-month
  // mrr_monthly). Reflects filters + channel toggles live. Falls back to the monthly
  // total when a row has no channel split yet (data from before the per-channel sync).
  const mrrSeries = useMemo(() => {
    const on = new Set(enabledCh)
    const sums = new Map<string, { mrr: number; n: number }>()
    for (const r of filtered) {
      let arr: MrrMonth[] = []
      try { arr = JSON.parse(r.mrr_monthly || "[]") } catch { /* ignore */ }
      for (const m of arr) {
        let v = 0
        if (m.ch) { for (const k of on) v += m.ch[k] || 0 }
        else v = m.mrr || 0
        if (!v) continue
        const e = sums.get(m.month) || { mrr: 0, n: 0 }
        e.mrr += v; e.n += 1; sums.set(m.month, e)
      }
    }
    return [...sums.entries()].sort((a, b) => (a[0] < b[0] ? -1 : 1))
      .map(([month, v]) => ({ month, mrr: Math.round(v.mrr), domains: v.n }))
  }, [filtered, enabledCh])

  // Per-channel Σ of the latest month over the filtered set — shown on the toggle chips.
  const channelTotals = useMemo(() => {
    const t: Record<string, number> = {}
    for (const r of filtered) {
      let ch: Record<string, number> = {}
      try { ch = JSON.parse(r.mrr_channels || "{}") } catch { /* ignore */ }
      for (const k in ch) t[k] = (t[k] || 0) + (ch[k] || 0)
    }
    return t
  }, [filtered])

  // Sales by manager (count = paid deals, money = Σ paid value), over the loaded
  // period. Used by the two interactive boards; clicking filters the table.
  const managerStats = useMemo(() => {
    const m = new Map<string, { count: number; money: number; clients: number }>()
    for (const r of rows) {
      if (!r.manager) continue
      const e = m.get(r.manager) || { count: 0, money: 0, clients: 0 }
      e.count += r.won_deals || 0
      e.money += r.total_paid_value || 0
      e.clients += 1
      m.set(r.manager, e)
    }
    return [...m.entries()].map(([manager, v]) => ({ manager, ...v }))
  }, [rows])

  const toggleSort = (col: string) =>
    sortCol === col ? setSortDir(d => (d === 1 ? -1 : 1)) : (setSortCol(col), setSortDir(1))

  const exportCSV = useCallback(() => {
    const cols = ["domain", "org_name", "manager", "main_deal_id", "tariff", "deals_status", "status_pipedrive", "status_fact", "risk",
      "paid_m1", "paid_m2", "paid_m3", "last_contact_at", "last_paid_at", "won_deals", "open_deals",
      "lost_deals", "total_deals", "total_paid_value", "currency", "paid_breakdown", "mrr", "mrr_channels", "mrr_monthly", "computed_at"]
    const esc = (v: any) => `"${String(v ?? "").replace(/"/g, '""')}"`
    const csv = [cols.join(","), ...filtered.map(r => cols.map(c => esc((r as any)[c])).join(","))].join("\n")
    const a = document.createElement("a")
    a.href = URL.createObjectURL(new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8" }))
    a.download = `pipedrive_status_${today}.csv`
    a.click()
  }, [filtered, today])

  const selStyle: React.CSSProperties = {
    padding: "5px 8px", background: "var(--bg-2)", border: "1px solid var(--border)",
    borderRadius: "var(--radius)", color: "var(--text)", fontSize: 12, outline: "none", minWidth: 110,
  }
  const dot = (v: boolean) => <span style={{ display: "inline-block", width: 11, height: 11, borderRadius: "50%", background: v ? "#22c55e" : "var(--border)", margin: "0 2px" }} />
  const mrr3 = (j: string) => {
    let arr: MrrMonth[] = []
    try { arr = JSON.parse(j || "[]") } catch { /* ignore */ }
    if (!arr.length) return <span style={{ color: "var(--text-3)" }}>—</span>
    const v = (i: number) => arr[i]?.mrr || 0
    const cell = (val: number, main: boolean) => (
      <span style={{ color: val ? (main ? "#22c55e" : "var(--text-2)") : "var(--text-3)", fontWeight: main ? 700 : 400 }}>
        {val ? Math.round(val).toLocaleString() : "·"}
      </span>
    )
    const sep = <span style={{ color: "var(--text-3)" }}> / </span>
    return (
      <span title={arr.slice(0, 3).reverse().map(m => `${m.month}: ${Math.round(m.mrr).toLocaleString()}`).join("   ")}>
        {cell(v(2), false)}{sep}{cell(v(1), false)}{sep}{cell(v(0), true)}
      </span>
    )
  }
  // One <td> per channel (latest-month value); greyed/dimmed when that channel is
  // toggled off. Informational — the Σ column still carries the full monthly MRR.
  const chCells = (chJson: string) => {
    let ch: Record<string, number> = {}
    try { ch = JSON.parse(chJson || "{}") } catch { /* ignore */ }
    return CHANNELS.map(c => {
      const v = ch[c.key] || 0
      const on = chOn[c.key]
      return (
        <td key={c.key} style={{ textAlign: "right", fontFamily: "var(--mono)", fontSize: 11, whiteSpace: "nowrap",
          color: v && on ? "var(--text-2)" : "var(--text-3)", opacity: on ? 1 : 0.5 }}>
          {v ? Math.round(v).toLocaleString() : "·"}
        </td>
      )
    })
  }
  const dealUrl = (id: number | null) => (id && company) ? `https://${company}.pipedrive.com/deal/${id}` : ""
  const factOf = (s: string) => s === "won" ? "Won" : s === "open" ? "Open" : "Lost"
  const dealNum = (id: number | null) => {
    if (!id) return <span style={{ color: "var(--text-3)" }}>—</span>
    const u = dealUrl(id)
    return u
      ? <a href={u} target="_blank" rel="noopener" onClick={e => e.stopPropagation()} style={{ fontFamily: "var(--mono)", fontSize: 11 }}>#{id}</a>
      : <span style={{ fontFamily: "var(--mono)", fontSize: 11 }}>#{id}</span>
  }

  return (
    <div className="page page-wide">
      <div className="page-header">
        <h1 className="page-title">{tx.title}</h1>
        <span style={{ fontSize: 12, color: "var(--text-3)" }}>
          {tx.subtitle}{dateTo !== today ? ` · ${tx.asOf} ${dateTo}` : ""}
        </span>
      </div>

      {/* Summary cards */}
      <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 12 }}>
        {(["Won", "Lost", "—"] as const).map(k => (
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

      {/* Trend chart (deal counts) */}
      <TrendChart data={series} title={tx.chartTitle + (managerFilter ? ` · 👤 ${managerFilter}` : "")} />
      {/* Channel toggles — decide which directions sum into the money chart below */}
      <div className="card" style={{ marginBottom: 12, padding: "8px 12px" }}>
        <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 6 }}>{tx.chTogglesNote}</div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
          {CHANNELS.map(c => {
            const on = chOn[c.key]
            const tot = channelTotals[c.key] || 0
            return (
              <button key={c.key} onClick={() => setChOn(s => ({ ...s, [c.key]: !s[c.key] }))}
                title={c.label + (tot ? ` · ${fmtMoney(tot)}$` : "")}
                style={{
                  cursor: "pointer", fontSize: 11, padding: "3px 9px", borderRadius: 14, whiteSpace: "nowrap",
                  background: on ? "rgba(34,197,94,0.16)" : "var(--bg-2)",
                  color: on ? "#22c55e" : "var(--text-3)",
                  border: `1px solid ${on ? "rgba(34,197,94,0.45)" : "var(--border)"}`,
                }}>
                {on ? "●" : "○"} {c.label}{tot ? ` · ${fmtMoney(tot)}` : ""}
              </button>
            )
          })}
        </div>
      </div>
      {/* Money chart (real MRR of the filtered companies, enabled channels only) */}
      <MoneyChart data={mrrSeries} title={tx.moneyTitle} numsLabel={tx.moneyNums} />

      {/* Sales-by-manager boards (interactive → filter the table) */}
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 12 }}>
        <ManagerBoard title={tx.boardCount} stats={managerStats} metric="count"
          active={managerFilter} onPick={setManagerFilter} fmt={n => n.toLocaleString()} />
        <ManagerBoard title={tx.boardMoney} note={tx.boardNote} stats={managerStats} metric="money"
          active={managerFilter} onPick={setManagerFilter} fmt={fmtMoney} />
      </div>

      {/* Filters + period */}
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="tech-filters" style={{ flexWrap: "wrap", gap: 10, alignItems: "flex-end" }}>
          <div className="tech-filter-group">
            <label className="tech-filter-label">{tx.period} ({tx.from})</label>
            <input type="date" style={selStyle} value={dateFrom} onChange={e => setDateFrom(e.target.value)} />
          </div>
          <div className="tech-filter-group">
            <label className="tech-filter-label">{tx.to}</label>
            <input type="date" style={selStyle} value={dateTo} onChange={e => setDateTo(e.target.value)} />
          </div>
          <div className="tech-filter-group">
            <label className="tech-filter-label">{tx.dealsStatus}</label>
            <select style={selStyle} value={dealsFilter} onChange={e => setDealsFilter(e.target.value)}>
              <option value="">{tx.allFact}</option>
              {["won", "open", "lost"].map(f => <option key={f} value={f}>{f}</option>)}
            </select>
          </div>
          <div className="tech-filter-group">
            <label className="tech-filter-label">{tx.fact}</label>
            <select style={selStyle} value={factFilter} onChange={e => setFactFilter(e.target.value)}>
              <option value="">{tx.allFact}</option>
              {["Won", "Lost", "—"].map(f => <option key={f} value={f}>{f}</option>)}
            </select>
          </div>
          <div className="tech-filter-group">
            <label className="tech-filter-label">{tx.risk}</label>
            <select style={selStyle} value={riskFilter} onChange={e => setRiskFilter(e.target.value)}>
              <option value="">{tx.allRisk}</option>
              {["Alarm", "Churn"].map(f => <option key={f} value={f}>{f}</option>)}
            </select>
          </div>
          <button className="btn-search" onClick={() => load(true)} disabled={loading}>{loading ? tx.loading : "🔄"} {tx.load}</button>
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
          {managerFilter && (
            <button onClick={() => setManagerFilter("")} title="clear manager filter"
              style={{ fontSize: 12, padding: "3px 10px", borderRadius: 14, cursor: "pointer",
                background: "rgba(59,130,246,0.18)", color: "#60a5fa", border: "1px solid rgba(59,130,246,0.4)" }}>
              👤 {managerFilter} ✕
            </button>
          )}
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
                {([
                  ["domain", tx.thDomain, "left"],
                  ["org_name", tx.thOrg, "left"],
                  ["manager", tx.thManager, "left"],
                  ["main_deal_id", tx.thNum, "left"],
                  ["tariff", tx.thTariff, "left"],
                  ["deals_status", tx.thPd, "left"],
                  ["status_fact", tx.thFact, "left"],
                  ["risk", tx.thRisk, "left"],
                  ["paid_m1", tx.months, "left"],
                  ["last_contact_at", tx.thContact, "left"],
                  ["last_paid_at", tx.thLast, "left"],
                  ["total_paid_value", tx.thValue, "right"],
                ] as [string, string, string][]).map(([col, label, align]) => (
                  <th key={col} onClick={() => toggleSort(col)}
                    title={col === "paid_m1" ? tx.months : undefined}
                    style={{ cursor: "pointer", userSelect: "none", textAlign: align as any, whiteSpace: "nowrap" }}>
                    {label}{sortCol === col ? (sortDir === 1 ? " ▲" : " ▼") : ""}
                  </th>
                ))}
                {/* MRR direction columns (current month) — short labels; full name on
                    hover, dimmed when the channel is toggled off on the chart. */}
                {CHANNELS.map(c => (
                  <th key={c.key} title={tx.chByChannel + " · " + c.label}
                    style={{ textAlign: "right", whiteSpace: "nowrap",
                      color: chOn[c.key] ? "var(--text-2)" : "var(--text-3)", opacity: chOn[c.key] ? 1 : 0.55 }}>
                    {c.short}
                  </th>
                ))}
                {/* Σ total column = full monthly MRR (drives the 3-mo window + status) */}
                <th onClick={() => toggleSort("mrr")}
                  style={{ cursor: "pointer", userSelect: "none", textAlign: "right", whiteSpace: "nowrap" }}>
                  {tx.thMrr}{sortCol === "mrr" ? (sortDir === 1 ? " ▲" : " ▼") : ""}
                </th>
              </tr>
            </thead>
            <tbody>
              {filtered.slice(0, 800).map((r) => {
                const multi = r.total_deals > 1
                const isOpen = expanded === r.domain
                let deals: DealDetail[] = []
                if (isOpen) { try { deals = JSON.parse(r.deals_json || "[]") } catch { /* ignore */ } }
                return (
                  <Fragment key={r.domain}>
                    <tr style={multi ? { cursor: "pointer" } : undefined}
                      onClick={multi ? () => setExpanded(isOpen ? null : r.domain) : undefined}>
                      <td className="td-domain">
                        {multi && <span style={{ color: "var(--text-3)", marginRight: 4 }}>{isOpen ? "▾" : "▸"}</span>}
                        <a href={`https://${r.domain}`} target="_blank" rel="noopener" onClick={e => e.stopPropagation()}>{r.domain}</a>
                      </td>
                      <td style={{ fontSize: 12, color: "var(--text-2)" }}>{r.org_name || "—"}</td>
                      <td style={{ fontSize: 12, color: "var(--text-2)", whiteSpace: "nowrap" }}>{r.manager || "—"}</td>
                      <td style={{ whiteSpace: "nowrap" }}>
                        {dealNum(r.main_deal_id)}
                        {multi && <span style={{ color: "var(--text-3)", fontSize: 10, marginLeft: 4 }}>+{r.total_deals - 1}</span>}
                      </td>
                      <td style={{ fontSize: 11, color: "var(--text-2)", maxWidth: 180, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }} title={r.tariff}>{r.tariff || "—"}</td>
                      <td><Badge text={r.deals_status} color={FACT_COLORS[factOf(r.deals_status)] || "#6b7280"} /></td>
                      <td><Badge text={r.status_fact} color={FACT_COLORS[r.status_fact] || "#6b7280"} /></td>
                      <td>{r.risk ? <Badge text={r.risk} color={RISK_COLORS[r.risk] || "#6b7280"} /> : ""}</td>
                      <td style={{ whiteSpace: "nowrap" }}>{dot(r.paid_m3)}{dot(r.paid_m2)}{dot(r.paid_m1)}</td>
                      <td style={{ fontFamily: "var(--mono)", fontSize: 11, whiteSpace: "nowrap" }}>{r.last_contact_at || "—"}</td>
                      <td style={{ fontFamily: "var(--mono)", fontSize: 11, whiteSpace: "nowrap" }}>{r.last_paid_at || "—"}</td>
                      <td style={{ textAlign: "right", fontFamily: "var(--mono)", fontSize: 11, whiteSpace: "nowrap" }}
                        title={r.paid_breakdown && r.paid_breakdown.includes(",") ? r.paid_breakdown : undefined}>
                        {r.total_paid_value ? r.total_paid_value.toLocaleString() : "0"} {r.currency}
                        {r.paid_breakdown && r.paid_breakdown.includes(",") && <span style={{ color: "var(--text-3)", marginLeft: 3 }}>＊</span>}
                      </td>
                      {chCells(r.mrr_channels)}
                      <td style={{ textAlign: "right", fontFamily: "var(--mono)", fontSize: 11, whiteSpace: "nowrap" }}>
                        {mrr3(r.mrr_monthly)}
                      </td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={13 + CHANNELS.length} style={{ background: "var(--bg-2)", padding: "8px 16px" }}>
                          <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 4 }}>{tx.dealsHdr}</div>
                          {deals.map((d, j) => (
                            <div key={j} style={{ display: "flex", gap: 10, alignItems: "center", fontSize: 12, padding: "2px 0" }}>
                              <span style={{ minWidth: 64 }}>{dealNum(d.deal_id)}</span>
                              <Badge text={d.status} color={FACT_COLORS[factOf(d.status)]} />
                              <span style={{ flex: 1 }}>{d.title || "—"}</span>
                              <span style={{ fontFamily: "var(--mono)", color: "var(--text-3)" }}>
                                {d.status === "won" ? `💰 ${d.won_time || ""}`
                                  : d.status === "lost" ? `✕ ${d.lost_time || ""}`
                                  : `↗ ${d.add_time || ""}`}
                              </span>
                              <span style={{ fontFamily: "var(--mono)", color: d.status === "won" ? "var(--text)" : "var(--text-3)" }}>
                                {d.value ? d.value.toLocaleString() : "0"} {d.currency}
                              </span>
                            </div>
                          ))}
                        </td>
                      </tr>
                    )}
                  </Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {!loading && filtered.length > 800 && (
        <div style={{ fontSize: 11, color: "var(--text-3)", marginTop: 6 }}>
          {lang === "ua"
            ? `Показано перші 800 із ${filtered.length.toLocaleString()} — уточни фільтр або вивантаж CSV (усі рядки).`
            : `Showing first 800 of ${filtered.length.toLocaleString()} — refine the filter or export CSV (all rows).`}
        </div>
      )}
      {!loading && rows.length === 0 && <div className="empty-state">{tx.empty}</div>}
      {!loading && rows.length > 0 && filtered.length === 0 && <div className="empty-state">{tx.noMatch}</div>}
    </div>
  )
}
