import { useState, useEffect, useMemo, useCallback, Fragment } from "react"
import { type Lang } from "./i18n"
import { CHANNELS, DEFAULT_OFF, fmtMoney, MoneyChart } from "./Pipedrive"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

interface MrrMonth { month: string; mrr: number; ch?: Record<string, number> }
interface Client {
  partner: string
  organisation_id: number
  organisation: string
  domain: string
  types: string
  mrr: number
  mrr_channels: string
  mrr_monthly: string   // JSON recent-first [{month, mrr, ch:{chan:val}}]
}

const CH_KEY = "partnership_channels_on"

const L = (lang: Lang) => ({
  title: lang === "ua" ? "Партнерства — агенції та їхні клієнти" : "Partnership — agencies & their clients",
  subtitle: lang === "ua"
    ? "Приналежність клієнтів до агенцій + помісячний MRR ($). Покриття — клієнти з білінговим MRR (майстер-реєстр у Google-Таблиці, частина без MRR може бути відсутня)."
    : "Agency ↔ client membership + monthly MRR ($). Coverage = clients with billed MRR (master registry lives in a Google Sheet; a few non-billing clients may be missing).",
  search: lang === "ua" ? "Пошук: клієнт / домен / агенція…" : "Search: client / domain / agency…",
  clearSearch: lang === "ua" ? "очистити пошук" : "clear search",
  period: lang === "ua" ? "Період" : "Period",
  addAgency: lang === "ua" ? "+ агенція" : "+ agency",
  addClient: lang === "ua" ? "+ клієнт" : "+ client",
  clearAll: lang === "ua" ? "Очистити все" : "Clear all",
  moneyTitle: lang === "ua" ? "MRR партнерських клієнтів — сума за місяцями, $" : "Partner clients MRR — monthly total, $",
  moneyNums: lang === "ua" ? "Цифри" : "Numbers",
  chTogglesNote: lang === "ua"
    ? "Канали, що входять у суми (сірі — виключені)"
    : "Channels counted in the sums (greyed = excluded)",
  thClient: lang === "ua" ? "Клієнт" : "Client",
  thType: lang === "ua" ? "Тип" : "Type",
  thSum: lang === "ua" ? "Σ за період" : "Σ period",
  grand: lang === "ua" ? "Σ Разом" : "Σ Total",
  clients: lang === "ua" ? "клієнтів" : "clients",
  empty: lang === "ua" ? "Немає даних. Адмін має натиснути «Синхронізувати Partnership» у Setup." : "No data. An admin must run the Partnership sync in Setup.",
  noMatch: lang === "ua" ? "Нічого не знайдено за фільтром." : "Nothing matches the filter.",
  openSite: lang === "ua" ? "Відкрити сайт" : "Open site",
  filterByAgency: lang === "ua" ? "Клік — лише ця агенція" : "Click to show only this agency",
  srcSheet: lang === "ua" ? "Джерело: повний реєстр (Google-Таблиця)" : "Source: full registry (Google Sheet)",
  srcHier: lang === "ua"
    ? "Джерело: corpBQ — білінговий реєстр + ієрархія організацій (нативно, включно з неплатними клієнтами). Для агенцій, що не є окремою орг (напр. OL Agency), список = платні клієнти."
    : "Source: corpBQ — billed registry + org hierarchy (native, incl. non-billing clients). For agencies not modeled as an org (e.g. OL Agency), the list is billed clients only.",
  srcBilled: lang === "ua"
    ? "Джерело: лише клієнти з білінговим MRR."
    : "Source: billed clients only.",
})

const srcText = (s: string, tx: ReturnType<typeof L>) =>
  s === "sheet" ? tx.srcSheet : s === "commissions+hierarchy" ? tx.srcHier : tx.srcBilled
const srcGood = (s: string) => s === "sheet" || s === "commissions+hierarchy"

// Compact money for the many month cells; "·" for an empty cell.
function cell(v: number) {
  return v > 0 ? fmtMoney(v) : "·"
}

export default function PartnershipPage({ lang }: { lang: Lang }) {
  const tx = L(lang)
  const [clients, setClients] = useState<Client[]>([])
  const [months, setMonths] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")
  const [source, setSource] = useState("")
  const [search, setSearch] = useState("")
  const [agencySel, setAgencySel] = useState<Set<string>>(new Set())      // selected agencies
  const [clientSel, setClientSel] = useState<Set<number>>(new Set())      // selected org ids
  const [fromIdx, setFromIdx] = useState(0)
  const [toIdx, setToIdx] = useState(0)

  const toggleAgency = (name: string) => setAgencySel(s => {
    const n = new Set(s); n.has(name) ? n.delete(name) : n.add(name); return n
  })
  const toggleClient = (id: number) => setClientSel(s => {
    const n = new Set(s); n.has(id) ? n.delete(id) : n.add(id); return n
  })

  // Channel toggles (same set as Pipedrive; SMS/Viber off by default), per-browser.
  const [chOn, setChOn] = useState<Record<string, boolean>>(() => {
    const d: Record<string, boolean> = {}
    for (const c of CHANNELS) d[c.key] = !DEFAULT_OFF.has(c.key)
    try { const s = JSON.parse(localStorage.getItem(CH_KEY) || "null"); if (s && typeof s === "object") return { ...d, ...s } } catch { /* ignore */ }
    return d
  })
  useEffect(() => { try { localStorage.setItem(CH_KEY, JSON.stringify(chOn)) } catch { /* ignore */ } }, [chOn])
  const enabledCh = useMemo(() => new Set(CHANNELS.filter(c => chOn[c.key]).map(c => c.key)), [chOn])

  useEffect(() => {
    apiFetch("/api/partnership/data")
      .then(d => {
        setClients(d.clients || [])
        setMonths(d.months || [])
        setSource(d.source || "")
        if ((d.months || []).length) { setFromIdx(0); setToIdx(d.months.length - 1) }
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  // Pre-parse each client's monthly map once: month → {total, ch}.
  const parsed = useMemo(() => clients.map(c => {
    let arr: MrrMonth[] = []
    try { arr = JSON.parse(c.mrr_monthly || "[]") } catch { /* ignore */ }
    const map: Record<string, MrrMonth> = {}
    for (const m of arr) map[m.month] = m
    return { c, map }
  }), [clients])

  const periodMonths = useMemo(() => {
    if (!months.length) return []
    const a = Math.min(fromIdx, toIdx), b = Math.max(fromIdx, toIdx)
    return months.slice(a, b + 1)
  }, [months, fromIdx, toIdx])

  // Value of one client in one month, counting only enabled channels.
  const valOf = useCallback((m: MrrMonth | undefined) => {
    if (!m) return 0
    if (m.ch) { let v = 0; for (const k in m.ch) if (enabledCh.has(k)) v += m.ch[k] || 0; return v }
    return m.mrr || 0
  }, [enabledCh])

  // Dropdown sources: agencies (with client counts) and the flat client list.
  const agencyList = useMemo(() => {
    const m = new Map<string, number>()
    for (const c of clients) m.set(c.partner, (m.get(c.partner) || 0) + 1)
    return [...m.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
  }, [clients])
  const clientList = useMemo(() =>
    [...clients].sort((a, b) =>
      (a.organisation || a.domain || "").localeCompare(b.organisation || b.domain || "")),
    [clients])
  const pickedClients = useMemo(() =>
    clients.filter(c => clientSel.has(c.organisation_id)),
    [clients, clientSel])

  // Agencies that have at least one of their own clients individually picked —
  // for those, the client pick DRILLS DOWN (narrows the agency to the picked
  // clients) instead of being a no-op union member.
  const drilledAgencies = useMemo(
    () => new Set(pickedClients.map(c => c.partner)),
    [pickedClients])

  // Filter → group by agency. Each group carries per-month sums + a period total.
  // Selection semantics (search always ANDs on top):
  //   • a selected agency shows ALL its clients …
  //   • … unless one of its clients is individually picked, then it narrows to
  //     just the picked client(s) (drill-down);
  //   • a client picked from a non-selected agency is added on its own (union).
  const groups = useMemo(() => {
    const q = search.trim().toLowerCase()
    const noSel = agencySel.size === 0 && clientSel.size === 0
    const rows = parsed.filter(({ c }) =>
      (noSel
        || clientSel.has(c.organisation_id)
        || (agencySel.has(c.partner) && !drilledAgencies.has(c.partner))) &&
      (!q || c.organisation.toLowerCase().includes(q) || c.domain.includes(q) || (c.partner || "").toLowerCase().includes(q))
    )
    const byAgency = new Map<string, { c: Client; map: Record<string, MrrMonth>; perMonth: Record<string, number>; sum: number }[]>()
    for (const { c, map } of rows) {
      const perMonth: Record<string, number> = {}
      let sum = 0
      for (const mo of periodMonths) { const v = valOf(map[mo]); perMonth[mo] = v; sum += v }
      if (!byAgency.has(c.partner)) byAgency.set(c.partner, [])
      byAgency.get(c.partner)!.push({ c, map, perMonth, sum })
    }
    const out = [...byAgency.entries()].map(([partner, cls]) => {
      const perMonth: Record<string, number> = {}
      let sum = 0
      for (const mo of periodMonths) { perMonth[mo] = cls.reduce((a, x) => a + (x.perMonth[mo] || 0), 0); sum += perMonth[mo] }
      cls.sort((a, b) => b.sum - a.sum)
      return { partner, clients: cls, perMonth, sum }
    })
    out.sort((a, b) => b.sum - a.sum)
    return out
  }, [parsed, search, agencySel, clientSel, drilledAgencies, periodMonths, valOf])

  const grand = useMemo(() => {
    const perMonth: Record<string, number> = {}
    let sum = 0, nClients = 0
    for (const g of groups) { nClients += g.clients.length; for (const mo of periodMonths) perMonth[mo] = (perMonth[mo] || 0) + g.perMonth[mo] }
    for (const mo of periodMonths) sum += perMonth[mo] || 0
    return { perMonth, sum, nClients }
  }, [groups, periodMonths])

  const chartData = useMemo(() =>
    periodMonths.map(mo => ({
      month: mo,
      mrr: Math.round(grand.perMonth[mo] || 0),
      domains: groups.reduce((a, g) => a + g.clients.filter(x => (x.perMonth[mo] || 0) > 0).length, 0),
    })), [periodMonths, grand, groups])

  const exportCSV = useCallback(() => {
    const head = ["agency", "client", "organisation_id", "domain", "type", ...periodMonths, "period_sum"]
    const lines = [head.join(",")]
    for (const g of groups) for (const x of g.clients) {
      const row = [g.partner, x.c.organisation, String(x.c.organisation_id), x.c.domain, x.c.types,
        ...periodMonths.map(mo => String(Math.round(x.perMonth[mo] || 0))), String(Math.round(x.sum))]
      lines.push(row.map(v => `"${String(v).replace(/"/g, '""')}"`).join(","))
    }
    const blob = new Blob([lines.join("\n")], { type: "text/csv" })
    const a = document.createElement("a"); a.href = URL.createObjectURL(blob)
    a.download = "partnership_mrr.csv"; a.click()
  }, [groups, periodMonths])

  const money = (v: number) => (v > 0 ? Math.round(v).toLocaleString() : "0")

  return (
    <div className="page-wide">
      <div className="page-header">
        <h1 className="page-title">{tx.title}</h1>
        <span style={{ fontSize: 12, color: "var(--text-3)", maxWidth: 680 }}>{tx.subtitle}</span>
      </div>

      {loading && <div className="loading-center"><span className="spinner-lg" /></div>}
      {error && <div style={{ color: "var(--danger)", fontSize: 13 }}>⚠ {error}</div>}

      {!loading && !error && !clients.length && (
        <div className="card" style={{ fontSize: 13, color: "var(--text-2)" }}>{tx.empty}</div>
      )}

      {!loading && !error && clients.length > 0 && (<>
        {source && (
          <div style={{ fontSize: 11, marginBottom: 10, padding: "6px 10px", borderRadius: 6,
            background: srcGood(source) ? "rgba(34,197,94,0.12)" : "rgba(234,179,8,0.12)",
            color: srcGood(source) ? "#16a34a" : "#a16207",
            border: `1px solid ${srcGood(source) ? "#22c55e44" : "#eab30844"}` }}>
            {srcText(source, tx)}
          </div>
        )}
        {/* Channel toggles — which spend counts into every sum/column/chart */}
        <div className="card" style={{ marginBottom: 12 }}>
          <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 8 }}>{tx.chTogglesNote}</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {CHANNELS.map(c => {
              const on = chOn[c.key]
              return (
                <button key={c.key} onClick={() => setChOn(p => ({ ...p, [c.key]: !p[c.key] }))} title={c.label}
                  style={{ cursor: "pointer", fontSize: 12, padding: "3px 10px", borderRadius: 14,
                    border: `1px solid ${on ? "#22c55e66" : "var(--border)"}`,
                    background: on ? "#22c55e22" : "transparent",
                    color: on ? "#16a34a" : "var(--text-3)" }}>
                  {on ? "●" : "○"} {c.label}
                </button>
              )
            })}
          </div>
        </div>

        <MoneyChart data={chartData} title={tx.moneyTitle} numsLabel={tx.moneyNums} />

        {/* Controls */}
        <div className="filter-row" style={{ marginBottom: 8 }}>
          {/* Agency dropdown — adds to the multi-selection */}
          <select className="filter-input" style={{ padding: "4px 8px" }}
            value="" onChange={e => { if (e.target.value) toggleAgency(e.target.value) }}>
            <option value="">{tx.addAgency} ({clients.length})</option>
            {agencyList.map(([name, n]) => (
              <option key={name} value={name}>{agencySel.has(name) ? "✓ " : ""}{name} ({n})</option>
            ))}
          </select>
          {/* Client dropdown — adds to the multi-selection; label shows its agency */}
          <select className="filter-input" style={{ padding: "4px 8px", maxWidth: 260 }}
            value="" onChange={e => { if (e.target.value) toggleClient(+e.target.value) }}>
            <option value="">{tx.addClient}</option>
            {clientList.map(c => (
              <option key={c.organisation_id} value={c.organisation_id}>
                {clientSel.has(c.organisation_id) ? "✓ " : ""}{(c.organisation || c.domain || `#${c.organisation_id}`)} — {c.partner}
              </option>
            ))}
          </select>
          <input className="filter-input" placeholder={tx.search} value={search} onChange={e => setSearch(e.target.value)} />
          {search && (
            <button onClick={() => setSearch("")} title={tx.clearSearch}
              style={{ fontSize: 12, padding: "3px 10px", borderRadius: 14, cursor: "pointer",
                background: "rgba(148,163,184,0.18)", color: "var(--text-2)", border: "1px solid var(--border)" }}>
              🔎 {search} ✕
            </button>
          )}
          <span style={{ display: "inline-flex", alignItems: "center", gap: 4, fontSize: 12, color: "var(--text-3)" }}>
            {tx.period}:
            <select value={fromIdx} onChange={e => setFromIdx(+e.target.value)} className="filter-input" style={{ padding: "3px 6px" }}>
              {months.map((m, i) => <option key={m} value={i}>{m}</option>)}
            </select>
            –
            <select value={toIdx} onChange={e => setToIdx(+e.target.value)} className="filter-input" style={{ padding: "3px 6px" }}>
              {months.map((m, i) => <option key={m} value={i}>{m}</option>)}
            </select>
          </span>
          <span className="filter-count">{grand.nClients} {tx.clients} · {groups.length}</span>
          <button className="btn-export" onClick={exportCSV}>↓ CSV</button>
        </div>

        {/* Selected agencies / clients as removable chips */}
        {(agencySel.size > 0 || clientSel.size > 0) && (
          <div className="filter-row" style={{ marginBottom: 8, gap: 6, flexWrap: "wrap" }}>
            {[...agencySel].map(name => (
              <button key={"a" + name} onClick={() => toggleAgency(name)} title={tx.clearSearch}
                style={{ fontSize: 12, padding: "3px 10px", borderRadius: 14, cursor: "pointer",
                  background: "rgba(59,130,246,0.18)", color: "#60a5fa", border: "1px solid rgba(59,130,246,0.4)" }}>
                🏢 {name} ✕
              </button>
            ))}
            {pickedClients.map(c => (
              <button key={"c" + c.organisation_id} onClick={() => toggleClient(c.organisation_id)} title={tx.clearSearch}
                style={{ fontSize: 12, padding: "3px 10px", borderRadius: 14, cursor: "pointer",
                  background: "rgba(168,85,247,0.16)", color: "#a855f7", border: "1px solid rgba(168,85,247,0.4)" }}>
                👤 {c.organisation || c.domain} → 🏢 {c.partner} ✕
              </button>
            ))}
            <button onClick={() => { setAgencySel(new Set()); setClientSel(new Set()) }}
              style={{ fontSize: 12, padding: "3px 10px", borderRadius: 14, cursor: "pointer",
                background: "transparent", color: "var(--text-3)", border: "1px solid var(--border)" }}>
              {tx.clearAll}
            </button>
          </div>
        )}

        {!groups.length ? (
          <div className="card" style={{ fontSize: 13, color: "var(--text-3)" }}>{tx.noMatch}</div>
        ) : (
          <div className="table-wrap table-fixed-height">
            <table className="results-table">
              <thead>
                <tr>
                  <th style={{ textAlign: "left" }}>{tx.thClient}</th>
                  <th style={{ textAlign: "left" }}>{tx.thType}</th>
                  {periodMonths.map(mo => (
                    <th key={mo} style={{ textAlign: "right", whiteSpace: "nowrap", fontFamily: "var(--mono)" }}>{mo.slice(2)}</th>
                  ))}
                  <th style={{ textAlign: "right", whiteSpace: "nowrap" }}>{tx.thSum}</th>
                </tr>
              </thead>
              <tbody>
                {groups.map(g => (
                  <Fragment key={g.partner}>
                    {/* Agency subtotal / group header */}
                    <tr style={{ background: "var(--bg-2)" }}>
                      <td colSpan={2} style={{ fontWeight: 700 }}>
                        <span style={{ cursor: "pointer" }} title={tx.filterByAgency}
                          onClick={() => toggleAgency(g.partner)}>
                          🏢 {g.partner}
                        </span>
                        <span style={{ color: "var(--text-3)", fontWeight: 400, fontSize: 11, marginLeft: 6 }}>
                          {g.clients.length} {tx.clients}
                        </span>
                      </td>
                      {periodMonths.map(mo => (
                        <td key={mo} style={{ textAlign: "right", fontWeight: 700, fontFamily: "var(--mono)", fontSize: 11, color: "#16a34a" }}>
                          {cell(g.perMonth[mo] || 0)}
                        </td>
                      ))}
                      <td style={{ textAlign: "right", fontWeight: 700, fontFamily: "var(--mono)", fontSize: 11, color: "#16a34a" }}>
                        {money(g.sum)}
                      </td>
                    </tr>
                    {/* Client rows */}
                    {g.clients.map(x => (
                      <tr key={x.c.organisation_id}>
                        <td className="td-domain" style={{ paddingLeft: 18 }}>
                          {x.c.domain
                            ? <>
                                <span>{x.c.organisation || x.c.domain}</span>
                                <a className="dom-open" href={`https://${x.c.domain}`} target="_blank" rel="noopener"
                                  title={tx.openSite}>↗</a>
                              </>
                            : <span>{x.c.organisation || `#${x.c.organisation_id}`}</span>}
                        </td>
                        <td style={{ fontSize: 10, color: "var(--text-3)", whiteSpace: "nowrap" }}>{x.c.types}</td>
                        {periodMonths.map(mo => {
                          const v = x.perMonth[mo] || 0
                          return (
                            <td key={mo} style={{ textAlign: "right", fontFamily: "var(--mono)", fontSize: 11,
                              color: v > 0 ? "var(--text-2)" : "var(--text-3)" }}
                              title={v > 0 ? Math.round(v).toLocaleString() : undefined}>
                              {cell(v)}
                            </td>
                          )
                        })}
                        <td style={{ textAlign: "right", fontFamily: "var(--mono)", fontSize: 11, fontWeight: 600 }}>
                          {money(x.sum)}
                        </td>
                      </tr>
                    ))}
                  </Fragment>
                ))}
                {/* Grand total */}
                <tr style={{ background: "var(--bg-2)", borderTop: "2px solid var(--border)" }}>
                  <td colSpan={2} style={{ fontWeight: 800 }}>{tx.grand}
                    <span style={{ color: "var(--text-3)", fontWeight: 400, fontSize: 11, marginLeft: 6 }}>
                      {grand.nClients} {tx.clients}
                    </span>
                  </td>
                  {periodMonths.map(mo => (
                    <td key={mo} style={{ textAlign: "right", fontWeight: 800, fontFamily: "var(--mono)", fontSize: 11 }}>
                      {cell(grand.perMonth[mo] || 0)}
                    </td>
                  ))}
                  <td style={{ textAlign: "right", fontWeight: 800, fontFamily: "var(--mono)", fontSize: 11 }}>{money(grand.sum)}</td>
                </tr>
              </tbody>
            </table>
          </div>
        )}
      </>)}
    </div>
  )
}
