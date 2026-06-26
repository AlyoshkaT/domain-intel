import { useState, useEffect, useCallback } from "react"
import { t, type Lang } from "./i18n"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

// ── BQ Call Stats ────────────────────────────────────────────────────────────

const BQ_STAT_ROWS = [
  { key: "corp_sw",  label: "corpBQ SW",    dot: "#60a5fa" },
  { key: "corp_bw",  label: "corpBQ BW",    dot: "#60a5fa" },
  { key: "corp_ai",  label: "corpBQ AI",    dot: "#60a5fa" },
  { key: "priv_sw",  label: "privatBQ SW",  dot: "#34d399" },
  { key: "priv_bw",  label: "privatBQ BW",  dot: "#34d399" },
  { key: "priv_ai",  label: "privatBQ AI",  dot: "#34d399" },
]

function BqCallStatsSection({ lang }: { lang: Lang }) {
  const [resources, setResources] = useState<Record<string, { today: number; week: number; month: number }>>({})
  const [loading, setLoading]     = useState(true)
  const [updatedAt, setUpdatedAt] = useState("")
  const [bytes, setBytes] = useState<{ corp_gb: number | null; priv_gb: number | null; max_gb: number } | null>(null)

  const load = useCallback(async () => {
    try {
      const r = await apiFetch("/api/setup/bq_call_stats")
      setResources(r.resources || {})
      setUpdatedAt(new Date().toLocaleTimeString("uk-UA"))
      setBytes(r.bytes || null)
    } catch {}
    finally { setLoading(false) }
  }, [])

  useEffect(() => {
    load()
    const ti = setInterval(load, 30000)
    return () => clearInterval(ti)
  }, [load])

  const fmt = (n: number) => (n || 0).toLocaleString("uk-UA")

  const totals = BQ_STAT_ROWS.reduce(
    (acc, { key }) => {
      const r = resources[key] || { today: 0, week: 0, month: 0 }
      return { today: acc.today + r.today, week: acc.week + r.week, month: acc.month + r.month }
    },
    { today: 0, week: 0, month: 0 }
  )

  return (
    <div className="card">
      <div className="setup-section-header">
        <div className="card-section-title">{t('bq_stats_title', lang)}</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {updatedAt && <span style={{ fontSize: 11, color: "var(--text-3)" }}>{t('bq_stats_updated', lang)(updatedAt)}</span>}
          <button className="btn-export" onClick={load} disabled={loading}>↻</button>
        </div>
      </div>

      {loading ? <div className="loading-center"><span className="spinner-lg" /></div> : (
        <div style={{ overflowX: "auto" }}>
          <table className="results-table" style={{ marginTop: 8 }}>
            <thead>
              <tr>
                <th style={{ minWidth: 130 }}>{t('bq_stats_resource', lang)}</th>
                <th style={{ textAlign: "right", minWidth: 90 }}>{t('bq_stats_today', lang)}</th>
                <th style={{ textAlign: "right", minWidth: 90 }}>{t('bq_stats_week', lang)}</th>
                <th style={{ textAlign: "right", minWidth: 110 }}>{t('bq_stats_month', lang)}</th>
              </tr>
            </thead>
            <tbody>
              {BQ_STAT_ROWS.map(({ key, label, dot }, idx) => {
                const r = resources[key] || { today: 0, week: 0, month: 0 }
                const isActive = r.today > 0 || r.week > 0
                // separator before priv group
                return (
                  <>
                    {idx === 3 && (
                      <tr key="sep" style={{ height: 4 }}>
                        <td colSpan={4} style={{ padding: 0, borderBottom: "1px solid var(--border)" }} />
                      </tr>
                    )}
                    <tr key={key} style={{ opacity: isActive ? 1 : 0.55 }}>
                      <td>
                        <span style={{
                          display: "inline-block", width: 8, height: 8,
                          borderRadius: "50%", background: dot,
                          marginRight: 8, flexShrink: 0,
                          boxShadow: isActive ? `0 0 4px ${dot}` : "none",
                        }} />
                        <span style={{ fontSize: 13 }}>{label}</span>
                      </td>
                      <td style={{ textAlign: "right", fontFamily: "var(--mono)", fontWeight: r.today > 0 ? 600 : 400 }}>
                        {fmt(r.today)}
                      </td>
                      <td style={{ textAlign: "right", fontFamily: "var(--mono)" }}>
                        {fmt(r.week)}
                      </td>
                      <td style={{ textAlign: "right", fontFamily: "var(--mono)" }}>
                        {fmt(r.month)}
                      </td>
                    </tr>
                  </>
                )
              })}
            </tbody>
            <tfoot>
              <tr style={{ fontWeight: 600, borderTop: "2px solid var(--border)" }}>
                <td style={{ fontSize: 12, color: "var(--text-3)" }}>{t('bq_stats_total', lang)}</td>
                <td style={{ textAlign: "right", fontFamily: "var(--mono)" }}>{fmt(totals.today)}</td>
                <td style={{ textAlign: "right", fontFamily: "var(--mono)" }}>{fmt(totals.week)}</td>
                <td style={{ textAlign: "right", fontFamily: "var(--mono)" }}>{fmt(totals.month)}</td>
              </tr>
            </tfoot>
          </table>
        </div>
      )}
      {bytes && (
        <div style={{ marginTop: 12, display: "flex", gap: 16, flexWrap: "wrap" }}>
          {[
            { label: t('bq_stats_corp_billed', lang), gb: bytes.corp_gb, dot: "#60a5fa" },
            { label: t('bq_stats_priv_billed', lang), gb: bytes.priv_gb, dot: "#34d399" },
          ].map(({ label, gb, dot }) => (
            <div key={label} style={{
              background: "var(--bg-2)", borderRadius: 8, padding: "8px 14px",
              display: "flex", alignItems: "center", gap: 10,
              border: "1px solid var(--border)",
            }}>
              <span style={{ width: 8, height: 8, borderRadius: "50%", background: dot, display: "inline-block", flexShrink: 0 }} />
              <span style={{ fontSize: 12, color: "var(--text-2)" }}>{label}:</span>
              <span style={{ fontFamily: "var(--mono)", fontWeight: 600, fontSize: 13 }}>
                {gb === null ? "—" : gb >= 1 ? `${gb.toFixed(2)} GB` : `${(gb * 1024).toFixed(0)} MB`}
              </span>
              {gb !== null && bytes.max_gb && (
                <span style={{ fontSize: 11, color: gb / bytes.max_gb > 0.7 ? "var(--danger)" : "var(--text-3)" }}>
                  {t('bq_stats_limit', lang)(bytes.max_gb)}
                </span>
              )}
            </div>
          ))}
        </div>
      )}
      <p style={{ fontSize: 11, color: "var(--text-3)", marginTop: 8, marginBottom: 0 }}>
        {t('bq_stats_footnote', lang)}
      </p>
    </div>
  )
}

// ── Catalog ───────────────────────────────────────────────────────────────────

type Sheet = "cms" | "ems" | "osearch"
const SHEET_LABELS: Record<Sheet, string> = { cms: "CMS", ems: "EMS", osearch: "OnSiteSearch" }

function CatalogSection({ lang }: { lang: Lang }) {
  const [tab, setTab] = useState<Sheet>("cms")
  const [catalog, setCatalog] = useState<{ cms: string[]; ems: string[]; osearch: { technology: string; group: string }[] }>({ cms: [], ems: [], osearch: [] })
  const [loading, setLoading] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [rematching, setRematching] = useState(false)
  const [rebuildingTech, setRebuildingTech] = useState(false)
  const [refreshingDesc, setRefreshingDesc] = useState(false)
  const [addVal, setAddVal] = useState("")
  const [addGroup, setAddGroup] = useState("")
  const [msg, setMsg] = useState("")
  const [removing, setRemoving] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    try { setCatalog(await apiFetch("/api/setup/catalog")) } catch (e: any) { setMsg(e.message) }
    finally { setLoading(false) }
  }, [])

  useEffect(() => { load() }, [load])

  const sync = async () => {
    setSyncing(true); setMsg("")
    try {
      const r = await apiFetch("/api/setup/catalog/sync", { method: "POST" })
      setMsg(t('setup_synced', lang)(String(r.counts.cms), String(r.counts.ems), String(r.counts.osearch)))
      await load()
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
    finally { setSyncing(false) }
  }

  const rematch = async () => {
    if (!window.confirm(t('setup_rematch_confirm', lang))) return
    setRematching(true); setMsg("")
    try {
      const r = await apiFetch("/api/catalog/rematch", { method: "POST" })
      setMsg(t('setup_rematch_done', lang)(r.updated, r.elapsed))
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
    finally { setRematching(false) }
  }

  const rebuildTech = async () => {
    if (!window.confirm(t('setup_tech_rebuild_confirm', lang))) return
    setRebuildingTech(true); setMsg("")
    try {
      const r = await apiFetch("/api/explore/tech_rebuild", { method: "POST" })
      setMsg(t('setup_tech_rebuild_done', lang)(r.techs, r.pairs, r.elapsed))
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
    finally { setRebuildingTech(false) }
  }

  const refreshDesc = async () => {
    if (!window.confirm(t('setup_tech_desc_confirm', lang))) return
    setRefreshingDesc(true); setMsg("")
    try {
      const r = await apiFetch("/api/explore/tech_descriptions/refresh", { method: "POST" })
      setMsg(t('setup_tech_desc_done', lang)(r.techs, r.mb_billed_corp, r.elapsed))
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
    finally { setRefreshingDesc(false) }
  }

  const add = async () => {
    if (!addVal.trim()) return
    setMsg("")
    try {
      await apiFetch("/api/setup/catalog/add", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sheet: tab, technology: addVal.trim(), group_name: addGroup.trim() })
      })
      setAddVal(""); setAddGroup(""); await load()
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
  }

  const remove = async (technology: string) => {
    if (!window.confirm(t('setup_confirm_delete_tech', lang)(technology, SHEET_LABELS[tab]))) return
    setMsg(""); setRemoving(technology)
    try {
      await apiFetch(`/api/setup/catalog?sheet=${tab}&technology=${encodeURIComponent(technology)}`, { method: "DELETE" })
      await load()
      setMsg(t('setup_tech_deleted', lang)(technology))
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
    finally { setRemoving(null) }
  }

  const items: string[] = (tab === "osearch" || tab === "ems" || tab === "cms"
    ? (catalog[tab] as unknown as {technology: string; group: string}[]).map(e => e.technology)
    : catalog[tab] as string[]
  ).slice().sort((a, b) => a.localeCompare(b, "uk"))

  return (
    <div className="card">
      <div className="setup-section-header">
        <div className="card-section-title">Каталог технологій</div>
        <button className="btn-export" onClick={sync} disabled={syncing}>
          {syncing ? t('setup_syncing', lang) : t('setup_sync_btn', lang)}
        </button>
        <button className="btn-export" onClick={rematch} disabled={rematching} style={{marginLeft: 8}}>
          {rematching ? t('setup_rematching', lang) : t('setup_rematch_btn', lang)}
        </button>
        <button className="btn-export" onClick={rebuildTech} disabled={rebuildingTech} style={{marginLeft: 8}}>
          {rebuildingTech ? t('setup_tech_rebuilding', lang) : t('setup_tech_rebuild_btn', lang)}
        </button>
        <button className="btn-export" onClick={refreshDesc} disabled={refreshingDesc} style={{marginLeft: 8}}>
          {refreshingDesc ? t('setup_tech_desc_refreshing', lang) : t('setup_tech_desc_btn', lang)}
        </button>
      </div>
      <div className="setup-tabs">
        {(Object.keys(SHEET_LABELS) as Sheet[]).map(s => (
          <button key={s} className={`setup-tab${tab === s ? " active" : ""}`} onClick={() => setTab(s)}>
            {SHEET_LABELS[s]}
            <span className="setup-tab-count">
              {catalog[s].length}
            </span>
          </button>
        ))}
      </div>
      <div className="setup-add-row">
        <input className="filter-input" placeholder={`Назва технології (${SHEET_LABELS[tab]})...`}
          value={addVal} onChange={e => setAddVal(e.target.value)}
          onKeyDown={e => e.key === "Enter" && add()} style={{ flex: 1 }} />
        {tab === "osearch" && (
          <input className="filter-input" placeholder={t('setup_ph_group', lang)}
            value={addGroup} onChange={e => setAddGroup(e.target.value)} style={{ width: 160 }} />
        )}
        <button className="btn-primary" style={{ padding: "6px 16px", fontSize: 13 }} onClick={add}>+ Додати</button>
      </div>
      {msg && <div className="setup-msg">{msg}</div>}
      {loading ? <div className="loading-center"><span className="spinner-lg" /></div> : (
        <div className="setup-catalog-list">
          {items.length === 0 && <div className="empty-state" style={{ padding: "20px 0" }}>Каталог порожній</div>}
          {items.map(tech => (
            <div key={tech} className="setup-catalog-item">
              <span className="setup-catalog-name">{tech}</span>
              {tab === "osearch" && (
                <span className="setup-catalog-group">
                  {catalog.osearch.find(o => o.technology === tech)?.group || ""}
                </span>
              )}
              {(tab === "ems" || tab === "cms") && (
                <span className="setup-catalog-group">
                  {(catalog[tab] as unknown as {technology: string; group: string}[]).find(e => e.technology === tech)?.group || ""}
                </span>
              )}
              <button className="setup-remove-btn" onClick={() => remove(tech)}
                disabled={removing === tech} title={t('setup_delete', lang)}>
                {removing === tech ? "⏳" : "✕"}
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Users ─────────────────────────────────────────────────────────────────────

const ALL_PERMISSIONS = [
  { key: "explorer", label: "Explorer",  descKey: "setup_perm_explorer" as const },
  { key: "jobs",     label: "Jobs",      descKey: "setup_perm_jobs" as const },
  { key: "download", label: "Download",  descKey: "setup_perm_download" as const },
  { key: "sheets",   label: "Sheets",    descKey: "setup_perm_sheets" as const },
  { key: "admin",    label: "Admin",     descKey: "setup_perm_admin" as const },
]

const PRESETS = [
  { label: "Viewer",  perms: ["explorer"] },
  { label: "Manager", perms: ["explorer","jobs","download","sheets"] },
  { label: "Admin",   perms: ["explorer","jobs","download","sheets","admin"] },
]

const PERM_ORDER = ALL_PERMISSIONS.map(p => p.key)

// Map legacy permission values to new system, always sorted by rank
function parsePerms(s?: string): string[] {
  if (!s) return ["explorer"]
  const legacy: Record<string, string[]> = {
    read:     ["explorer"],
    add:      ["explorer","jobs"],
    download: ["explorer","download"],
    admin:    ["explorer","jobs","download","sheets","admin"],
  }
  const parts = s.split(",").map(p => p.trim()).filter(Boolean)
  // if single legacy value — map it
  if (parts.length === 1 && legacy[parts[0]]) return legacy[parts[0]]
  // filter to known keys and sort by canonical rank
  const known = new Set(PERM_ORDER)
  const valid = parts.filter(p => known.has(p))
  return PERM_ORDER.filter(k => valid.includes(k))
}

function PermissionToggle({ value, onChange, lang }: { value: string[], onChange: (v: string[]) => void; lang: Lang }) {
  const toggle = (key: string) =>
    onChange(value.includes(key) ? value.filter(k => k !== key) : [...value, key])
  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
      {ALL_PERMISSIONS.map(p => (
        <button key={p.key} title={t(p.descKey, lang)}
          onClick={() => toggle(p.key)}
          style={{
            padding: "3px 10px", fontSize: 12, borderRadius: 4, cursor: "pointer", border: "1px solid",
            background: value.includes(p.key) ? "var(--accent)" : "var(--bg-2)",
            color: value.includes(p.key) ? "#fff" : "var(--text-2)",
            borderColor: value.includes(p.key) ? "var(--accent)" : "var(--border)",
            fontWeight: value.includes(p.key) ? 600 : 400,
          }}>
          {p.label}
        </button>
      ))}
      <span style={{ color: "var(--border)", margin: "0 2px" }}>|</span>
      {PRESETS.map(pr => (
        <button key={pr.label} onClick={() => onChange(pr.perms)}
          style={{
            padding: "3px 8px", fontSize: 11, borderRadius: 4, cursor: "pointer",
            background: "transparent", color: "var(--text-3)",
            border: "1px dashed var(--border)",
          }}>
          {pr.label}
        </button>
      ))}
    </div>
  )
}



interface User {
  username: string; permissions: string; created_at: string
  first_name?: string; last_name?: string; email?: string
  google_folder?: string; display_name?: string
}

const emptyNew = () => ({
  username: "", password: "", permissions: ["explorer"] as string[],
  first_name: "", last_name: "", email: "", google_folder: "",
})

function UsersSection({ lang }: { lang: Lang }) {
  const [users, setUsers] = useState<User[]>([])
  const [loading, setLoading] = useState(false)
  const [newUser, setNewUser] = useState(emptyNew())
  const [editingUser, setEditingUser] = useState<string | null>(null)
  const [editFields, setEditFields] = useState<Partial<User & { password: string; _perms: string[] }>>({})
  const [msg, setMsg] = useState("")
  const [userSearch, setUserSearch] = useState("")

  const load = useCallback(async () => {
    setLoading(true)
    try { const r = await apiFetch("/api/setup/users"); setUsers(r.users) }
    catch (e: any) { setMsg(t('setup_err_load_users', lang)(e.message)) }
    finally { setLoading(false) }
  }, [lang])

  useEffect(() => { load() }, [load])

  const add = async () => {
    if (!newUser.username.trim() || !newUser.password.trim()) { setMsg(t('setup_err_login_pwd', lang)); return }
    setMsg("")
    try {
      await apiFetch("/api/setup/users", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...newUser, permissions: newUser.permissions.join(",") })
      })
      setNewUser(emptyNew())
      await load()
      setMsg(t('setup_user_added', lang))
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
  }

  const startEdit = (u: User) => {
    setEditingUser(u.username)
    setEditFields({
      first_name: u.first_name || "", last_name: u.last_name || "",
      email: u.email || "", google_folder: u.google_folder || "",
      permissions: u.permissions, password: "",
      _perms: parsePerms(u.permissions) as any,
    })
  }

  const saveEdit = async (username: string) => {
    const fields: any = { ...editFields }
    if (fields._perms) { fields.permissions = (fields._perms as string[]).join(","); delete fields._perms }
    if (!fields.password) delete fields.password
    for (const k of ["first_name","last_name","email","google_folder"]) {
      if (fields[k] === "") fields[k] = null
    }
    try {
      await apiFetch(`/api/setup/users/${encodeURIComponent(username)}`, {
        method: "PATCH", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(fields)
      })
      setEditingUser(null)
      await load()
      setMsg(t('setup_saved', lang))
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
  }

  const remove = async (username: string) => {
    if (!window.confirm(t('setup_confirm_delete_user', lang)(username))) return
    setMsg("")
    try {
      await apiFetch(`/api/setup/users/${encodeURIComponent(username)}`, { method: "DELETE" })
      await load()
      setMsg(t('setup_user_deleted', lang))
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
  }

  return (
    <div className="card">
      <div className="card-section-title">Користувачі</div>

      {/* Add form */}
      <div style={{ background: "var(--bg-2)", borderRadius: 8, padding: "12px 14px", marginBottom: 16 }}>
        <div style={{ fontWeight: 600, fontSize: 11, color: "var(--text-3)", marginBottom: 8, textTransform: "uppercase", letterSpacing: .5 }}>{t('setup_new_user', lang)}</div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, marginBottom: 8 }}>
          <input className="filter-input" placeholder={t('setup_ph_first', lang)} value={newUser.first_name}
            onChange={e => setNewUser(p => ({ ...p, first_name: e.target.value }))} />
          <input className="filter-input" placeholder={t('setup_ph_last', lang)} value={newUser.last_name}
            onChange={e => setNewUser(p => ({ ...p, last_name: e.target.value }))} />
          <input className="filter-input" placeholder={t('setup_ph_login', lang)} value={newUser.username}
            onChange={e => setNewUser(p => ({ ...p, username: e.target.value }))} />
          <input className="filter-input" placeholder={t('setup_ph_password', lang)} type="password" value={newUser.password}
            onChange={e => setNewUser(p => ({ ...p, password: e.target.value }))} />
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <input className="filter-input" placeholder="Email (Google)..." value={newUser.email}
            onChange={e => setNewUser(p => ({ ...p, email: e.target.value }))} style={{ flex: "1 1 180px" }} />
          <input className="filter-input" placeholder="Google Folder ID (необов'язково)..." value={newUser.google_folder}
            onChange={e => setNewUser(p => ({ ...p, google_folder: e.target.value }))} style={{ flex: "1 1 160px" }} />
          <button className="btn-primary" style={{ padding: "6px 16px", fontSize: 13, whiteSpace: "nowrap" }} onClick={add}>+ Додати</button>
        </div>
        <div style={{ marginTop: 8 }}>
          <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 4 }}>{t('setup_access_rights', lang)}</div>
          <PermissionToggle value={newUser.permissions}
            onChange={v => setNewUser(p => ({ ...p, permissions: v }))} lang={lang} />
        </div>
      </div>

      {msg && <div className="setup-msg" style={{ marginBottom: 8 }}>{msg}</div>}

      <div style={{ marginBottom: 10, display: "flex", alignItems: "center", gap: 8 }}>
        <input
          className="filter-input"
          placeholder={t('setup_search_ph', lang)}
          value={userSearch}
          onChange={e => setUserSearch(e.target.value)}
          style={{ flex: 1 }}
        />
        <span style={{ fontSize: 11, color: "var(--text-3)", whiteSpace: "nowrap", fontFamily: "var(--mono)" }}>
          {userSearch.trim()
            ? `${users.filter(u => { const q = userSearch.trim().toLowerCase(); return u.username.toLowerCase().includes(q) || (u.first_name||"").toLowerCase().includes(q) || (u.last_name||"").toLowerCase().includes(q) || (u.email||"").toLowerCase().includes(q) }).length} / ${users.length}`
            : `${users.length}`}
        </span>
        {userSearch && (
          <button onClick={() => setUserSearch("")}
            style={{ background: "none", border: "none", cursor: "pointer", color: "var(--text-3)", fontSize: 16 }}>✕</button>
        )}
      </div>

      {loading ? <div className="loading-center"><span className="spinner-lg" /></div> : (
        <div style={{ overflowX: "auto" }}>
        <table className="results-table">
          <thead>
            <tr>
              <th>{t('setup_col_name', lang)}</th>
              <th>{t('setup_col_login', lang)}</th>
              <th>Email</th>
              <th>{t('setup_col_access', lang)}</th>
              <th>Google Folder</th>
              <th>{t('setup_col_created', lang)}</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {users.length === 0 && (
              <tr><td colSpan={7} style={{ textAlign: "center", color: "var(--text-3)", padding: 16 }}>
                {t('setup_no_users', lang)}
              </td></tr>
            )}
            {users.length > 0 && userSearch.trim() && users.filter(u => {
              const q = userSearch.trim().toLowerCase()
              return u.username.toLowerCase().includes(q) ||
                (u.first_name || "").toLowerCase().includes(q) ||
                (u.last_name || "").toLowerCase().includes(q) ||
                (u.email || "").toLowerCase().includes(q)
            }).length === 0 && (
              <tr><td colSpan={7} style={{ textAlign: "center", color: "var(--text-3)", padding: 16 }}>
                {t('setup_no_match', lang)}
              </td></tr>
            )}
            {users.filter(u => {
              if (!userSearch.trim()) return true
              const q = userSearch.trim().toLowerCase()
              return u.username.toLowerCase().includes(q) ||
                (u.first_name || "").toLowerCase().includes(q) ||
                (u.last_name || "").toLowerCase().includes(q) ||
                (u.email || "").toLowerCase().includes(q)
            }).map(u => editingUser === u.username ? (
              <tr key={u.username} style={{ background: "var(--bg-2)" }}>
                <td>
                  <div style={{ display: "flex", gap: 4 }}>
                    <input className="filter-input" placeholder={t('setup_ph_first', lang)} value={editFields.first_name || ""}
                      onChange={e => setEditFields(p => ({ ...p, first_name: e.target.value }))} style={{ width: 90 }} />
                    <input className="filter-input" placeholder={t('setup_ph_last', lang)} value={editFields.last_name || ""}
                      onChange={e => setEditFields(p => ({ ...p, last_name: e.target.value }))} style={{ width: 90 }} />
                  </div>
                </td>
                <td style={{ fontFamily: "var(--mono)", fontSize: 12 }}>{u.username}</td>
                <td>
                  <input className="filter-input" placeholder="email@..." value={editFields.email || ""}
                    onChange={e => setEditFields(p => ({ ...p, email: e.target.value }))} style={{ width: 160 }} />
                </td>
                <td colSpan={2}>
                  <PermissionToggle
                    value={(editFields as any)._perms || parsePerms(editFields.permissions)}
                    onChange={v => setEditFields(p => ({ ...p, _perms: v as any }))} lang={lang} />
                </td>
                <td>
                  <input className="filter-input" placeholder="Folder ID..." value={editFields.google_folder || ""}
                    onChange={e => setEditFields(p => ({ ...p, google_folder: e.target.value }))} style={{ width: 130 }} />
                </td>
                <td>
                  <input className="filter-input" placeholder={t('setup_ph_new_password', lang)} type="password" value={editFields.password || ""}
                    onChange={e => setEditFields(p => ({ ...p, password: e.target.value }))} style={{ width: 110 }} />
                </td>
                <td>
                  <div style={{ display: "flex", gap: 4 }}>
                    <button className="btn-primary" style={{ padding: "3px 10px", fontSize: 12 }} onClick={() => saveEdit(u.username)}>✓</button>
                    <button className="setup-remove-btn" onClick={() => setEditingUser(null)}>✕</button>
                  </div>
                </td>
              </tr>
            ) : (
              <tr key={u.username}>
                <td>{[u.first_name, u.last_name].filter(Boolean).join(" ") || <span style={{ color: "var(--text-3)" }}>—</span>}</td>
                <td style={{ fontFamily: "var(--mono)", fontSize: 12, fontWeight: 500 }}>{u.username}</td>
                <td style={{ fontSize: 12 }}>{u.email || <span style={{ color: "var(--text-3)" }}>—</span>}</td>
                <td>{parsePerms(u.permissions).map(p => <span key={p} className="service-tag" style={{marginRight:3}}>{p}</span>)}</td>
                <td style={{ fontSize: 11, color: "var(--text-3)", maxWidth: 120, overflow: "hidden", textOverflow: "ellipsis" }}>
                  {u.google_folder ? <span title={u.google_folder}>{u.google_folder.slice(0, 12)}…</span> : "—"}
                </td>
                <td style={{ fontSize: 11, color: "var(--text-3)" }}>{u.created_at ? new Date(u.created_at).toLocaleDateString("uk-UA") : "—"}</td>
                <td>
                  <div style={{ display: "flex", gap: 4 }}>
                    <button className="btn-export" style={{ padding: "2px 8px", fontSize: 11 }} onClick={() => startEdit(u)}>✎</button>
                    <button className="setup-remove-btn" onClick={() => remove(u.username)}>✕</button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      )}

      <div className="setup-permissions-legend" style={{ marginTop: 12 }}>
        {ALL_PERMISSIONS.map(p => (
          <div key={p.key} className="setup-perm-row">
            <span className="service-tag">{p.label}</span>
            <span style={{ fontSize: 12, color: "var(--text-2)" }}>{t(p.descKey, lang)}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Activity Logs ─────────────────────────────────────────────────────────────

function LogsSection({ lang }: { lang: Lang }) {
  const [logs, setLogs] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [clearing, setClearing] = useState(false)
  const [testMsg, setTestMsg] = useState("")

  const load = useCallback(async () => {
    setLoading(true)
    try { const r = await apiFetch("/api/setup/logs"); setLogs(r.logs) } catch {}
    finally { setLoading(false) }
  }, [])

  const testLog = async () => {
    setTestMsg(t('setup_log_testing', lang))
    try {
      const r = await apiFetch("/api/setup/logs/test", { method: "POST" })
      setTestMsg(t('setup_log_ok', lang)(r.recent_logs?.length ?? 0))
      setLogs(r.recent_logs || [])
    } catch (e: any) { setTestMsg(t('setup_log_err', lang)(e.message)) }
  }

  const clearLogs = async () => {
    if (!window.confirm(t('setup_log_confirm_clear', lang))) return
    setClearing(true)
    setTestMsg("")
    try {
      const r = await apiFetch("/api/setup/logs/clear", { method: "DELETE" })
      setTestMsg(t('setup_log_cleared', lang)(String(r.deleted ?? 0)))
      setLogs([])
    } catch (e: any) { setTestMsg(t('setup_err', lang)(e.message)) }
    finally { setClearing(false) }
  }

  useEffect(() => {
    load()
    const ti = setInterval(load, 30000)
    return () => clearInterval(ti)
  }, [load])

  const ACTION_LABELS: Record<string, string> = {
    job_created: "🚀 Новий job",
    job_export_csv: "↓ Job CSV", job_export_xlsx: "↓ Job XLSX", job_export_sheets: "↗ Job Sheets",
    explore_export_csv: "↓ Explorer CSV", explore_export_xlsx: "↓ Explorer XLSX", explore_export_sheets: "↗ Explorer Sheets",
    tech_export_csv: "↓ Tech CSV", tech_export_xlsx: "↓ Tech XLSX",
    log_test: "🧪 Тест", login: "🔑 Вхід",
  }

  return (
    <div className="card">
      <div className="setup-section-header">
        <div className="card-section-title">Лог дій</div>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          {testMsg && <span style={{ fontSize: 11, color: "var(--text-3)" }}>{testMsg}</span>}
          <button className="btn-export" onClick={testLog}>{t('setup_log_test_btn', lang)}</button>
          <button className="btn-export" onClick={load} disabled={loading}>↻ {t('setup_log_refresh', lang)}</button>
          <button
            className="btn-export"
            onClick={clearLogs}
            disabled={clearing || logs.length === 0}
            style={{ color: "var(--danger)", borderColor: "var(--danger)" }}
          >
            🗑 {t('setup_log_clear_btn', lang)}
          </button>
        </div>
      </div>
      {loading ? <div className="loading-center"><span className="spinner-lg" /></div> : (
        <table className="results-table" style={{ marginTop: 8 }}>
          <thead><tr><th>Дата / Час</th><th>Користувач</th><th>Дія</th><th>Деталі</th></tr></thead>
          <tbody>
            {logs.length === 0 && (
              <tr><td colSpan={4} style={{ textAlign: "center", color: "var(--text-3)", padding: 16 }}>{t('setup_log_none', lang)}</td></tr>
            )}
            {logs.map((l, i) => {
              let details = ""
              try { const d = JSON.parse(l.details || "{}"); details = Object.entries(d).map(([k,v]) => `${k}: ${v}`).join(", ") } catch {}
              return (
                <tr key={i}>
                  <td style={{ fontSize: 11, color: "var(--text-3)", whiteSpace: "nowrap" }}>
                    {l.logged_at ? new Date(l.logged_at).toLocaleString("uk-UA") : "—"}
                  </td>
                  <td style={{ fontFamily: "var(--mono)", fontSize: 12 }}>{l.username}</td>
                  <td style={{ whiteSpace: "nowrap" }}>{ACTION_LABELS[l.action] || l.action}</td>
                  <td style={{ fontSize: 11, color: "var(--text-2)" }}>{details}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </div>
  )
}

// ── Cache TTL ─────────────────────────────────────────────────────────────────

function CacheSection({ lang }: { lang: Lang }) {
  const [days, setDays] = useState(90)
  const [bqMaxBytes, setBqMaxBytes] = useState(50)
  const [bqFloor, setBqFloor] = useState(1)
  const [autoSync, setAutoSync] = useState(true)
  const [syncFreq, setSyncFreq] = useState("daily")
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [savingBq, setSavingBq] = useState(false)
  const [togglingSync, setTogglingSync] = useState(false)
  const [manualSyncing, setManualSyncing] = useState(false)
  const [msg, setMsg] = useState("")
  const [msgBq, setMsgBq] = useState("")
  const [msgSync, setMsgSync] = useState("")
  const [showBqWarning, setShowBqWarning] = useState(false)

  useEffect(() => {
    apiFetch("/api/setup/settings")
      .then(r => {
        setDays(r.cache_ttl_days)
        setBqMaxBytes(r.bq_max_bytes_gb ?? 50)
        setBqFloor(r.bq_max_bytes_gb_floor ?? 1)
        setAutoSync(r.auto_sync_enabled !== false)
        setSyncFreq(r.auto_sync_frequency || "daily")
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [])

  const save = async () => {
    if (days < 1 || days > 3650) { setMsg(t('setup_cache_days_err', lang)); return }
    setSaving(true); setMsg("")
    try {
      await apiFetch("/api/setup/settings", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cache_ttl_days: days })
      })
      setMsg(t('setup_saved', lang))
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
    finally { setSaving(false) }
  }

  const setFrequency = async (freq: string) => {
    setTogglingSync(true)
    try {
      await apiFetch("/api/setup/settings", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ auto_sync_frequency: freq })
      })
      setSyncFreq(freq)
      setAutoSync(freq !== "off")
    } catch (e: any) { setMsgBq(t('setup_err', lang)(e.message)) }
    finally { setTogglingSync(false) }
  }

  const manualSync = async () => {
    if (!window.confirm(t('manual_sync_confirm', lang))) return
    setManualSyncing(true); setMsgSync("")
    try {
      const r = await apiFetch("/api/admin/sync_parsed_from_corp", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" })
      if (r.error) setMsgSync(t('setup_err', lang)(r.error))
      else setMsgSync(t('manual_sync_done', lang)(r.sw_rows ?? 0, r.bw_rows ?? 0, r.ai_rows ?? 0, r.elapsed ?? 0))
    } catch (e: any) { setMsgSync(t('setup_err', lang)(e.message)) }
    finally { setManualSyncing(false) }
  }

  const saveBqLimit = async () => {
    if (bqMaxBytes < bqFloor || bqMaxBytes > 1000) {
      setMsgBq(t('bq_limit_err', lang)(bqFloor)); return
    }
    setSavingBq(true); setMsgBq("")
    try {
      await apiFetch("/api/setup/settings", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ bq_max_bytes_gb: bqMaxBytes })
      })
      setMsgBq(t('setup_saved', lang))
      setShowBqWarning(false)
    } catch (e: any) { setMsgBq(t('setup_err', lang)(e.message)) }
    finally { setSavingBq(false) }
  }

  return (
    <div className="card">
      <div className="card-section-title">{t('cache_title', lang)}</div>
      <p style={{ fontSize: 13, color: "var(--text-2)", marginTop: 4, marginBottom: 12 }}>
        {t('setup_cache_desc', lang)}
      </p>
      {loading ? <div className="loading-center"><span className="spinner-lg" /></div> : (<>
        <div className="setup-add-row">
          <input className="flt-num-input" type="number" min={1} max={3650} value={days}
            onChange={e => setDays(parseInt(e.target.value) || 90)} style={{ width: 90 }} />
          <span style={{ fontSize: 13, color: "var(--text-2)" }}>{t('setup_days', lang)}</span>
          <span style={{ fontSize: 12, color: "var(--text-3)" }}>
            {t('setup_months_years', lang)(Math.round(days / 30), (days / 365).toFixed(1))}
          </span>
          <button className="btn-primary" style={{ padding: "6px 16px", fontSize: 13 }} onClick={save} disabled={saving}>
            {saving ? t('setup_saving', lang) : t('setup_save', lang)}
          </button>
        </div>
        {msg && <div className="setup-msg">{msg}</div>}

        <div style={{ borderTop: "1px solid var(--border)", marginTop: 16, paddingTop: 16 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text-1)", marginBottom: 6 }}>
            {t('bq_limit_title', lang)}
          </div>
          <p style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 10 }}>
            {t('bq_limit_desc', lang)(bqFloor)}
          </p>
          <div className="setup-add-row" style={{ marginBottom: 0 }}>
            <input className="flt-num-input" type="number" min={bqFloor} max={1000} value={bqMaxBytes}
              onChange={e => {
                const v = parseInt(e.target.value) || bqFloor
                setBqMaxBytes(v)
                setShowBqWarning(v > 25)
              }}
              style={{ width: 90 }} />
            <span style={{ fontSize: 13, color: "var(--text-2)" }}>GB</span>
            <button className="btn-primary" style={{ padding: "6px 16px", fontSize: 13 }} onClick={saveBqLimit} disabled={savingBq}>
              {savingBq ? t('setup_saving', lang) : t('setup_save', lang)}
            </button>
          </div>
          {showBqWarning && (
            <div style={{
              marginTop: 12, padding: "10px 14px",
              background: "rgba(251,191,36,0.12)", border: "1px solid rgba(251,191,36,0.5)",
              borderRadius: 8, fontSize: 13, color: "#fbbf24",
            }}>
              {t('bq_limit_warn', lang)}
            </div>
          )}
          {msgBq && <div className="setup-msg" style={{ marginTop: 8 }}>{msgBq}</div>}
        </div>

        <div style={{ borderTop: "1px solid var(--border)", marginTop: 16, paddingTop: 16 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text-1)", marginBottom: 6 }}>
            {t('auto_sync_title', lang)}
          </div>
          <p style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 10 }}>
            {t('auto_sync_desc', lang)}
          </p>
          <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
            {(["daily", "weekly", "monthly", "off"] as const).map(f => {
              const active = syncFreq === f
              const isOff = f === "off"
              return (
                <button
                  key={f}
                  onClick={() => setFrequency(f)}
                  disabled={togglingSync || active}
                  style={{
                    padding: "6px 14px", borderRadius: 20, fontSize: 13, fontWeight: 600,
                    border: active ? "none" : "1px solid var(--border)",
                    cursor: togglingSync ? "wait" : active ? "default" : "pointer",
                    background: active
                      ? (isOff ? "rgba(239,68,68,0.2)" : "rgba(52,211,153,0.2)")
                      : "transparent",
                    color: active ? (isOff ? "#f87171" : "#34d399") : "var(--text-2)",
                    transition: "all 0.2s",
                  }}
                >
                  {t(`sync_freq_${f}` as any, lang)}
                </button>
              )
            })}
            {syncFreq === "off" && (
              <span style={{ fontSize: 12, color: "#f87171" }}>
                {t('auto_sync_paused', lang)}
              </span>
            )}
          </div>
          <div style={{ marginTop: 12, display: "flex", alignItems: "center", gap: 12 }}>
            <button className="btn-export" onClick={manualSync} disabled={manualSyncing}>
              {manualSyncing ? t('manual_sync_running', lang) : t('manual_sync_btn', lang)}
            </button>
            {msgSync && <span style={{ fontSize: 12, color: "var(--text-2)" }}>{msgSync}</span>}
          </div>
        </div>
      </>)}
    </div>
  )
}

// ── Job History ───────────────────────────────────────────────────────────────

function JobsSection({ lang }: { lang: Lang }) {
  const [count, setCount] = useState<number | null>(null)
  const [clearing, setClearing] = useState(false)
  const [msg, setMsg] = useState("")

  const loadCount = useCallback(async () => {
    try { const r = await apiFetch("/api/setup/jobs/count"); setCount(r.count) } catch {}
  }, [])

  useEffect(() => { loadCount() }, [loadCount])

  const clear = async () => {
    if (!window.confirm(t('setup_confirm_clear_jobs', lang)(String(count)))) return
    setClearing(true); setMsg("")
    try {
      await apiFetch("/api/setup/jobs/clear", { method: "POST" })
      setMsg(t('setup_history_cleared', lang))
      setCount(0)
    } catch (e: any) { setMsg(t('setup_err', lang)(e.message)) }
    finally { setClearing(false) }
  }

  return (
    <div className="card">
      <div className="card-section-title">Історія Job-ів</div>
      <p style={{ fontSize: 13, color: "var(--text-2)", marginTop: 4, marginBottom: 12 }}>
        {t('setup_clear_desc', lang)}
      </p>
      <div className="setup-add-row">
        {count !== null && (
          <span style={{ fontSize: 14, color: "var(--text-2)" }}>
            {t('setup_completed_count', lang)(String(count))}
          </span>
        )}
        <button className="btn-danger" onClick={clear} disabled={clearing || count === 0}>
          {clearing ? t('setup_clearing', lang) : t('setup_clear_btn', lang)}
        </button>
      </div>
      {msg && <div className="setup-msg">{msg}</div>}
    </div>
  )
}

// ── Main Setup Page ───────────────────────────────────────────────────────────

export default function SetupPage({ lang }: { lang: Lang }) {
  return (
    <div className="page-wide">
      <div className="page-header">
        <h1 className="page-title">{t('setup_title', lang)}</h1>
        <span style={{ fontSize: 12, color: "var(--text-3)" }}>{t('setup_subtitle', lang)}</span>
      </div>
      <BqCallStatsSection lang={lang} />
      <div style={{ height: 16 }} />
      <CatalogSection lang={lang} />
      <div style={{ height: 16 }} />
      <UsersSection lang={lang} />
      <div style={{ height: 16 }} />
      <CacheSection lang={lang} />
      <div style={{ height: 16 }} />
      <JobsSection lang={lang} />
      <div style={{ height: 16 }} />
      <LogsSection lang={lang} />
    </div>
  )
}
