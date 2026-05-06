import { useState, useEffect, useCallback } from "react"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

// ── Catalog ───────────────────────────────────────────────────────────────────

type Sheet = "cms" | "ems" | "osearch"
const SHEET_LABELS: Record<Sheet, string> = { cms: "CMS", ems: "EMS", osearch: "OnSiteSearch" }

function CatalogSection() {
  const [tab, setTab] = useState<Sheet>("cms")
  const [catalog, setCatalog] = useState<{ cms: string[]; ems: string[]; osearch: { technology: string; group: string }[] }>({ cms: [], ems: [], osearch: [] })
  const [loading, setLoading] = useState(false)
  const [syncing, setSyncing] = useState(false)
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
      setMsg(`Синхронізовано: CMS ${r.counts.cms}, EMS ${r.counts.ems}, OSearch ${r.counts.osearch}`)
      await load()
    } catch (e: any) { setMsg("Помилка: " + e.message) }
    finally { setSyncing(false) }
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
    } catch (e: any) { setMsg("Помилка: " + e.message) }
  }

  const remove = async (technology: string) => {
    if (!window.confirm(`Видалити "${technology}" з ${SHEET_LABELS[tab]}?`)) return
    setMsg(""); setRemoving(technology)
    try {
      await apiFetch(`/api/setup/catalog?sheet=${tab}&technology=${encodeURIComponent(technology)}`, { method: "DELETE" })
      await load()
      setMsg(`✓ Видалено: ${technology}`)
    } catch (e: any) { setMsg("Помилка: " + e.message) }
    finally { setRemoving(null) }
  }

  const items: string[] = (tab === "osearch"
    ? catalog.osearch.map(o => o.technology)
    : catalog[tab] as string[]
  ).slice().sort((a, b) => a.localeCompare(b, "uk"))

  return (
    <div className="card">
      <div className="setup-section-header">
        <div className="card-section-title">Каталог технологій</div>
        <button className="btn-export" onClick={sync} disabled={syncing}>
          {syncing ? "⏳ Синхронізація..." : "↻ Sync з Google Sheets"}
        </button>
      </div>
      <div className="setup-tabs">
        {(Object.keys(SHEET_LABELS) as Sheet[]).map(s => (
          <button key={s} className={`setup-tab${tab === s ? " active" : ""}`} onClick={() => setTab(s)}>
            {SHEET_LABELS[s]}
            <span className="setup-tab-count">
              {s === "osearch" ? catalog.osearch.length : (catalog[s] as string[]).length}
            </span>
          </button>
        ))}
      </div>
      <div className="setup-add-row">
        <input className="filter-input" placeholder={`Назва технології (${SHEET_LABELS[tab]})...`}
          value={addVal} onChange={e => setAddVal(e.target.value)}
          onKeyDown={e => e.key === "Enter" && add()} style={{ flex: 1 }} />
        {tab === "osearch" && (
          <input className="filter-input" placeholder="Група (напр. Algolia)..."
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
              <button className="setup-remove-btn" onClick={() => remove(tech)}
                disabled={removing === tech} title="Видалити">
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
  { key: "explorer", label: "Explorer",  desc: "Перегляд Explorer та дашборду" },
  { key: "jobs",     label: "Jobs",      desc: "Створення та запуск завдань" },
  { key: "download", label: "Download",  desc: "Скачати CSV / XLSX" },
  { key: "sheets",   label: "Sheets",    desc: "Експорт у Google Sheets" },
  { key: "admin",    label: "Admin",     desc: "Керування системою та юзерами" },
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
    download: ["explorer","jobs","download"],
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

function PermissionToggle({ value, onChange }: { value: string[], onChange: (v: string[]) => void }) {
  const toggle = (key: string) =>
    onChange(value.includes(key) ? value.filter(k => k !== key) : [...value, key])
  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
      {ALL_PERMISSIONS.map(p => (
        <button key={p.key} title={p.desc}
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

function UsersSection() {
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
    catch (e: any) { setMsg("Помилка завантаження користувачів: " + e.message) }
    finally { setLoading(false) }
  }, [])

  useEffect(() => { load() }, [load])

  const add = async () => {
    if (!newUser.username.trim() || !newUser.password.trim()) { setMsg("Введіть логін та пароль"); return }
    setMsg("")
    try {
      await apiFetch("/api/setup/users", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...newUser, permissions: newUser.permissions.join(",") })
      })
      setNewUser(emptyNew())
      await load()
      setMsg("✓ Користувача додано")
    } catch (e: any) { setMsg("Помилка: " + e.message) }
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
      setMsg("✓ Збережено")
    } catch (e: any) { setMsg("Помилка: " + e.message) }
  }

  const remove = async (username: string) => {
    if (!window.confirm(`Видалити користувача "${username}"?`)) return
    setMsg("")
    try {
      await apiFetch(`/api/setup/users/${encodeURIComponent(username)}`, { method: "DELETE" })
      await load()
    } catch (e: any) { setMsg("Помилка: " + e.message) }
  }

  return (
    <div className="card">
      <div className="card-section-title">Користувачі</div>

      {/* Add form */}
      <div style={{ background: "var(--bg-2)", borderRadius: 8, padding: "12px 14px", marginBottom: 16 }}>
        <div style={{ fontWeight: 600, fontSize: 11, color: "var(--text-3)", marginBottom: 8, textTransform: "uppercase", letterSpacing: .5 }}>Новий користувач</div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, marginBottom: 8 }}>
          <input className="filter-input" placeholder="Ім'я..." value={newUser.first_name}
            onChange={e => setNewUser(p => ({ ...p, first_name: e.target.value }))} />
          <input className="filter-input" placeholder="Прізвище..." value={newUser.last_name}
            onChange={e => setNewUser(p => ({ ...p, last_name: e.target.value }))} />
          <input className="filter-input" placeholder="Логін..." value={newUser.username}
            onChange={e => setNewUser(p => ({ ...p, username: e.target.value }))} />
          <input className="filter-input" placeholder="Пароль..." type="password" value={newUser.password}
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
          <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 4 }}>Права доступу:</div>
          <PermissionToggle value={newUser.permissions}
            onChange={v => setNewUser(p => ({ ...p, permissions: v }))} />
        </div>
      </div>

      {msg && <div className="setup-msg" style={{ marginBottom: 8 }}>{msg}</div>}

      <div style={{ marginBottom: 10, display: "flex", alignItems: "center", gap: 8 }}>
        <input
          className="filter-input"
          placeholder="🔍 Пошук за ім'ям, логіном, email..."
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
              <th>Ім'я / Прізвище</th>
              <th>Логін</th>
              <th>Email</th>
              <th>Доступ</th>
              <th>Google Folder</th>
              <th>Створено</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {users.length === 0 && (
              <tr><td colSpan={7} style={{ textAlign: "center", color: "var(--text-3)", padding: 16 }}>
                Немає користувачів — авторизація вимкнена
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
                Нічого не знайдено
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
                    <input className="filter-input" placeholder="Ім'я" value={editFields.first_name || ""}
                      onChange={e => setEditFields(p => ({ ...p, first_name: e.target.value }))} style={{ width: 90 }} />
                    <input className="filter-input" placeholder="Прізвище" value={editFields.last_name || ""}
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
                    onChange={v => setEditFields(p => ({ ...p, _perms: v as any }))} />
                </td>
                <td>
                  <input className="filter-input" placeholder="Folder ID..." value={editFields.google_folder || ""}
                    onChange={e => setEditFields(p => ({ ...p, google_folder: e.target.value }))} style={{ width: 130 }} />
                </td>
                <td>
                  <input className="filter-input" placeholder="Новий пароль..." type="password" value={editFields.password || ""}
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
            <span style={{ fontSize: 12, color: "var(--text-2)" }}>{p.desc}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Activity Logs ─────────────────────────────────────────────────────────────

function LogsSection() {
  const [logs, setLogs] = useState<any[]>([])
  const [loading, setLoading] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    try { const r = await apiFetch("/api/setup/logs"); setLogs(r.logs) } catch {}
    finally { setLoading(false) }
  }, [])

  useEffect(() => { load() }, [load])

  const ACTION_LABELS: Record<string, string> = {
    job_created: "🚀 Новий job", export_csv: "↓ CSV", export_xlsx: "↓ XLSX",
    export_sheets: "↗ Sheets", explore_export_xlsx: "↓ Explorer XLSX",
    explore_export_sheets: "↗ Explorer Sheets",
  }

  return (
    <div className="card">
      <div className="setup-section-header">
        <div className="card-section-title">Лог дій</div>
        <button className="btn-export" onClick={load} disabled={loading}>↻ Оновити</button>
      </div>
      {loading ? <div className="loading-center"><span className="spinner-lg" /></div> : (
        <table className="results-table" style={{ marginTop: 8 }}>
          <thead><tr><th>Дата / Час</th><th>Користувач</th><th>Дія</th><th>Деталі</th></tr></thead>
          <tbody>
            {logs.length === 0 && (
              <tr><td colSpan={4} style={{ textAlign: "center", color: "var(--text-3)", padding: 16 }}>Логів немає</td></tr>
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

function CacheSection() {
  const [days, setDays] = useState(90)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [msg, setMsg] = useState("")

  useEffect(() => {
    apiFetch("/api/setup/settings")
      .then(r => { setDays(r.cache_ttl_days); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  const save = async () => {
    if (days < 1 || days > 3650) { setMsg("Введіть значення від 1 до 3650 днів"); return }
    setSaving(true); setMsg("")
    try {
      await apiFetch("/api/setup/settings", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cache_ttl_days: days })
      })
      setMsg("Збережено")
    } catch (e: any) { setMsg("Помилка: " + e.message) }
    finally { setSaving(false) }
  }

  return (
    <div className="card">
      <div className="card-section-title">Термін актуальності кешу</div>
      <p style={{ fontSize: 13, color: "var(--text-2)", marginTop: 4, marginBottom: 12 }}>
        Дані старше зазначеного терміну вважаються застарілими і будуть оновлені при наступному аналізі.
      </p>
      {loading ? <div className="loading-center"><span className="spinner-lg" /></div> : (
        <div className="setup-add-row">
          <input className="flt-num-input" type="number" min={1} max={3650} value={days}
            onChange={e => setDays(parseInt(e.target.value) || 90)} style={{ width: 90 }} />
          <span style={{ fontSize: 13, color: "var(--text-2)" }}>днів</span>
          <span style={{ fontSize: 12, color: "var(--text-3)" }}>
            ({Math.round(days / 30)} міс. / {(days / 365).toFixed(1)} р.)
          </span>
          <button className="btn-primary" style={{ padding: "6px 16px", fontSize: 13 }} onClick={save} disabled={saving}>
            {saving ? "Зберігаємо..." : "Зберегти"}
          </button>
        </div>
      )}
      {msg && <div className="setup-msg">{msg}</div>}
    </div>
  )
}

// ── Job History ───────────────────────────────────────────────────────────────

function JobsSection() {
  const [count, setCount] = useState<number | null>(null)
  const [clearing, setClearing] = useState(false)
  const [msg, setMsg] = useState("")

  const loadCount = useCallback(async () => {
    try { const r = await apiFetch("/api/setup/jobs/count"); setCount(r.count) } catch {}
  }, [])

  useEffect(() => { loadCount() }, [loadCount])

  const clear = async () => {
    if (!window.confirm(`Видалити ${count} завершених job-ів? Активні (running/pending) залишаться.`)) return
    setClearing(true); setMsg("")
    try {
      await apiFetch("/api/setup/jobs/clear", { method: "POST" })
      setMsg("Historія очищена")
      setCount(0)
    } catch (e: any) { setMsg("Помилка: " + e.message) }
    finally { setClearing(false) }
  }

  return (
    <div className="card">
      <div className="card-section-title">Історія Job-ів</div>
      <p style={{ fontSize: 13, color: "var(--text-2)", marginTop: 4, marginBottom: 12 }}>
        Видаляє всі завершені/скасовані job-и та їх результати. Активні job-и (running/pending) не зачіпаються.
      </p>
      <div className="setup-add-row">
        {count !== null && (
          <span style={{ fontSize: 14, color: "var(--text-2)" }}>
            Завершених job-ів: <strong>{count}</strong>
          </span>
        )}
        <button className="btn-danger" onClick={clear} disabled={clearing || count === 0}>
          {clearing ? "⏳ Очищення..." : "&#128465; Очистити історію"}
        </button>
      </div>
      {msg && <div className="setup-msg">{msg}</div>}
    </div>
  )
}

// ── Main Setup Page ───────────────────────────────────────────────────────────

export default function SetupPage() {
  return (
    <div className="page-wide">
      <div className="page-header">
        <h1 className="page-title">Setup</h1>
        <span style={{ fontSize: 12, color: "var(--text-3)" }}>Налаштування системи</span>
      </div>
      <CatalogSection />
      <div style={{ height: 16 }} />
      <UsersSection />
      <div style={{ height: 16 }} />
      <CacheSection />
      <div style={{ height: 16 }} />
      <JobsSection />
      <div style={{ height: 16 }} />
      <LogsSection />
    </div>
  )
}
