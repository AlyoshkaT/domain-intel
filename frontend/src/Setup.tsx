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

const PERMISSION_LABELS: Record<string, string> = {
  read: "Read — тільки перегляд",
  add: "Add — подавати запити на обробку",
  download: "Download — скачувати дані",
  admin: "Admin — керування системою",
}

function UsersSection() {
  const [users, setUsers] = useState<{ username: string; permissions: string; created_at: string }[]>([])
  const [loading, setLoading] = useState(false)
  const [newUser, setNewUser] = useState({ username: "", password: "", permissions: "read" })
  const [msg, setMsg] = useState("")

  const load = useCallback(async () => {
    setLoading(true)
    try { const r = await apiFetch("/api/setup/users"); setUsers(r.users) } catch {}
    finally { setLoading(false) }
  }, [])

  useEffect(() => { load() }, [load])

  const add = async () => {
    if (!newUser.username.trim() || !newUser.password.trim()) { setMsg("Введіть логін та пароль"); return }
    setMsg("")
    try {
      await apiFetch("/api/setup/users", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(newUser)
      })
      setNewUser({ username: "", password: "", permissions: "read" })
      await load()
      setMsg("Користувача додано")
    } catch (e: any) { setMsg("Помилка: " + e.message) }
  }

  const remove = async (username: string) => {
    setMsg("")
    try {
      await apiFetch(`/api/setup/users/${encodeURIComponent(username)}`, { method: "DELETE" })
      await load()
    } catch (e: any) { setMsg("Помилка: " + e.message) }
  }

  return (
    <div className="card">
      <div className="card-section-title">Користувачі</div>
      <div className="setup-users-grid">
        <div style={{ fontWeight: 600, fontSize: 11, color: "var(--text-3)", marginBottom: 4 }}>Новий користувач</div>
        <div className="setup-add-row">
          <input className="filter-input" placeholder="Логін..." value={newUser.username}
            onChange={e => setNewUser(p => ({ ...p, username: e.target.value }))} style={{ width: 140 }} />
          <input className="filter-input" placeholder="Пароль..." type="password" value={newUser.password}
            onChange={e => setNewUser(p => ({ ...p, password: e.target.value }))} style={{ width: 140 }} />
          <select className="flt-select-sm" value={newUser.permissions}
            onChange={e => setNewUser(p => ({ ...p, permissions: e.target.value }))}>
            {Object.entries(PERMISSION_LABELS).map(([v, l]) => <option key={v} value={v}>{l}</option>)}
          </select>
          <button className="btn-primary" style={{ padding: "6px 16px", fontSize: 13 }} onClick={add}>+ Додати</button>
        </div>
        {msg && <div className="setup-msg">{msg}</div>}
      </div>

      {loading ? <div className="loading-center"><span className="spinner-lg" /></div> : (
        <table className="results-table" style={{ marginTop: 12 }}>
          <thead><tr><th>Логін</th><th>Доступ</th><th>Створено</th><th></th></tr></thead>
          <tbody>
            {users.length === 0 && (
              <tr><td colSpan={4} style={{ textAlign: "center", color: "var(--text-3)", padding: 16 }}>
                Немає користувачів — авторизація вимкнена
              </td></tr>
            )}
            {users.map(u => (
              <tr key={u.username}>
                <td style={{ fontFamily: "var(--mono)", fontWeight: 500 }}>{u.username}</td>
                <td><span className="service-tag">{u.permissions}</span></td>
                <td style={{ fontSize: 11, color: "var(--text-3)" }}>{u.created_at ? new Date(u.created_at).toLocaleDateString("uk-UA") : "—"}</td>
                <td><button className="setup-remove-btn" onClick={() => remove(u.username)}>&#10005;</button></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="setup-permissions-legend">
        {Object.entries(PERMISSION_LABELS).map(([k, v]) => (
          <div key={k} className="setup-perm-row">
            <span className="service-tag">{k}</span>
            <span style={{ fontSize: 12, color: "var(--text-2)" }}>{v.split("—")[1]?.trim()}</span>
          </div>
        ))}
      </div>
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
    <div className="page">
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
    </div>
  )
}
