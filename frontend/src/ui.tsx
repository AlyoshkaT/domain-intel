/**
 * Shared UI helpers: column sorting, "Підказки" (help) mode, country labels.
 * Used across all data grids so sort + hints behave identically everywhere.
 */
import {
  createContext, useContext, useState, useMemo, useRef,
  ReactNode, ThHTMLAttributes,
} from "react"
import { Lang } from "./i18n"

/* ── Help mode (Підказки toggle) ──────────────────────────────────────────
 * A single app-wide flag. When on, sortable headers reveal an ⓘ marker whose
 * hover tooltip explains the column. Wrap the app in <HelpProvider>. */
const HelpCtx = createContext<{ on: boolean; toggle: () => void }>({ on: false, toggle: () => {} })

export function HelpProvider({ children }: { children: ReactNode }) {
  const [on, setOn] = useState(false)
  const value = useMemo(() => ({ on, toggle: () => setOn(v => !v) }), [on])
  return <HelpCtx.Provider value={value}>{children}</HelpCtx.Provider>
}
export const useHelp = () => useContext(HelpCtx)

/* Nav button that toggles help mode. Must render inside <HelpProvider>. */
export function HelpToggle({ title = "Підказки" }: { title?: string }) {
  const { on, toggle } = useHelp()
  return (
    <button
      className={`theme-toggle help-toggle${on ? " active" : ""}`}
      onClick={toggle}
      title={title}
      aria-pressed={on}
    >ⓘ</button>
  )
}

/* ── Column sorting ───────────────────────────────────────────────────────
 * useSort keeps the sort state and returns the sorted rows. The accessor maps
 * (row, columnKey) → a comparable value. It's read through a ref so an inline
 * accessor doesn't force a re-sort of large lists on every render. */
export type SortDir = "asc" | "desc"
export interface SortState { key: string | null; dir: SortDir }

export function useSort<T>(
  rows: T[],
  accessor: (row: T, key: string) => unknown,
  initial: SortState = { key: null, dir: "asc" },
) {
  const [sort, setSort] = useState<SortState>(initial)
  const accRef = useRef(accessor)
  accRef.current = accessor

  const sorted = useMemo(() => {
    if (!sort.key) return rows
    const key = sort.key
    const dir = sort.dir === "asc" ? 1 : -1
    const acc = accRef.current
    return [...rows].sort((a, b) => {
      const va = acc(a, key) as any
      const vb = acc(b, key) as any
      const ea = va == null || va === ""
      const eb = vb == null || vb === ""
      if (ea && eb) return 0
      if (ea) return 1            // empty/undefined always sorts last
      if (eb) return -1
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * dir
      return String(va).localeCompare(String(vb), undefined, { numeric: true, sensitivity: "base" }) * dir
    })
  }, [rows, sort])

  // asc → desc → off (unsorted)
  const onSort = (key: string) => setSort(s =>
    s.key !== key ? { key, dir: "asc" }
    : s.dir === "asc" ? { key, dir: "desc" }
    : { key: null, dir: "asc" })

  return { sorted, sort, onSort }
}

/* A sortable <th>. Drop-in replacement for <th>, plus col/sort/onSort and an
 * optional hint (shown as an ⓘ tooltip when help mode is on). */
interface SortHeaderProps extends ThHTMLAttributes<HTMLTableCellElement> {
  col: string
  sort: SortState
  onSort: (key: string) => void
  hint?: string
  children: ReactNode
}
export function SortHeader({ col, sort, onSort, hint, children, className, title, ...rest }: SortHeaderProps) {
  const { on: helpOn } = useHelp()
  const active = sort.key === col
  const arrow = active ? (sort.dir === "asc" ? "▲" : "▼") : ""
  const showHint = helpOn && !!hint
  return (
    <th
      {...rest}
      onClick={() => onSort(col)}
      title={showHint ? hint : title}
      className={`th-sort${active ? " th-sort-active" : ""}${className ? " " + className : ""}`}
    >
      <span className="th-sort-label">{children}{showHint && <span className="th-hint">ⓘ</span>}</span>
      <span className="th-sort-arrow">{arrow}</span>
    </th>
  )
}

/* ── Country / region labels ──────────────────────────────────────────────
 * "UA" → "Ukraine (UA)" via the browser's built-in Intl.DisplayNames (no data
 * table to maintain). Non-ISO values are shown unchanged. */
const _dn: Record<string, Intl.DisplayNames> = {}
function regionDN(lang: Lang): Intl.DisplayNames {
  const loc = lang === "ua" ? "uk" : "en"
  if (!_dn[loc]) {
    try { _dn[loc] = new Intl.DisplayNames([loc], { type: "region" }) }
    catch { _dn[loc] = new Intl.DisplayNames(["en"], { type: "region" }) }
  }
  return _dn[loc]
}
export function regionLabel(code: string | null | undefined, lang: Lang = "en"): string {
  if (code == null || code === "") return "—"
  const c = String(code).trim()
  if (!/^[A-Za-z]{2}$/.test(c)) return c          // not an alpha-2 ISO code → as-is
  try {
    const name = regionDN(lang).of(c.toUpperCase())
    if (name && name.toUpperCase() !== c.toUpperCase()) return `${name} (${c.toUpperCase()})`
  } catch { /* fall through */ }
  return c.toUpperCase()
}
