// Dashboard.tsx — Charts reacting to Explorer filters
// Row 1: Country Map | Traffic Groups | CMS | EMS | Onsite Search
// Row 2: Category SW | Subcategory SW
import { useMemo } from "react"

interface Profile {
  domain: string; sw_visits?: number; sw_category?: string; sw_subcategory?: string
  sw_primary_region?: string; cms_list?: string; ems_list?: string; osearch?: string
}

const TRAFFIC_GROUPS = [
  { label: "SuperVIP >25M", min: 25_000_000, color: "#6366f1" },
  { label: "VIP >5M",       min: 5_000_000,  color: "#818cf8" },
  { label: "Large >2.5M",   min: 2_500_000,  color: "#22c55e" },
  { label: "Medium >200k",  min: 200_000,    color: "#4ade80" },
  { label: "Small >100k",   min: 100_000,    color: "#fbbf24" },
  { label: "Micro >10k",    min: 10_000,     color: "#fb923c" },
  { label: "Nano <10k",     min: 0,          color: "#f87171" },
]
const COLORS = ["#6366f1","#22c55e","#fbbf24","#f87171","#60a5fa","#a78bfa","#34d399","#fb923c","#f472b6","#94a3b8"]

function getGroup(v?: number) {
  if (!v || v <= 0) return "Nano <10k"
  for (const g of TRAFFIC_GROUPS) if (v >= g.min) return g.label
  return "Nano <10k"
}

function flag(code: string) {
  if (!code || code.length !== 2) return "🌍"
  return String.fromCodePoint(...code.toUpperCase().split("").map(c => 127397 + c.charCodeAt(0)))
}

interface Slice { label: string; count: number; color: string }

function topN(map: Map<string, number>, n: number): Slice[] {
  const sorted = [...map.entries()].sort((a, b) => b[1] - a[1])
  const top = sorted.slice(0, n).map(([label, count], i) => ({ label: label || "(порожнє)", count, color: COLORS[i % COLORS.length] }))
  const rest = sorted.slice(n).reduce((s, [, c]) => s + c, 0)
  if (rest > 0) top.push({ label: "Інші", count: rest, color: "#94a3b8" })
  return top
}

function Donut({ data, title }: { data: Slice[]; title: string }) {
  const total = data.reduce((s, d) => s + d.count, 0)
  if (!total) return null
  const sz = 120, cx = 60, cy = 60, r = 44, ir = 26
  let start = -Math.PI / 2
  const slices = data.map(item => {
    const pct = item.count / total
    const end = start + pct * 2 * Math.PI
    const [x1, y1] = [cx + r * Math.cos(start), cy + r * Math.sin(start)]
    const [x2, y2] = [cx + r * Math.cos(end), cy + r * Math.sin(end)]
    const [ix1, iy1] = [cx + ir * Math.cos(start), cy + ir * Math.sin(start)]
    const [ix2, iy2] = [cx + ir * Math.cos(end), cy + ir * Math.sin(end)]
    const lg = pct > 0.5 ? 1 : 0
    const d = `M${x1} ${y1}A${r} ${r} 0 ${lg} 1 ${x2} ${y2}L${ix2} ${iy2}A${ir} ${ir} 0 ${lg} 0 ${ix1} ${iy1}Z`
    const s = { d, color: item.color, label: item.label, count: item.count, pct }
    start = end
    return s
  })
  return (
    <div className="dash-chart">
      <div className="dash-chart-title">{title}</div>
      <div className="dash-chart-body">
        <svg width={sz} height={sz} style={{ flexShrink: 0 }}>
          {slices.map((s, i) => (
            <path key={i} d={s.d} fill={s.color} stroke="var(--bg)" strokeWidth={1.5}>
              <title>{s.label}: {s.count.toLocaleString()} ({(s.pct * 100).toFixed(1)}%)</title>
            </path>
          ))}
          <text x={cx} y={cy - 4} textAnchor="middle" fontSize={12} fontWeight={600} fill="var(--text)">{total.toLocaleString()}</text>
          <text x={cx} y={cy + 11} textAnchor="middle" fontSize={8} fill="var(--text-3)">доменів</text>
        </svg>
        <table className="dash-legend-table">
          <tbody>
            {data.map((item, i) => (
              <tr key={i} className="dash-legend-row">
                <td style={{width:14,paddingRight:4}}><span className="dash-legend-dot" style={{ background: item.color }} /></td>
                <td className="dash-legend-label">{item.label}</td>
                <td className="dash-legend-count">{item.count.toLocaleString()}</td>
                <td className="dash-legend-pct">{((item.count / total) * 100).toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function CountryBars({ data }: { data: Slice[] }) {
  if (!data.length) return null
  const max = data[0]?.count || 1
  const total = data.reduce((s, d) => s + d.count, 0)
  return (
    <div className="dash-chart">
      <div className="dash-chart-title">Country Map</div>
      <div className="dash-country-list">
        {data.slice(0, 12).map((item, i) => (
          <div key={i} className="dash-country-row">
            <span className="dash-country-flag">{flag(item.label)}</span>
            <span className="dash-country-code">{item.label}</span>
            <div className="dash-country-bar-wrap">
              <div className="dash-country-bar" style={{ width: `${(item.count / max) * 100}%`, background: item.color }} />
            </div>
            <span className="dash-country-count">{item.count.toLocaleString()}</span>
            <span className="dash-country-pct">{((item.count / total) * 100).toFixed(1)}%</span>
          </div>
        ))}
      </div>
    </div>
  )
}

export default function Dashboard({ profiles }: { profiles: Profile[] }) {
  const charts = useMemo(() => {
    const trafficMap = new Map<string, number>()
    TRAFFIC_GROUPS.forEach(g => trafficMap.set(g.label, 0))
    const countryMap = new Map<string, number>()
    const cmsMap = new Map<string, number>()
    const emsMap = new Map<string, number>()
    const searchMap = new Map<string, number>()
    const catMap = new Map<string, number>()
    const subMap = new Map<string, number>()
    let noRegion = 0

    for (const p of profiles) {
      const g = getGroup(p.sw_visits)
      trafficMap.set(g, (trafficMap.get(g) || 0) + 1)
      if (p.sw_primary_region) countryMap.set(p.sw_primary_region, (countryMap.get(p.sw_primary_region) || 0) + 1)
      else noRegion++
      const cms = p.cms_list?.trim() || "(порожнє)"; cmsMap.set(cms, (cmsMap.get(cms) || 0) + 1)
      const ems = p.ems_list?.trim() || "(порожнє)"; emsMap.set(ems, (emsMap.get(ems) || 0) + 1)
      const srch = p.osearch?.trim() || "(порожнє)"; searchMap.set(srch, (searchMap.get(srch) || 0) + 1)
      const cat = p.sw_category?.trim() || "(порожнє)"; catMap.set(cat, (catMap.get(cat) || 0) + 1)
      const sub = p.sw_subcategory?.trim() || "(порожнє)"; subMap.set(sub, (subMap.get(sub) || 0) + 1)
    }

    if (noRegion > 0) countryMap.set("(без регіону)", noRegion)

    const trafficData: Slice[] = TRAFFIC_GROUPS
      .map((g, i) => ({ label: g.label, count: trafficMap.get(g.label) || 0, color: g.color }))
      .filter(d => d.count > 0)

    return {
      trafficData,
      countryData: topN(countryMap, 10),
      cmsData:     topN(cmsMap, 8),
      emsData:     topN(emsMap, 8),
      searchData:  topN(searchMap, 8),
      catData:     topN(catMap, 8),
      subData:     topN(subMap, 8),
    }
  }, [profiles])

  if (!profiles.length) return null

  return (
    <div>
      {/* Row 1: Traffic Groups | CMS | EMS | Onsite Search — 4 per row */}
      <div className="dashboard dashboard-4">
        <Donut data={charts.trafficData} title="Traffic Groups" />
        <Donut data={charts.cmsData}     title="CMS" />
        <Donut data={charts.emsData}     title="EMS" />
        <Donut data={charts.searchData}  title="Onsite Search" />
      </div>
      {/* Row 2: Country Map | Category SW | Subcategory SW | (empty) */}
      <div className="dashboard dashboard-4">
        <CountryBars data={charts.countryData} />
        <Donut data={charts.catData} title="Category SW" />
        <Donut data={charts.subData} title="Subcategory SW" />
      </div>
    </div>
  )
}
