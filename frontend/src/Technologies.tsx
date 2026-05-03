import { useState, useCallback, useMemo } from "react"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

interface Series { name: string; data: number[]; total: number }
interface TableRow { domain: string; name: string; description: string; link: string; tag: string; first_detected: string; last_detected: string }
interface AggResult { periods: string[]; series: Series[]; table: TableRow[]; unknown_count: number; unknown_top: [string,number][]; total_domains: number; error?: string }

const COLORS = ["#6366f1","#22c55e","#f59e0b","#ef4444","#3b82f6","#a855f7","#14b8a6","#f97316","#ec4899","#64748b","#84cc16","#06b6d4","#8b5cf6","#d97706","#059669"]

function yTicks(maxVal: number, n = 5): number[] {
  if (maxVal <= 0) return [0]
  const ticks: number[] = []
  const seen = new Set<number>()
  for (let i = 0; i < n; i++) {
    const v = Math.round(maxVal * i / (n - 1))
    if (!seen.has(v)) { seen.add(v); ticks.push(v) }
  }
  return ticks
}

function LineChart({ periods, series, hovered, onHover }: {
  periods: string[]; series: Series[]
  hovered: string|null; onHover: (n: string|null) => void
}) {
  const W=900, H=300, PT=20, PR=20, PB=60, PL=50
  const plotW=W-PL-PR, plotH=H-PT-PB
  const maxVal = Math.max(1, ...series.flatMap(s=>s.data))
  const xStep = plotW / Math.max(1, periods.length - 1)
  const yScale = (v: number) => plotH - (v/maxVal)*plotH
  const path = (data: number[]) => data.map((v,i)=>`${i===0?"M":"L"} ${PL+i*xStep} ${PT+yScale(v)}`).join(" ")
  const labelStep = Math.ceil(periods.length/12)
  const ticks = yTicks(maxVal)
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{width:"100%",height:H,overflow:"visible"}}>
      {ticks.map(val=>{
        const y=PT+plotH*(1-val/maxVal)
        return <g key={val}>
          <line x1={PL} y1={y} x2={W-PR} y2={y} stroke="var(--border)" strokeWidth={1}/>
          <text x={PL-6} y={y+4} textAnchor="end" fontSize={10} fill="var(--text-3)">{val}</text>
        </g>
      })}
      {series.map((s,i)=>{
        const isHovered = hovered === s.name
        const isOther = hovered !== null && !isHovered
        return (
          <path key={s.name} d={path(s.data)} fill="none"
            stroke={COLORS[i%COLORS.length]}
            strokeWidth={isHovered ? 3 : isOther ? 1 : 2}
            opacity={isHovered ? 1 : isOther ? 0.12 : 0.85}
            style={{cursor:"pointer",transition:"opacity 0.15s,stroke-width 0.15s"}}
            onMouseEnter={()=>onHover(s.name)}
            onMouseLeave={()=>onHover(null)}>
            <title>{s.name}: {s.total.toLocaleString()}</title>
          </path>
        )
      })}
      {periods.map((p,i)=>i%labelStep===0?(
        <text key={p} x={PL+i*xStep} y={H-PB+16} textAnchor="middle" fontSize={9} fill="var(--text-3)"
          transform={`rotate(-45,${PL+i*xStep},${H-PB+16})`}>{p}</text>
      ):null)}
    </svg>
  )
}

function Legend({ series, visibleSet, onToggle, hovered, onHover }: {
  series: Series[]; visibleSet: Set<string>; onToggle: (n:string)=>void
  hovered: string|null; onHover: (n: string|null) => void
}) {
  return (
    <div className="tech-legend">
      {series.slice(0,15).map((s,i)=>{
        const isHovered = hovered === s.name
        const isOther = hovered !== null && !isHovered
        return (
          <div key={s.name}
            className={`tech-legend-item${!visibleSet.has(s.name)?" dimmed":""}${isHovered?" highlighted":""}${isOther?" legend-dim":""}`}
            onClick={()=>onToggle(s.name)}
            onMouseEnter={()=>onHover(s.name)}
            onMouseLeave={()=>onHover(null)}>
            <span className="tech-legend-dot" style={{
              background:COLORS[i%COLORS.length],
              transform: isHovered ? "scale(1.4)" : "scale(1)",
              transition:"transform 0.15s"
            }}/>
            <span className="tech-legend-name">{s.name}</span>
            <span className="tech-legend-count">{s.total.toLocaleString()}</span>
          </div>
        )
      })}
    </div>
  )
}

export default function TechnologiesPage({ domains = [], onBack }: { domains?: string[]; onBack?: () => void }) {
  const now = new Date()
  const defaultTo = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}`
  const ago = new Date(now); ago.setMonth(ago.getMonth()-24)
  const defaultFrom = `${ago.getFullYear()}-${String(ago.getMonth()+1).padStart(2,"0")}`

  const [dateFrom, setDateFrom] = useState(defaultFrom)
  const [dateTo, setDateTo] = useState(defaultTo)
  const [granularity, setGranularity] = useState<"month"|"quarter"|"year">("month")
  const [showUnknown, setShowUnknown] = useState(false)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<AggResult|null>(null)
  const [visible, setVisible] = useState<Set<string>>(new Set())
  const [tableFilter, setTableFilter] = useState("")
  const [uniqueOnly, setUniqueOnly] = useState(false)
  const [hoveredSeries, setHoveredSeries] = useState<string|null>(null)

  const load = useCallback(async()=>{
    setLoading(true)
    try {
      const data = await apiFetch("/api/technologies/aggregate",{
        method:"POST",headers:{"Content-Type":"application/json"},
        body:JSON.stringify({date_from:dateFrom,date_to:dateTo,granularity,show_unknown:showUnknown,domains:domains.slice(0,10000)})
      })
      setResult(data)
      setVisible(new Set(data.series?.slice(0,15).map((s:Series)=>s.name)||[]))
    } catch(e){console.error(e)}
    finally{setLoading(false)}
  },[dateFrom,dateTo,granularity,showUnknown])

  const toggleVisible=(name:string)=>setVisible(prev=>{const n=new Set(prev);n.has(name)?n.delete(name):n.add(name);return n})
  const filteredSeries=useMemo(()=>result?.series.filter(s=>visible.has(s.name))||[],[result,visible])
  const filteredTable=useMemo(()=>{
    let rows = result?.table.filter(r=>!tableFilter||r.domain.toLowerCase().includes(tableFilter.toLowerCase())||r.name.toLowerCase().includes(tableFilter.toLowerCase())||r.tag.toLowerCase().includes(tableFilter.toLowerCase()))||[]
    if(uniqueOnly){
      const seen = new Map<string,TableRow>()
      for(const r of rows){
        const key = `${r.domain}|${r.name}`
        const existing = seen.get(key)
        if(!existing || r.last_detected > existing.last_detected) seen.set(key, r)
      }
      rows = [...seen.values()]
    }
    return rows
  },[result,tableFilter,uniqueOnly])

  const exportCSV = useCallback(()=>{
    const cols = ["domain","name","tag","first_detected","last_detected","description","link"]
    const rows = filteredTable.map(r => cols.map(h => `"${String((r as any)[h]||"").replace(/"/g,'""')}"`).join(","))
    const csv = [cols.join(","), ...rows].join("\n")
    const a = document.createElement("a")
    a.href = URL.createObjectURL(new Blob(["﻿"+csv],{type:"text/csv;charset=utf-8"}))
    a.download = `technologies_${new Date().toISOString().slice(0,10)}.csv`; a.click()
  },[filteredTable])

  const exportXLSX = useCallback(async()=>{
    try {
      const cols = ["domain","name","tag","first_detected","last_detected","description","link"]
      const res = await fetch("/api/technologies/export/xlsx",{
        method:"POST",headers:{"Content-Type":"application/json"},
        body:JSON.stringify({rows:filteredTable,columns:cols})
      })
      if(!res.ok) throw new Error("Export failed")
      const blob = await res.blob()
      const a = document.createElement("a"); a.href = URL.createObjectURL(blob)
      a.download = `technologies_${new Date().toISOString().slice(0,10)}.xlsx`; a.click()
    } catch { alert("XLSX export error") }
  },[filteredTable])

  return (
    <div className="page page-wide">
      <div className="page-header">
        <h1 className="page-title">Used Technologies</h1>
        <span style={{fontSize:12,color:"var(--text-3)"}}>{domains.length > 0 ? `${domains.length.toLocaleString()} відфільтрованих доменів` : "Всі домени"}</span>
        {onBack && <button className="back-btn" onClick={onBack}>&#8592; Назад до Explorer</button>}
      </div>
      <div className="card" style={{marginBottom:12}}>
        <div className="tech-filters">
          <div className="tech-filter-group">
            <label className="tech-filter-label">Від</label>
            <input type="month" className="flt-num-input" value={dateFrom} onChange={e=>setDateFrom(e.target.value)}/>
          </div>
          <div className="tech-filter-group">
            <label className="tech-filter-label">До</label>
            <input type="month" className="flt-num-input" value={dateTo} onChange={e=>setDateTo(e.target.value)}/>
          </div>
          <div className="tech-filter-group">
            <label className="tech-filter-label">Деталізація</label>
            <div className="gran-btns">
              {(["month","quarter","year"] as const).map(g=>(
                <button key={g} className={`gran-btn${granularity===g?" active":""}`} onClick={()=>setGranularity(g)}>
                  {g==="month"?"Місяці":g==="quarter"?"Квартали":"Роки"}
                </button>
              ))}
            </div>
          </div>
          <div className="tech-filter-group">
            <label className="tech-filter-label">Невідомі</label>
            <button className={`service-toggle ${showUnknown?"active":""}`} style={{padding:"5px 12px"}} onClick={()=>setShowUnknown(!showUnknown)}>
              <div className="service-toggle-dot"/>
              <span className="service-toggle-label">Показати</span>
            </button>
          </div>
          <button className="btn-search" onClick={load} disabled={loading}>{loading?"⏳":"🔍"} Застосувати</button>
          <div className="tech-filter-group">
            <label className="tech-filter-label">Унікальність</label>
            <button className={`service-toggle ${uniqueOnly?"active":""}`} style={{padding:"5px 12px"}} onClick={()=>setUniqueOnly(!uniqueOnly)}>
              <div className="service-toggle-dot"/>
              <span className="service-toggle-label">Тільки остання</span>
            </button>
          </div>
        </div>
        {result?.unknown_count?<div style={{fontSize:11,color:"var(--text-3)",marginTop:8}}>
          Невідомих: {result.unknown_count.toLocaleString()}
          {result.unknown_top.length>0&&` (топ: ${result.unknown_top.slice(0,5).map(([n])=>n).join(", ")})`}
        </div>:null}
      </div>

      {loading&&<div className="loading-center"><span className="spinner-lg"/></div>}

      {!loading&&result&&result.series.length>0&&(
        <>
          <div className="card" style={{marginBottom:12}}>
            <div className="card-section-title">Використання технологій у часі</div>
            <div style={{overflowX:"auto"}}>
              <LineChart periods={result.periods} series={filteredSeries}
                hovered={hoveredSeries} onHover={setHoveredSeries}/>
            </div>
            <Legend series={result.series} visibleSet={visible} onToggle={toggleVisible}
              hovered={hoveredSeries} onHover={setHoveredSeries}/>
            <div style={{fontSize:11,color:"var(--text-3)",marginTop:8}}>
              Натисніть на технологію щоб сховати/показати лінію · Наведіть щоб підсвітити
            </div>
          </div>
          <div className="filter-row" style={{marginBottom:8}}>
            <input className="filter-input" placeholder="Фільтр по домену, технології, тегу..." value={tableFilter} onChange={e=>setTableFilter(e.target.value)}/>
            <span className="filter-count">{filteredTable.length.toLocaleString()} записів</span>
            <div style={{display:"flex",gap:6}}>
              <button className="btn-export" onClick={exportCSV}>&#8595; CSV</button>
              <button className="btn-export" onClick={exportXLSX}>&#8595; XLSX</button>
            </div>
          </div>
          <div className="table-wrap table-fixed-height">
            <table className="results-table">
              <thead><tr>
                <th>Domain</th><th>Technology</th><th>Tag</th>
                <th>First Detected</th><th>Last Detected</th><th>Description</th><th>Link</th>
              </tr></thead>
              <tbody>
                {filteredTable.map((r,i)=>(
                  <tr key={`${r.domain}-${r.name}-${i}`}>
                    <td className="td-domain"><a href={`https://${r.domain}`} target="_blank" rel="noopener">{r.domain}</a></td>
                    <td style={{fontWeight:500}}>{r.name}</td>
                    <td><span className="service-tag">{r.tag}</span></td>
                    <td style={{fontFamily:"var(--mono)",fontSize:11}}>{r.first_detected}</td>
                    <td style={{fontFamily:"var(--mono)",fontSize:11}}>{r.last_detected}</td>
                    <td className="td-desc" title={r.description}>{r.description||"—"}</td>
                    <td>{r.link?<a href={r.link} target="_blank" rel="noopener" style={{fontSize:11}}>&#8599;</a>:"—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
      {!loading&&result&&result.series.length===0&&<div className="empty-state">Немає даних для обраного періоду.</div>}
      {!loading&&!result&&<div className="empty-state">Натисніть "Застосувати" для завантаження даних.</div>}
    </div>
  )
}
