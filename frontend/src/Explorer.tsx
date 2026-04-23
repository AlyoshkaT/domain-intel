// Explorer.tsx — BQ Explorer with multi-filters
import { useState, useEffect, useCallback, useRef } from "react"

const API = ""
async function apiFetch(path: string, opts?: RequestInit) {
  const res = await fetch(API + path, opts)
  if (!res.ok) { const e = await res.json().catch(() => ({ detail: res.statusText })); throw new Error(e.detail) }
  return res.json()
}

interface FilterValue { value: string; count: number }
type TextFilterType  = "all" | "contains" | "not_contains" | "empty" | "not_empty"
type NumFilterType   = "all" | "gt" | "lt" | "between"
type MultiFilterType = "all" | "in" | "not_in" | "empty" | "not_empty"
interface TextFilter  { type: TextFilterType;  value: string }
interface NumFilter   { type: NumFilterType;   value: string; min: string; max: string }
interface MultiFilter { type: MultiFilterType; selected: string[]; search: string }
type FilterState = {
  domain: TextFilter; cms_list: MultiFilter; wcms_name: MultiFilter
  osearch: MultiFilter; ems_list: MultiFilter; ai_category: MultiFilter
  ai_is_ecommerce: MultiFilter; sw_category: MultiFilter; sw_primary_region: MultiFilter
  sw_visits: NumFilter; sw_primary_region_pct: NumFilter
}
const defaultText  = (): TextFilter  => ({ type:"all", value:"" })
const defaultNum   = (): NumFilter   => ({ type:"all", value:"", min:"", max:"" })
const defaultMulti = (): MultiFilter => ({ type:"all", selected:[], search:"" })
const defaultFilters = (): FilterState => ({
  domain:defaultText(), cms_list:defaultMulti(), wcms_name:defaultMulti(),
  osearch:defaultMulti(), ems_list:defaultMulti(), ai_category:defaultMulti(),
  ai_is_ecommerce:defaultMulti(), sw_category:defaultMulti(), sw_primary_region:defaultMulti(),
  sw_visits:defaultNum(), sw_primary_region_pct:defaultNum(),
})
const MULTI_FIELDS = ["cms_list","wcms_name","osearch","ems_list","ai_category","ai_is_ecommerce","sw_category","sw_primary_region"]

function MultiSelect({ field, filter, allValues, onChange }: {
  field:string; filter:MultiFilter; allValues:FilterValue[]; onChange:(f:MultiFilter)=>void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  useEffect(()=>{
    const h=(e:MouseEvent)=>{ if(ref.current&&!ref.current.contains(e.target as Node)) setOpen(false) }
    document.addEventListener("mousedown",h); return()=>document.removeEventListener("mousedown",h)
  },[])
  const filtered = allValues.filter(v=>!filter.search||v.value.toLowerCase().includes(filter.search.toLowerCase()))
  const toggleValue=(v:string)=>{
    const sel=filter.selected.includes(v)?filter.selected.filter(s=>s!==v):[...filter.selected,v]
    onChange({...filter,selected:sel,type:sel.length>0?"in":"all"})
  }
  const hasFilter=filter.type!=="all"||filter.selected.length>0
  return (
    <div className="flt-multi" ref={ref}>
      <select className="flt-select-sm" value={filter.type}
        onChange={e=>onChange({...filter,type:e.target.value as MultiFilterType,selected:[]})}>
        <option value="all">Всі</option><option value="in">Включити</option>
        <option value="not_in">Виключити</option><option value="empty">Порожнє</option>
        <option value="not_empty">Не порожнє</option>
      </select>
      {(filter.type==="in"||filter.type==="not_in")&&(
        <div className="flt-dropdown-wrap">
          <button className={`flt-dropdown-btn ${hasFilter?"active":""}`} onClick={()=>setOpen(!open)}>
            <span>{filter.selected.length===0?`Всі (${allValues.length})`:`Вибрано: ${filter.selected.length}`}</span>
            <span className="flt-chevron">{open?"▴":"▾"}</span>
          </button>
          {open&&(
            <div className="flt-dropdown">
              <input className="flt-search-input" placeholder="Пошук у списку..."
                value={filter.search} onChange={e=>onChange({...filter,search:e.target.value})} autoFocus/>
              {filter.selected.length>0&&(
                <div className="flt-clear-sel" onClick={()=>onChange({...filter,selected:[],type:"all",search:""})}>
                  ✕ Скинути вибір ({filter.selected.length})
                </div>
              )}
              <div className="flt-options">
                {filtered.length===0&&<div className="flt-loading">Нічого не знайдено</div>}
                {filtered.map(v=>(
                  <label key={v.value} className="flt-option">
                    <input type="checkbox" checked={filter.selected.includes(v.value)} onChange={()=>toggleValue(v.value)}/>
                    <span className="flt-option-text">{v.value}</span>
                    <span className="flt-option-count">{v.count}</span>
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
      {filter.selected.length>0&&(
        <div className="flt-selected-tags">
          {filter.selected.slice(0,3).map(v=>(
            <span key={v} className="flt-tag" onClick={()=>toggleValue(v)}>{v} ✕</span>
          ))}
          {filter.selected.length>3&&<span className="flt-tag flt-tag-more">+{filter.selected.length-3}</span>}
        </div>
      )}
    </div>
  )
}

function NumericFilter({ filter, onChange }:{ filter:NumFilter; onChange:(f:NumFilter)=>void }) {
  return (
    <div className="flt-num">
      <select className="flt-select-sm" value={filter.type} onChange={e=>onChange({...filter,type:e.target.value as NumFilterType})}>
        <option value="all">Всі</option><option value="gt">Більше ніж</option>
        <option value="lt">Менше ніж</option><option value="between">Від — До</option>
      </select>
      {(filter.type==="gt"||filter.type==="lt")&&(
        <input className="flt-num-input" type="number" placeholder="Значення" value={filter.value} onChange={e=>onChange({...filter,value:e.target.value})}/>
      )}
      {filter.type==="between"&&(
        <div className="flt-between">
          <input className="flt-num-input" type="number" placeholder="Від" value={filter.min} onChange={e=>onChange({...filter,min:e.target.value})}/>
          <span className="flt-between-sep">—</span>
          <input className="flt-num-input" type="number" placeholder="До" value={filter.max} onChange={e=>onChange({...filter,max:e.target.value})}/>
        </div>
      )}
    </div>
  )
}

function DomainFilter({ filter, allValues, onChange }:{ filter:TextFilter; allValues:FilterValue[]; onChange:(f:TextFilter)=>void }) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  useEffect(()=>{
    const h=(e:MouseEvent)=>{ if(ref.current&&!ref.current.contains(e.target as Node)) setOpen(false) }
    document.addEventListener("mousedown",h); return()=>document.removeEventListener("mousedown",h)
  },[])
  const filtered = allValues.filter(v=>!filter.value||v.value.toLowerCase().includes(filter.value.toLowerCase()))
  return (
    <div className="flt-text" ref={ref}>
      <select className="flt-select-sm" value={filter.type} onChange={e=>onChange({...filter,type:e.target.value as TextFilterType})}>
        <option value="all">Всі</option><option value="contains">Містить</option>
        <option value="not_contains">Не містить</option><option value="empty">Порожнє</option>
        <option value="not_empty">Не порожнє</option>
      </select>
      {(filter.type==="contains"||filter.type==="not_contains")&&(
        <div style={{position:"relative"}}>
          <input className="flt-num-input" placeholder="Домен або текст..."
            value={filter.value}
            onChange={e=>{ onChange({...filter,value:e.target.value}); setOpen(true) }}
            onFocus={()=>setOpen(true)}/>
          {open&&filter.value&&filtered.length>0&&(
            <div className="flt-dropdown" style={{position:"absolute",top:"calc(100% + 4px)",left:0,right:0,zIndex:300}}>
              <div className="flt-options">
                {filtered.slice(0,15).map(v=>(
                  <div key={v.value} className="flt-option" onClick={()=>{ onChange({...filter,value:v.value}); setOpen(false) }}>
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

function FilterPanel({ filters, fieldValues, onChange, onSearch, loading, activeCount }:{
  filters:FilterState; fieldValues:Record<string,FilterValue[]>
  onChange:(f:FilterState)=>void; onSearch:()=>void; loading:boolean; activeCount:number
}) {
  const upd=(key:keyof FilterState,val:any)=>onChange({...filters,[key]:val})
  const sections=[
    {key:"domain",label:"Domain",type:"text"},
    {key:"sw_visits",label:"Traffic",type:"num"},
    {key:"cms_list",label:"CMS",type:"multi"},
    {key:"wcms_name",label:"WhatCMS",type:"multi"},
    {key:"osearch",label:"oSearch",type:"multi"},
    {key:"ems_list",label:"EMS",type:"multi"},
    {key:"ai_category",label:"AI Category",type:"multi"},
    {key:"ai_is_ecommerce",label:"AI Ecomm",type:"multi"},
    {key:"sw_category",label:"Category SW",type:"multi"},
    {key:"sw_primary_region",label:"Region",type:"multi"},
    {key:"sw_primary_region_pct",label:"Region %",type:"num"},
  ] as const
  return (
    <div className="filter-panel">
      <div className="filter-panel-header">
        <span className="filter-panel-title">Фільтри</span>
        <button className="flt-reset-btn" onClick={()=>onChange(defaultFilters())}>Скинути</button>
      </div>
      {sections.map(s=>(
        <div key={s.key} className="filter-section">
          <div className="filter-section-label">{s.label}</div>
          {s.type==="text"&&<DomainFilter filter={filters[s.key] as TextFilter} allValues={fieldValues[s.key]||[]} onChange={v=>upd(s.key,v)}/>}
          {s.type==="num"&&<NumericFilter filter={filters[s.key] as NumFilter} onChange={v=>upd(s.key,v)}/>}
          {s.type==="multi"&&<MultiSelect field={s.key} filter={filters[s.key] as MultiFilter} allValues={fieldValues[s.key]||[]} onChange={v=>upd(s.key,v)}/>}
        </div>
      ))}
      <button className="btn-primary explorer-search-btn" onClick={onSearch} disabled={loading}>
        {loading?<span className="spinner"/>:"🔍"}
        {loading?"Пошук...":"Застосувати"}
        {activeCount>0&&<span className="flt-count-badge">{activeCount}</span>}
      </button>
    </div>
  )
}

function cell(v?:string|null){ return v&&v.trim()?v:"—" }

interface ExploreResult {
  domain:string; sw_visits?:number; cms_list?:string; wcms_name?:string
  osearch?:string; ems_list?:string; ai_category?:string; ai_is_ecommerce?:string
  ai_industry?:string; bw_vertical?:string; sw_category?:string; sw_subcategory?:string
  sw_description?:string; sw_title?:string; company_name?:string
  sw_primary_region?:string; sw_primary_region_pct?:number; status?:string
}

function SyncButton({ onSync }: { onSync: () => void }) {
  const [status, setStatus] = useState<any>({})
  const [syncing, setSyncing] = useState(false)

  const loadStatus = useCallback(async () => {
    try {
      const s = await apiFetch("/api/explore/sync/status")
      setStatus(s)
      if (!s.running) setSyncing(false)
    } catch {}
  }, [])

  useEffect(() => {
    loadStatus()
    const iv = setInterval(loadStatus, 2000)
    return () => clearInterval(iv)
  }, [loadStatus])

  const handleSync = async () => {
    setSyncing(true)
    await apiFetch("/api/explore/refresh", { method: "POST" })
    setTimeout(onSync, 5000)
  }

  const isRunning = syncing || status.running

  return (
    <div style={{display:"flex",flexDirection:"column",alignItems:"flex-end",gap:3}}>
      <div style={{display:"flex",alignItems:"center",gap:6}}>
        {status.last_sync && !isRunning && (
          <span style={{fontSize:10,color:"var(--text-3)"}}>
            {new Date(status.last_sync).toLocaleString("uk-UA")}
            {status.total_domains ? ` · ${status.total_domains.toLocaleString()} доменів` : ""}
          </span>
        )}
        <button className="flt-reset-btn" onClick={handleSync} disabled={isRunning}>
          {isRunning ? "⏳" : "↻"} {isRunning ? "Синхронізація..." : "Синхронізувати БД"}
        </button>
      </div>
      {isRunning && status.progress && (
        <span style={{fontSize:10,color:"var(--accent)",fontFamily:"var(--mono)"}}>{status.progress}</span>
      )}
      {status.error && (
        <span style={{fontSize:10,color:"var(--red)"}}>Помилка: {status.error.slice(0,80)}</span>
      )}
    </div>
  )
}

export default function ExplorerPage() {
  const [filters, setFilters] = useState<FilterState>(defaultFilters())
  const [fieldValues, setFieldValues] = useState<Record<string,FilterValue[]>>({})
  const [results, setResults] = useState<ExploreResult[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [stats, setStats] = useState<any>({})
  const [offset, setOffset] = useState(0)
  const [sheetsLoading, setSheetsLoading] = useState(false)
  const [sheetsUrl, setSheetsUrl] = useState<string|null>(null)
  const LIMIT = 100

  useEffect(()=>{
    apiFetch("/api/explore/stats").then(setStats).catch(()=>{})
    // Load all field values upfront
    Promise.all([...MULTI_FIELDS,"domain"].map(async field=>{
      try{ const d=await apiFetch(`/api/explore/values/${field}`); return [field,d.values||[]] as [string,FilterValue[]] }
      catch{ return [field,[]] as [string,FilterValue[]] }
    })).then(entries=>setFieldValues(Object.fromEntries(entries)))
    // Load initial results
    doSearch(defaultFilters(),0)
  },[])

  const buildPayload=(f:FilterState,off:number)=>{
    const af:any={}
    if(f.domain.type!=="all"&&(f.domain.value||f.domain.type==="empty"||f.domain.type==="not_empty"))
      af.domain={type:f.domain.type,value:f.domain.value}
    for(const field of MULTI_FIELDS){
      const flt=f[field as keyof FilterState] as MultiFilter
      if(flt.type==="empty"||flt.type==="not_empty") af[field]={type:flt.type}
      else if((flt.type==="in"||flt.type==="not_in")&&flt.selected.length>0) af[field]={type:flt.type,values:flt.selected}
    }
    for(const field of ["sw_visits","sw_primary_region_pct"] as (keyof FilterState)[]){
      const flt=f[field] as NumFilter
      if(flt.type==="gt"&&flt.value) af[field]={type:"gt",value:parseFloat(flt.value)}
      else if(flt.type==="lt"&&flt.value) af[field]={type:"lt",value:parseFloat(flt.value)}
      else if(flt.type==="between"&&flt.min&&flt.max) af[field]={type:"between",min:parseFloat(flt.min),max:parseFloat(flt.max)}
    }
    return {filters:af,limit:LIMIT,offset:off}
  }

  const doSearch=async(f:FilterState,off:number)=>{
    setLoading(true); setSheetsUrl(null)
    try{
      const data=await apiFetch("/api/explore/search",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(buildPayload(f,off))})
      setResults(data.results||[]); setTotal(data.total||0)
    }catch{setResults([]);setTotal(0)}
    finally{setLoading(false)}
  }

  const handleSearch=()=>{ setOffset(0); doSearch(filters,0) }
  const handlePrev=()=>{ const o=Math.max(0,offset-LIMIT); setOffset(o); doSearch(filters,o) }
  const handleNext=()=>{ const o=offset+LIMIT; setOffset(o); doSearch(filters,o) }

  const handleExportSheets=async()=>{
    setSheetsLoading(true)
    try{
      await apiFetch("/api/explore/export/sheets",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({results})})
      for(let i=0;i<10;i++){
        await new Promise(r=>setTimeout(r,2000))
        const data=await apiFetch("/api/explore/export/sheets/url")
        if(data.url){setSheetsUrl(data.url);break}
      }
    }catch{}finally{setSheetsLoading(false)}
  }

  const activeCount=Object.values(filters).filter((f:any)=>f.type&&f.type!=="all"||(f.selected&&f.selected.length>0)).length

  return (
    <div className="explorer-layout">
      <aside className="explorer-sidebar">
        <FilterPanel filters={filters} fieldValues={fieldValues} onChange={setFilters} onSearch={handleSearch} loading={loading} activeCount={activeCount}/>
      </aside>
      <main className="explorer-main">
        <div className="stats-grid" style={{marginBottom:16}}>
          {[{label:"Доменів",value:stats.total_domains},{label:"Job-ів",value:stats.total_jobs},{label:"З CMS",value:stats.with_cms},{label:"З трафіком",value:stats.with_traffic},{label:"З AI",value:stats.with_ai},{label:"З EMS",value:stats.with_ems}].map(s=>(
            <div key={s.label} className="stat-card"><div className="stat-label">{s.label}</div><div className="stat-value">{s.value?.toLocaleString()||"—"}</div></div>
          ))}
        </div>
        <div className="explorer-results-header">
          <div style={{display:"flex",alignItems:"center",gap:8}}>
            <span className="explorer-total">{loading?"Завантаження...":`Знайдено: ${total.toLocaleString()} доменів`}</span>
            <SyncButton onSync={()=>{ setTimeout(()=>doSearch(filters,0), 3000) }}/>
          </div>
          <div style={{display:"flex",gap:8,alignItems:"center"}}>
            {results.length>0&&(sheetsUrl
              ?<a href={sheetsUrl} target="_blank" rel="noopener" className="btn-export btn-sheets">📊 Відкрити Sheets</a>
              :<button className="btn-export btn-sheets" onClick={handleExportSheets} disabled={sheetsLoading}>{sheetsLoading?"⏳":"📊"} Sheets</button>
            )}
            {total>LIMIT&&(
              <div className="pagination">
                <button className="page-btn" onClick={handlePrev} disabled={offset===0||loading}>←</button>
                <span className="page-info">{offset+1}–{Math.min(offset+LIMIT,total)} / {total.toLocaleString()}</span>
                <button className="page-btn" onClick={handleNext} disabled={offset+LIMIT>=total||loading}>→</button>
              </div>
            )}
          </div>
        </div>
        {loading&&<div className="loading-center"><span className="spinner-lg"/></div>}
        {!loading&&results.length>0&&(
          <div className="table-wrap">
            <table className="results-table">
              <thead><tr>
                <th>Domain</th><th>Traffic</th><th>CMS</th><th>WhatCMS</th>
                <th>oSearch</th><th>EMS</th><th>AI Category</th><th>AI Ecomm</th>
                <th>AI Industry</th><th>Category SW</th><th>Subcategory</th>
                <th>Description</th><th>Title</th><th>Company</th><th>Region</th><th>Region %</th>
              </tr></thead>
              <tbody>
                {results.map((r,i)=>(
                  <tr key={`${r.domain}-${i}`}>
                    <td className="td-domain"><a href={`https://${r.domain}`} target="_blank" rel="noopener">{r.domain}</a></td>
                    <td className="td-traffic">{r.sw_visits?r.sw_visits.toLocaleString("en-US"):"—"}</td>
                    <td>{cell(r.cms_list)}</td><td>{cell(r.wcms_name)}</td>
                    <td>{cell(r.osearch)}</td><td>{cell(r.ems_list)}</td>
                    <td>{cell(r.ai_category)}</td><td>{cell(r.ai_is_ecommerce)}</td>
                    <td>{cell(r.ai_industry)}</td><td>{cell(r.sw_category)}</td>
                    <td>{cell(r.sw_subcategory)}</td>
                    <td className="td-desc" title={r.sw_description||""}>{cell(r.sw_description)}</td>
                    <td>{cell(r.sw_title)}</td><td>{cell(r.company_name)}</td>
                    <td>{cell(r.sw_primary_region)}</td>
                    <td>{r.sw_primary_region_pct!=null?`${r.sw_primary_region_pct}%`:"—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {!loading&&results.length===0&&<div className="empty-state">Нічого не знайдено.</div>}
      </main>
    </div>
  )
}
