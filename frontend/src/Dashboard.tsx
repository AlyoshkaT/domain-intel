// Dashboard.tsx — Charts reacting to Explorer filters, all clickable → filter
import { useMemo, useState, useEffect, useRef } from "react"
import { t, type Lang } from "./i18n"
import {
  ComposableMap,
  Geographies,
  Geography,
  ZoomableGroup,
  Annotation,
} from "react-simple-maps"

interface Profile {
  domain: string; sw_visits?: number; sw_category?: string; sw_subcategory?: string
  sw_primary_region?: string; cms_list?: string; ems_list?: string; osearch?: string
}

export const TRAFFIC_GROUPS = [
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

interface Slice { label: string; count: number; color: string; items?: { label: string; count: number }[] }

function topN(map: Map<string, number>, n: number, lang: Lang): Slice[] {
  const sorted = [...map.entries()].sort((a, b) => b[1] - a[1])
  const top: Slice[] = sorted.slice(0, n).map(([label, count], i) => ({ label: label || t('dash_empty', lang), count, color: COLORS[i % COLORS.length] }))
  const restEntries = sorted.slice(n)
  const rest = restEntries.reduce((s, [, c]) => s + c, 0)
  if (rest > 0) top.push({
    label: t('dash_other', lang),
    count: rest,
    color: "#94a3b8",
    // Keep what's folded into "Others" so the UI can expand it on click
    items: restEntries.map(([label, count]) => ({ label: label || t('dash_empty', lang), count })),
  })
  return top
}

type OnFilter = (field: string, label: string) => void

function Donut({ data, title, field, onFilter, lang }: {
  data: Slice[]; title: string; field?: string; onFilter?: OnFilter; lang: Lang
}) {
  const [hovered, setHovered] = useState<string | null>(null)
  const [othersOpen, setOthersOpen] = useState(false)
  const total = data.reduce((s, d) => s + d.count, 0)
  if (!total) return null
  const sz = 120, cx = 60, cy = 60, r = 44, ir = 26
  let start = -Math.PI / 2
  const other = t('dash_other', lang)
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

  const clickable = !!field && !!onFilter
  const othersItems = data.find(d => d.label === other)?.items || []
  const handleClick = (label: string) => {
    if (!clickable) return
    if (label === other) {
      // Expand/collapse the hidden breakdown instead of filtering
      if (othersItems.length) setOthersOpen(o => !o)
      return
    }
    onFilter!(field!, label)
  }

  return (
    <div className="dash-chart">
      <div className="dash-chart-title">{title}{clickable && <span className="dash-filter-hint"> ↗ фільтр</span>}</div>
      <div className="dash-chart-body">
        <svg width={sz} height={sz} style={{ flexShrink: 0 }}>
          {slices.map((s, i) => {
            const isHov = hovered === s.label
            const isDim = hovered !== null && !isHov
            return (
              <path key={i} d={s.d} fill={s.color} stroke="var(--bg)" strokeWidth={isDim ? 1 : 1.5}
                style={{ cursor: clickable && (s.label !== other || othersItems.length) ? "pointer" : "default", transition: "opacity 0.15s, stroke-width 0.15s" }}
                opacity={isDim ? 0.13 : isHov ? 1 : 0.9}
                onClick={() => handleClick(s.label)}
                onMouseEnter={() => setHovered(s.label)}
                onMouseLeave={() => setHovered(null)}>
                <title>{s.label}: {s.count.toLocaleString()} ({(s.pct * 100).toFixed(1)}%){clickable && s.label !== other ? t('dash_click_filter', lang) : s.label === other && othersItems.length ? t('dash_other_expand', lang) : ""}</title>
              </path>
            )
          })}
          <text x={cx} y={cy - 4} textAnchor="middle" fontSize={12} fontWeight={600} fill="var(--text)">{total.toLocaleString()}</text>
          <text x={cx} y={cy + 11} textAnchor="middle" fontSize={8} fill="var(--text-3)">доменів</text>
        </svg>
        <table className="dash-legend-table">
          <tbody>
            {data.map((item, i) => {
              const isHov = hovered === item.label
              const isDim = hovered !== null && !isHov
              return (
                <tr key={i}
                  className={`dash-legend-row${clickable && (item.label !== other || (item.items?.length)) ? " dash-row-clickable" : ""}`}
                  style={{ opacity: isDim ? 0.25 : 1, transition: "opacity 0.15s" }}
                  onClick={() => handleClick(item.label)}
                  onMouseEnter={() => setHovered(item.label)}
                  onMouseLeave={() => setHovered(null)}>
                  <td style={{width:14,paddingRight:4,verticalAlign:"middle"}}>
                    <span className="dash-legend-dot" style={{
                      background: item.color,
                      transform: isHov ? "scale(1.4)" : "scale(1)",
                      transition: "transform 0.15s",
                      display: "inline-block",
                    }} />
                  </td>
                  <td className="dash-legend-label">{item.label}{item.label === other && item.items?.length ? (othersOpen ? " ▾" : " ▸") : ""}</td>
                  <td className="dash-legend-sep">–</td>
                  <td className="dash-legend-count">{item.count.toLocaleString()}</td>
                  <td className="dash-legend-pct">{((item.count / total) * 100).toFixed(1)}%</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      {/* Expanded "Others" breakdown — click any to apply as filter */}
      {othersOpen && othersItems.length > 0 && (
        <div className="dash-others-list">
          {othersItems.map((it, i) => (
            <div key={i} className="dash-others-row dash-row-clickable"
              onClick={() => onFilter!(field!, it.label)}
              title={t('dash_click_filter', lang)}>
              <span className="dash-others-label">{it.label}</span>
              <span className="dash-others-count">{it.count.toLocaleString()}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function CountryBars({ data, onFilter, lang }: { data: Slice[]; onFilter?: OnFilter; lang: Lang }) {
  const [hovered, setHovered] = useState<string | null>(null)
  const [othersOpen, setOthersOpen] = useState(false)
  if (!data.length) return null
  const max = data[0]?.count || 1
  const total = data.reduce((s, d) => s + d.count, 0)
  const other = t('dash_other', lang)
  const othersItems = data.find(d => d.label === other)?.items || []
  return (
    <div className="dash-chart">
      <div className="dash-chart-title">Country Map{onFilter && <span className="dash-filter-hint"> ↗ фільтр</span>}</div>
      <div className="dash-country-list">
        {data.slice(0, 12).map((item, i) => {
          const isOther = item.label === other
          const clickable = !!onFilter && item.label !== t('dash_no_region', lang) && (!isOther || othersItems.length > 0)
          const isHov = hovered === item.label
          const isDim = hovered !== null && !isHov
          return (
            <div key={i} className={`dash-country-row${clickable ? " dash-row-clickable" : ""}`}
              style={{ opacity: isDim ? 0.25 : 1, transition: "opacity 0.15s" }}
              onClick={() => {
                if (!clickable) return
                if (isOther) setOthersOpen(o => !o)
                else onFilter!("sw_primary_region", item.label)
              }}
              onMouseEnter={() => setHovered(item.label)}
              onMouseLeave={() => setHovered(null)}
              title={clickable ? (isOther ? t('dash_other_expand', lang) : t('dash_click_filter_country', lang)) : undefined}>
              <span className="dash-country-flag">{isOther ? "•••" : flag(item.label)}</span>
              <span className="dash-country-code">{isOther ? `${item.label}${othersItems.length ? (othersOpen ? " ▾" : " ▸") : ""}` : item.label}</span>
              <div className="dash-country-bar-wrap">
                <div className="dash-country-bar" style={{ width: `${(item.count / max) * 100}%`, background: item.color }} />
              </div>
              <span className="dash-country-count">{item.count.toLocaleString()}</span>
              <span className="dash-country-pct">{((item.count / total) * 100).toFixed(1)}%</span>
            </div>
          )
        })}
      </div>
      {othersOpen && othersItems.length > 0 && (
        <div className="dash-others-list">
          {othersItems.map((it, i) => (
            <div key={i} className="dash-others-row dash-row-clickable"
              onClick={() => onFilter!("sw_primary_region", it.label)}
              title={t('dash_click_filter_country', lang)}>
              <span className="dash-others-label">{flag(it.label)} {it.label}</span>
              <span className="dash-others-count">{it.count.toLocaleString()}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ISO-2 → ISO numeric (3-digit) mapping for world-atlas topology
const ISO2_NUM: Record<string, number> = {
  AD:20,AE:784,AF:4,AG:28,AL:8,AM:51,AO:24,AR:32,AT:40,AU:36,AZ:31,
  BA:70,BB:52,BD:50,BE:56,BF:854,BG:100,BH:48,BI:108,BJ:204,BN:96,
  BO:68,BR:76,BT:64,BW:72,BY:112,BZ:84,CA:124,CD:180,CF:140,CG:178,
  CH:756,CI:384,CL:152,CM:120,CN:156,CO:170,CR:188,CU:192,CV:132,CY:196,
  CZ:203,DE:276,DJ:262,DK:208,DO:214,DZ:12,EC:218,EE:233,EG:818,ER:232,
  ES:724,ET:231,FI:246,FJ:242,FR:250,GA:266,GB:826,GE:268,GH:288,GM:270,
  GN:324,GQ:226,GR:300,GT:320,GW:624,GY:328,HN:340,HR:191,HT:332,HU:348,
  ID:360,IE:372,IL:376,IN:356,IQ:368,IR:364,IS:352,IT:380,JM:388,JO:400,
  JP:392,KE:404,KG:417,KH:116,KM:174,KP:408,KR:410,KW:414,KZ:398,LA:418,
  LB:422,LI:438,LK:144,LR:430,LS:426,LT:440,LU:442,LV:428,LY:434,MA:504,
  MC:492,MD:498,ME:499,MG:450,MK:807,ML:466,MM:104,MN:496,MR:478,MT:470,
  MU:480,MV:462,MW:454,MX:484,MY:458,MZ:508,NA:516,NE:562,NG:566,NI:558,
  NL:528,NO:578,NP:524,NZ:554,OM:512,PA:591,PE:604,PG:598,PH:608,PK:586,
  PL:616,PS:275,PT:620,PY:600,QA:634,RO:642,RS:688,RU:643,RW:646,SA:682,
  SB:90,SC:690,SD:729,SE:752,SG:702,SI:705,SK:703,SL:694,SN:686,SO:706,
  SR:740,SS:728,ST:678,SV:222,SY:760,SZ:748,TD:148,TG:768,TH:764,TJ:762,
  TL:626,TM:795,TN:788,TO:776,TR:792,TT:780,TZ:834,UA:804,UG:800,US:840,
  UY:858,UZ:860,VC:670,VE:862,VN:704,VU:548,WS:882,YE:887,ZA:710,ZM:894,ZW:716,
}

// Patched topology: Crimea correctly assigned to Ukraine (UA).
// Served via FastAPI route /world-110m-ua.json → frontend/dist/world-110m-ua.json
// In dev (Vite :5173) served from public/ automatically.
const WORLD_TOPO = "/world-110m-ua.json"

// Short country names for tooltip
const ISO2_NAME: Record<string, string> = {
  AD:"Andorra",AE:"UAE",AF:"Afghanistan",AG:"Antigua",AL:"Albania",AM:"Armenia",AO:"Angola",
  AR:"Argentina",AT:"Austria",AU:"Australia",AZ:"Azerbaijan",BA:"Bosnia",BB:"Barbados",
  BD:"Bangladesh",BE:"Belgium",BF:"Burkina Faso",BG:"Bulgaria",BH:"Bahrain",BI:"Burundi",
  BJ:"Benin",BN:"Brunei",BO:"Bolivia",BR:"Brazil",BT:"Bhutan",BW:"Botswana",BY:"Belarus",
  BZ:"Belize",CA:"Canada",CD:"DR Congo",CF:"C. African Rep.",CG:"Congo",CH:"Switzerland",
  CI:"Ivory Coast",CL:"Chile",CM:"Cameroon",CN:"China",CO:"Colombia",CR:"Costa Rica",
  CU:"Cuba",CV:"Cape Verde",CY:"Cyprus",CZ:"Czechia",DE:"Germany",DJ:"Djibouti",
  DK:"Denmark",DO:"Dominican Rep.",DZ:"Algeria",EC:"Ecuador",EE:"Estonia",EG:"Egypt",
  ER:"Eritrea",ES:"Spain",ET:"Ethiopia",FI:"Finland",FJ:"Fiji",FR:"France",GA:"Gabon",
  GB:"United Kingdom",GE:"Georgia",GH:"Ghana",GM:"Gambia",GN:"Guinea",GQ:"Eq. Guinea",
  GR:"Greece",GT:"Guatemala",GW:"Guinea-Bissau",GY:"Guyana",HN:"Honduras",HR:"Croatia",
  HT:"Haiti",HU:"Hungary",ID:"Indonesia",IE:"Ireland",IL:"Israel",IN:"India",IQ:"Iraq",
  IR:"Iran",IS:"Iceland",IT:"Italy",JM:"Jamaica",JO:"Jordan",JP:"Japan",KE:"Kenya",
  KG:"Kyrgyzstan",KH:"Cambodia",KP:"North Korea",KR:"South Korea",KW:"Kuwait",
  KZ:"Kazakhstan",LA:"Laos",LB:"Lebanon",LI:"Liechtenstein",LK:"Sri Lanka",LR:"Liberia",
  LS:"Lesotho",LT:"Lithuania",LU:"Luxembourg",LV:"Latvia",LY:"Libya",MA:"Morocco",
  MC:"Monaco",MD:"Moldova",ME:"Montenegro",MG:"Madagascar",MK:"N. Macedonia",ML:"Mali",
  MM:"Myanmar",MN:"Mongolia",MR:"Mauritania",MT:"Malta",MU:"Mauritius",MV:"Maldives",
  MW:"Malawi",MX:"Mexico",MY:"Malaysia",MZ:"Mozambique",NA:"Namibia",NE:"Niger",
  NG:"Nigeria",NI:"Nicaragua",NL:"Netherlands",NO:"Norway",NP:"Nepal",NZ:"New Zealand",
  OM:"Oman",PA:"Panama",PE:"Peru",PG:"Papua N.G.",PH:"Philippines",PK:"Pakistan",
  PL:"Poland",PS:"Palestine",PT:"Portugal",PY:"Paraguay",QA:"Qatar",RO:"Romania",
  RS:"Serbia",RU:"MORDER",RW:"Rwanda",SA:"Saudi Arabia",SB:"Solomon Is.",SC:"Seychelles",
  SD:"Sudan",SE:"Sweden",SG:"Singapore",SI:"Slovenia",SK:"Slovakia",SL:"Sierra Leone",
  SN:"Senegal",SO:"Somalia",SR:"Suriname",SS:"South Sudan",SV:"El Salvador",SY:"Syria",
  SZ:"Eswatini",TD:"Chad",TG:"Togo",TH:"Thailand",TJ:"Tajikistan",TL:"Timor-Leste",
  TM:"Turkmenistan",TN:"Tunisia",TR:"Turkey",TT:"Trinidad",TZ:"Tanzania",UA:"Ukraine",
  UG:"Uganda",US:"United States",UY:"Uruguay",UZ:"Uzbekistan",VC:"St Vincent",
  VE:"Venezuela",VN:"Vietnam",VU:"Vanuatu",YE:"Yemen",ZA:"South Africa",ZM:"Zambia",ZW:"Zimbabwe",
}

// Country centroids [lon, lat] for auto-zoom calculation
const ISO2_CENTER: Record<string, [number, number]> = {
  AD:[1.6,42.5],AE:[54,23.4],AF:[67.7,33.9],AL:[20,41],AM:[45,40],AO:[18.5,-11.2],
  AR:[-64,-34],AT:[14.5,47.5],AU:[133,-27],AZ:[47.5,40.4],BA:[17.8,44],BD:[90,24],
  BE:[4.5,50.5],BF:[-2,12],BG:[25,43],BH:[50.6,26],BY:[28,53],BO:[-65,-17],
  BR:[-51,-14],CA:[-96,60],CD:[24,-3],CF:[21,7],CG:[15,-1],CH:[8,47],CI:[-6,7.5],
  CL:[-71,-30],CM:[12.4,5.7],CN:[105,35],CO:[-74,4],CR:[-84,10],CU:[-80,22],
  CY:[33,35],CZ:[15.5,49.8],DE:[10,51],DK:[10,56],DO:[-70,18.7],DZ:[3,28],
  EC:[-78,-2],EE:[25,59],EG:[30,27],ES:[-4,40],ET:[40,8],FI:[26,64],FR:[2.2,46.2],
  GB:[-2,54],GE:[43.5,42],GH:[-2,8],GR:[22,39],GT:[-90.2,15.5],HR:[16,45],
  HU:[19,47],ID:[118,-5],IE:[-8,53],IL:[34.8,31.4],IN:[78.9,20.6],IQ:[44,33],
  IR:[53,32],IS:[-19,65],IT:[12.5,42.8],JO:[37,31],JP:[138,36],KE:[38,-1],
  KG:[75,41],KH:[105,12.5],KP:[127,40],KR:[128,36],KW:[47.7,29.3],KZ:[67,48],
  LB:[35.5,34],LT:[24,56],LU:[6.1,49.8],LV:[25,57],LY:[17,27],MA:[-7,32],
  MD:[29,47],ME:[19,42.7],MK:[21.7,41.6],ML:[-2,17],MM:[96,17],MN:[103,46],
  MT:[14.4,35.9],MX:[-102,24],MY:[112,2.5],MZ:[35,-18],NA:[18,-22],NG:[8,10],
  NI:[-85,12.8],NL:[5.3,52.3],NO:[10,62],NP:[84,28],NZ:[172,-42],OM:[57,21],
  PA:[-80,9],PE:[-76,-10],PH:[122,13],PK:[70,30],PL:[20,52],PS:[35.3,31.9],
  PT:[-8,39.5],PY:[-58,-23],QA:[51.2,25.3],RO:[25,46],RS:[21,44],RU:[100,60],
  RW:[30,-2],SA:[45,25],SD:[30,15],SE:[18,62],SG:[103.8,1.4],SI:[15,46],
  SK:[19.5,48.7],SN:[-14,14],SO:[46,6],SS:[31,7],SV:[-88.9,13.8],SY:[38,35],
  TD:[18,15],TH:[101,15],TJ:[71,39],TN:[9,34],TR:[35,39],TZ:[35,-6],
  UA:[32,49],UG:[32,1],US:[-98,38],UY:[-56,-33],UZ:[64,41],VE:[-66,8],
  VN:[108,14],YE:[48,15],ZA:[25,-29],ZM:[28,-13],ZW:[30,-20],
}

// Compute auto-zoom: fit all active countries into the map view
function computeAutoZoom(
  activeCodes: string[],
  mapW: number,
  mapH: number,
): { coordinates: [number, number]; zoom: number } {
  const pts = activeCodes.map(c => ISO2_CENTER[c]).filter(Boolean) as [number, number][]
  if (!pts.length) return { coordinates: [10, 15], zoom: 1 }
  if (pts.length === 1) return { coordinates: pts[0], zoom: 5 }

  let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity
  for (const [lon, lat] of pts) {
    minLon = Math.min(minLon, lon); maxLon = Math.max(maxLon, lon)
    minLat = Math.min(minLat, lat); maxLat = Math.max(maxLat, lat)
  }
  const cx = (minLon + maxLon) / 2
  const cy = (minLat + maxLat) / 2
  const spanLon = Math.max(maxLon - minLon, 10)
  const spanLat = Math.max(maxLat - minLat, 10)

  // Mercator: longitude is linear, latitude is compressed near poles.
  // Base scale 105 → full world ~320px wide at zoom=1.
  // 1 degree lon ≈ (mapW / 360) * zoom pixels at zoom=1 scale.
  const zoomByLon = (mapW * 0.7) / (spanLon * (mapW / 360))
  const zoomByLat = (mapH * 0.7) / (spanLat * (mapH / 170))
  const zoom = Math.min(zoomByLon, zoomByLat, 8)

  return { coordinates: [cx, cy], zoom: Math.max(zoom, 1) }
}

interface TooltipState { x: number; y: number; iso2: string; count: number }

function WorldMap({ countryData, onFilter }: {
  countryData: Slice[]
  onFilter?: OnFilter
  lang: Lang
}) {
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  const [pos, setPos] = useState({ coordinates: [10, 15] as [number, number], zoom: 1 })
  const [userMoved, setUserMoved] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<HTMLDivElement>(null)

  // Build numeric-id → count + iso2 map
  const numMap = useMemo(() => {
    const iso2count: Record<string, number> = {}
    for (const s of countryData) {
      if (s.label && s.label.length === 2) iso2count[s.label.toUpperCase()] = s.count
    }
    // Topology ids are zero-padded 3-char strings ("076" = Brazil, "040" = Austria),
    // so pad numeric codes — otherwise countries with code < 100 never match.
    const m: Record<string, { count: number; iso2: string }> = {}
    for (const [iso2, num] of Object.entries(ISO2_NUM)) {
      if (iso2count[iso2]) m[String(num).padStart(3, "0")] = { count: iso2count[iso2], iso2 }
    }
    return m
  }, [countryData])

  const maxCount = useMemo(() => Math.max(...countryData.map(d => d.count), 1), [countryData])

  // Auto-zoom to fit all countries with data whenever data changes (and user hasn't manually moved)
  useEffect(() => {
    if (userMoved) return
    const activeCodes = countryData
      .filter(s => s.label?.length === 2)
      .map(s => s.label.toUpperCase())
    const el = containerRef.current
    const w = el?.clientWidth || 500
    const h = 280
    const { coordinates, zoom } = computeAutoZoom(activeCodes, w, h)
    setPos({ coordinates, zoom })
  }, [countryData, userMoved])

  function countryColor(count: number) {
    if (!count) return "#d1d5db"
    const t = Math.pow(count / maxCount, 0.45)
    const r = Math.round(191 - t * (191 - 30))
    const g = Math.round(219 - t * (219 - 58))
    const b = Math.round(254 - t * (254 - 138))
    return `rgb(${r},${g},${b})`
  }

  // Prevent page scroll when mouse is over the map
  useEffect(() => {
    const el = mapRef.current
    if (!el) return
    const prevent = (e: WheelEvent) => e.preventDefault()
    el.addEventListener("wheel", prevent, { passive: false })
    return () => el.removeEventListener("wheel", prevent)
  }, [])

  const updateTooltip = (e: React.MouseEvent, iso2: string, count: number) => {
    const rect = containerRef.current?.getBoundingClientRect()
    if (!rect) return
    setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, iso2, count })
  }

  if (!countryData.length) return null

  const isDark = document.documentElement.classList.contains("dark") ||
    window.matchMedia("(prefers-color-scheme: dark)").matches

  return (
    <div className="dash-chart dash-chart-map" ref={containerRef}>
      <div className="dash-chart-title">
        World Map{onFilter && <span className="dash-filter-hint"> ↗ клік = фільтр · scroll = зум · drag = pan</span>}
      </div>
      <div style={{ position: "relative", borderRadius: 8, overflow: "hidden" }} ref={mapRef}>
        <ComposableMap
          projection="geoMercator"
          projectionConfig={{ scale: 105, center: [10, 25] }}
          style={{ width: "100%", height: "280px", background: isDark ? "#0f172a" : "#bfdbfe" }}
        >
          <ZoomableGroup
            zoom={pos.zoom}
            center={pos.coordinates}
            minZoom={0.8}
            maxZoom={12}
            // allow scroll wheel AND pinch (ctrlKey) — default filter blocks ctrlKey
            filterZoomEvent={(e: any) => e.type !== "dblclick"}
            onMoveEnd={({ zoom, coordinates }) => { setPos({ zoom, coordinates }); setUserMoved(true) }}
          >
            <Geographies geography={WORLD_TOPO}>
              {({ geographies }) =>
                geographies.map(geo => {
                  const info = numMap[String(geo.id).padStart(3, "0")]
                  const count = info?.count || 0
                  const iso2 = info?.iso2 || ""
                  const clickable = !!onFilter && !!iso2
                  return (
                    <Geography
                      key={geo.rsmKey}
                      geography={geo}
                      fill={countryColor(count)}
                      stroke={isDark ? "#1e293b" : "#fff"}
                      strokeWidth={0.5 / pos.zoom}
                      style={{
                        default: { outline: "none", cursor: clickable ? "pointer" : "default", transition: "fill 0.1s" },
                        hover:   { outline: "none", fill: count ? "#4f46e5" : "#9ca3af", cursor: clickable ? "pointer" : "default" },
                        pressed: { outline: "none", fill: "#3730a3" },
                      }}
                      onMouseEnter={(e: any) => { if (iso2 || count) updateTooltip(e, iso2, count) }}
                      onMouseMove={(e: any) => { if (tooltip) updateTooltip(e, iso2, count) }}
                      onMouseLeave={() => setTooltip(null)}
                      onClick={() => { if (clickable) onFilter!("sw_primary_region", iso2) }}
                    />
                  )
                })
              }
            </Geographies>
            {/* Country name labels — shown for countries with data */}
            {countryData.map(s => {
              const center = ISO2_CENTER[s.label]
              const name = ISO2_NAME[s.label] || s.label
              if (!center) return null
              const fontSize = Math.max(2.5, Math.min(8, 8 / pos.zoom))
              return (
                <Annotation
                  key={s.label}
                  subject={center}
                  dx={0} dy={0}
                  connectorProps={{ stroke: "none" }}
                >
                  <text
                    textAnchor="middle"
                    dominantBaseline="middle"
                    fontSize={fontSize}
                    fill={isDark ? "rgba(255,255,255,0.75)" : "rgba(0,0,0,0.65)"}
                    style={{ pointerEvents: "none", userSelect: "none", fontWeight: 500 }}
                  >
                    {name}
                  </text>
                </Annotation>
              )
            })}
          </ZoomableGroup>
        </ComposableMap>

        {/* Tooltip */}
        {tooltip && tooltip.iso2 && (
          <div style={{
            position: "absolute",
            left: Math.min(tooltip.x + 12, (containerRef.current?.clientWidth || 400) - 160),
            top: tooltip.y > 60 ? tooltip.y - 48 : tooltip.y + 12,
            background: "rgba(15,23,42,0.92)", color: "#f1f5f9",
            padding: "5px 10px", borderRadius: 7, fontSize: 12, pointerEvents: "none",
            whiteSpace: "nowrap", zIndex: 20,
            boxShadow: "0 4px 16px rgba(0,0,0,.4)",
            border: "1px solid rgba(99,102,241,0.3)",
          }}>
            <span style={{ marginRight: 5 }}>{flag(tooltip.iso2)}</span>
            <strong>{ISO2_NAME[tooltip.iso2] || tooltip.iso2}</strong>
            {tooltip.count > 0 && (
              <span style={{ marginLeft: 8, color: "#94a3b8" }}>
                {tooltip.count.toLocaleString()} домен{tooltip.count === 1 ? "" : "ів"}
              </span>
            )}
          </div>
        )}

        {/* Color legend */}
        <div style={{
          position: "absolute", bottom: 8, left: 10,
          display: "flex", alignItems: "center", gap: 4,
          background: "rgba(0,0,0,0.45)", borderRadius: 5, padding: "3px 8px",
        }}>
          <span style={{ fontSize: 10, color: "#94a3b8" }}>0</span>
          <div style={{
            width: 60, height: 7, borderRadius: 4,
            background: "linear-gradient(to right, #bfdbfe, #1e3a8a)",
          }} />
          <span style={{ fontSize: 10, color: "#94a3b8" }}>{maxCount.toLocaleString()}</span>
        </div>

        {/* Zoom controls */}
        <div style={{ position: "absolute", bottom: 6, right: 8, display: "flex", flexDirection: "column", gap: 3 }}>
          <button className="map-zoom-btn" onClick={() => { setPos(p => ({ ...p, zoom: Math.min(p.zoom * 1.7, 12) })); setUserMoved(true) }} title="Zoom in">+</button>
          <button className="map-zoom-btn" onClick={() => { setPos(p => ({ ...p, zoom: Math.max(p.zoom / 1.7, 0.8) })); setUserMoved(true) }} title="Zoom out">−</button>
          <button className="map-zoom-btn" onClick={() => setUserMoved(false)} title="Reset to data">⌂</button>
        </div>
      </div>
    </div>
  )
}

export default function Dashboard({ profiles, onFilter, lang }: { profiles: Profile[]; onFilter?: OnFilter; lang: Lang }) {
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
      const empty = t('dash_empty', lang)
      const cms = p.cms_list?.trim() || empty; cmsMap.set(cms, (cmsMap.get(cms) || 0) + 1)
      const ems = p.ems_list?.trim() || empty; emsMap.set(ems, (emsMap.get(ems) || 0) + 1)
      const srch = p.osearch?.trim() || empty; searchMap.set(srch, (searchMap.get(srch) || 0) + 1)
      const cat = p.sw_category?.trim() || empty; catMap.set(cat, (catMap.get(cat) || 0) + 1)
      const sub = p.sw_subcategory?.trim() || empty; subMap.set(sub, (subMap.get(sub) || 0) + 1)
    }

    if (noRegion > 0) countryMap.set(t('dash_no_region', lang), noRegion)

    const trafficData: Slice[] = TRAFFIC_GROUPS
      .map((g, i) => ({ label: g.label, count: trafficMap.get(g.label) || 0, color: g.color }))
      .filter(d => d.count > 0)

    return {
      trafficData,
      countryData:    topN(countryMap, 10, lang),   // top-10 for CountryBars legend
      countryDataAll: topN(countryMap, 250, lang),  // all for WorldMap coloring
      cmsData:        topN(cmsMap, 8, lang),
      emsData:     topN(emsMap, 8, lang),
      searchData:  topN(searchMap, 8, lang),
      catData:     topN(catMap, 8, lang),
      subData:     topN(subMap, 8, lang),
    }
  }, [profiles, lang])

  if (!profiles.length) return null

  return (
    <div>
      <div className="dashboard dashboard-4">
        <Donut data={charts.trafficData} title="Traffic Groups" field="sw_visits"   onFilter={onFilter} lang={lang} />
        <Donut data={charts.cmsData}     title="CMS"            field="cms_list"    onFilter={onFilter} lang={lang} />
        <Donut data={charts.emsData}     title="EMS"            field="ems_list"    onFilter={onFilter} lang={lang} />
        <Donut data={charts.searchData}  title="Onsite Search"  field="osearch"     onFilter={onFilter} lang={lang} />
      </div>
      <div className="dashboard dashboard-map">
        <WorldMap countryData={charts.countryDataAll} onFilter={onFilter} lang={lang} />
        <CountryBars data={charts.countryData} onFilter={onFilter} lang={lang} />
        <Donut data={charts.catData} title="Category SW" field="sw_category" onFilter={onFilter} lang={lang} />
        <Donut data={charts.subData} title="Subcategory SW" lang={lang} />
      </div>
    </div>
  )
}
