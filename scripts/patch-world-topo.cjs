#!/usr/bin/env node
/**
 * Patches world-atlas 110m topology so that Crimea is shown as part of Ukraine.
 * Natural Earth (source of world-atlas) shows Crimea under Russian de-facto control.
 * This script reassigns any ring in Russia's geometry that falls within Crimea's
 * geographic bounding box to Ukraine's geometry.
 *
 * Run once:  node scripts/patch-world-topo.cjs
 * Output:    frontend/public/world-110m-ua.json
 */

const https = require('https')
const fs    = require('fs')
const path  = require('path')

// Try 50m first (more detail → Crimea is more likely a separate ring)
const URLS = [
  'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-50m.json',
  'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json',
]
const OUT = path.join(__dirname, '../frontend/public/world-110m-ua.json')

// Crimea bounding box in geographic degrees
const CRIMEA = { lonMin: 32.4, lonMax: 36.8, latMin: 44.2, latMax: 46.4 }

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      let data = ''
      res.on('data', d => data += d)
      res.on('end', () => { try { resolve(JSON.parse(data)) } catch(e) { reject(e) } })
      res.on('error', reject)
    }).on('error', reject)
  })
}

// Decode a delta-encoded TopoJSON arc → [[lon, lat], ...]
function decodeArc(arcs, scale, translate, idx) {
  const raw = arcs[idx < 0 ? ~idx : idx]
  let x = 0, y = 0
  return raw.map(([dx, dy]) => {
    x += dx; y += dy
    return [x * scale[0] + translate[0], y * scale[1] + translate[1]]
  })
}

// Bounding box of decoded arc
function arcBbox(pts) {
  let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity
  for (const [lon, lat] of pts) {
    if (lon < minLon) minLon = lon; if (lon > maxLon) maxLon = lon
    if (lat < minLat) minLat = lat; if (lat > maxLat) maxLat = lat
  }
  return { minLon, maxLon, minLat, maxLat }
}

// Does the arc bbox overlap with Crimea?
function overlapsWithCrimea(bbox) {
  return (
    bbox.maxLon > CRIMEA.lonMin && bbox.minLon < CRIMEA.lonMax &&
    bbox.maxLat > CRIMEA.latMin && bbox.minLat < CRIMEA.latMax
  )
}

// Is the arc bbox mostly inside Crimea (not just touching)?
function isInCrimea(bbox) {
  const centerLon = (bbox.minLon + bbox.maxLon) / 2
  const centerLat = (bbox.minLat + bbox.maxLat) / 2
  return (
    centerLon > CRIMEA.lonMin && centerLon < CRIMEA.lonMax &&
    centerLat > CRIMEA.latMin && centerLat < CRIMEA.latMax
  )
}

function getAllRings(geom) {
  if (geom.type === 'Polygon')      return geom.arcs                  // [[ring], [hole], ...]
  if (geom.type === 'MultiPolygon') return geom.arcs.map(p => p).flat() // each polygon → [outer, ...holes]
  return []
}

async function patchTopology(topo) {
  const { scale, translate } = topo.transform
  const rawArcs = topo.arcs
  const geometries = topo.objects.countries.geometries

  // IDs may be strings or numbers depending on topology version
  const russia  = geometries.find(g => String(g.id) === '643')
  const ukraine = geometries.find(g => String(g.id) === '804')
  if (!russia || !ukraine) throw new Error('Russia (643) or Ukraine (804) not found in topology')

  // Analyse each ring in Russia's geometry
  const allRussiaPolygons = russia.type === 'MultiPolygon' ? russia.arcs : [russia.arcs]
  const keepPolygons   = []  // polygons that stay in Russia
  const crimeaPolygons = []  // polygons to move to Ukraine

  for (const polygon of allRussiaPolygons) {
    // polygon = [[outerRing], [hole1], ...]  where each ring = [arcIdx, ...]
    const outerRing = polygon[0]
    // Decode and check all arcs in the outer ring
    const arcBboxes = outerRing.map(idx => {
      const pts = decodeArc(rawArcs, scale, translate, idx)
      return arcBbox(pts)
    })
    const ringInCrimea = arcBboxes.some(bb => isInCrimea(bb))
    const ringOverlaps  = arcBboxes.some(bb => overlapsWithCrimea(bb))

    // Heuristic: if the ring's arcs center in Crimea and the polygon is small
    // (not the main European/Asian Russia mainland)
    const allPts = outerRing.flatMap(idx => decodeArc(rawArcs, scale, translate, idx))
    const rb = arcBbox(allPts)
    const spanLon = rb.maxLon - rb.minLon
    const spanLat = rb.maxLat - rb.minLat
    const isTiny  = spanLon < 6 && spanLat < 4   // Crimea is ~4° lon × 2° lat

    if (ringOverlaps && isTiny) {
      crimeaPolygons.push(polygon)
      console.log(`  → Moving polygon bbox [${rb.minLon.toFixed(1)},${rb.minLat.toFixed(1)}]–[${rb.maxLon.toFixed(1)},${rb.maxLat.toFixed(1)}] from Russia to Ukraine`)
    } else {
      keepPolygons.push(polygon)
    }
  }

  if (crimeaPolygons.length === 0) {
    console.warn('  ⚠  No Crimea polygons found at this resolution — topology saved unchanged')
    return topo
  }

  console.log(`  ✓ Moving ${crimeaPolygons.length} polygon(s) to Ukraine`)

  // Update Russia
  russia.type = 'MultiPolygon'
  russia.arcs  = keepPolygons

  // Update Ukraine — merge in crimea polygons
  if (ukraine.type === 'Polygon') {
    ukraine.type = 'MultiPolygon'
    ukraine.arcs = [ukraine.arcs, ...crimeaPolygons]
  } else {
    ukraine.arcs = [...ukraine.arcs, ...crimeaPolygons]
  }

  return topo
}

async function main() {
  let topo = null
  let usedUrl = ''

  for (const url of URLS) {
    console.log(`Fetching ${url} …`)
    try {
      topo = await fetchJSON(url)
      usedUrl = url
      console.log(`  ✓ Fetched (${(JSON.stringify(topo).length / 1024).toFixed(0)} KB)`)
      break
    } catch (e) {
      console.warn(`  ✗ Failed: ${e.message}`)
    }
  }

  if (!topo) { console.error('Could not fetch topology'); process.exit(1) }

  console.log('Patching Crimea …')
  const patched = await patchTopology(topo)

  fs.writeFileSync(OUT, JSON.stringify(patched))
  console.log(`\nSaved → ${OUT}`)
  console.log(`Source: ${usedUrl}`)
}

main().catch(e => { console.error(e); process.exit(1) })
