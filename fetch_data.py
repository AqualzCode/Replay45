#!/usr/bin/env python3
"""
Field Shift - data fetcher (NASA Space Apps 2026)

Downloads the open data the app needs and writes ONE file: fieldshift_data.json
Sources (all public, no login):
  - NASA POWER (MERRA-2 / CERES) monthly regional grid + daily point series
  - NASA MODIS MOD13Q1 NDVI via the ORNL DAAC subset API
  - USDA NASS Cropland Data Layer via CropScape point queries
  - USDA NRCS SSURGO via Soil Data Access

Run:   python3 fetch_data.py
Needs: Python 3.8+, no extra packages. Takes roughly 5-15 minutes.
Safe to re-run: finished requests are cached in ./fs_cache
(Script written with AI assistance - Claude - and disclosed as such.)
"""
import json, math, os, re, sys, time, hashlib, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

OUT = "fieldshift_data.json"
CACHE = "fs_cache"
ERA_A = (1981, 2000)
ERA_B = (2006, 2025)
LAT0, LAT1, LON0, LON1 = 24, 50, -125, -66
FARMS = [
    {"id": "cozad", "name": "Cozad, Nebraska", "lat": 40.885, "lon": -100.000},
    {"id": "loudoun", "name": "Loudoun County, Virginia", "lat": 39.200, "lon": -77.660,
     "scan": [(39.22, -77.53), (39.25, -77.55), (39.27, -77.60), (39.27, -77.66), (39.24, -77.62), (39.21, -77.58), (39.18, -77.56),
              (39.19, -77.70), (39.23, -77.72), (39.15, -77.74), (39.11, -77.70), (39.06, -77.74), (39.03, -77.66), (38.99, -77.76),
              (39.16, -77.63), (39.29, -77.70)]},
]
CDL_YEARS = list(range(2008, 2026))
LOG = {"errors": [], "notes": []}
os.makedirs(CACHE, exist_ok=True)


def say(*a):
    print(*a, flush=True)


def http(url, data=None, headers=None, timeout=90, tries=4, cache=True):
    key = hashlib.sha1((url + "|" + (data or "")).encode()).hexdigest()
    path = os.path.join(CACHE, key)
    if cache and os.path.exists(path):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, data=data.encode() if data else None,
                                         headers=dict({"User-Agent": "FieldShift-SpaceApps/1.0",
                                                       "Accept": "application/json"}, **(headers or {})))
            with urllib.request.urlopen(req, timeout=timeout) as r:
                txt = r.read().decode("utf-8", errors="replace")
            if cache:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(txt)
            return txt
        except urllib.error.HTTPError as e:
            last = "HTTP %s %s" % (e.code, e.read()[:200])
            if e.code in (400, 404, 422):
                break
        except Exception as e:  # noqa
            last = repr(e)
        time.sleep(2 + 4 * i)
    raise RuntimeError("%s -> %s" % (url[:140], last))


# ---------------------------------------------------------------- NASA POWER grid
POWER = "https://power.larc.nasa.gov/api/temporal"
GRID_PARAMS = ["T2M_MAX", "T2M_MIN", "PRECTOTCORR"]


def power_regional(param, la0, la1, lo0, lo1, y0, y1):
    url = ("%s/monthly/regional?parameters=%s&community=AG&latitude-min=%s&latitude-max=%s"
           "&longitude-min=%s&longitude-max=%s&start=%d&end=%d&format=JSON"
           % (POWER, param, la0, la1, lo0, lo1, y0, y1))
    j = json.loads(http(url, timeout=180))
    out = {}
    for f in j["features"]:
        lon, lat = f["geometry"]["coordinates"][0], f["geometry"]["coordinates"][1]
        elev = f["geometry"]["coordinates"][2] if len(f["geometry"]["coordinates"]) > 2 else None
        series = f["properties"]["parameter"][param]
        sums, cnt = [0.0] * 12, [0] * 12
        for k, v in series.items():
            m = int(k[4:6])
            if 1 <= m <= 12 and v is not None and v > -900:
                sums[m - 1] += v
                cnt[m - 1] += 1
        out[(round(lat, 3), round(lon, 3))] = ([sums[i] / cnt[i] if cnt[i] else None for i in range(12)], elev)
    return out


def fetch_grid():
    say("[1/5] NASA POWER climate grid (two 20-year eras)...")
    tiles = []
    la = LAT0
    while la < LAT1:
        la2 = min(la + 10, LAT1)
        if la2 - la < 2:
            la = la2 - 2
        lo = LON0
        while lo < LON1:
            lo2 = min(lo + 10, LON1)
            if lo2 - lo < 2:
                lo = lo2 - 2
            tiles.append((la, la2, lo, lo2))
            lo = lo2
        la = la2
    jobs = [(p, t, e) for p in GRID_PARAMS for t in tiles for e in (ERA_A, ERA_B)]
    cells = {}
    done = 0
    with ThreadPoolExecutor(4) as ex:
        futs = {ex.submit(power_regional, p, t[0], t[1], t[2], t[3], e[0], e[1]): (p, t, e) for p, t, e in jobs}
        for fu in as_completed(futs):
            p, t, e = futs[fu]
            done += 1
            try:
                res = fu.result()
            except Exception as ex_:
                LOG["errors"].append("grid %s %s %s: %s" % (p, t, e, ex_))
                say("   ! failed", p, t, e, str(ex_)[:120])
                continue
            tag = "A" if e == ERA_A else "B"
            for k, (clim, elev) in res.items():
                c = cells.setdefault(k, {"elev": elev})
                c[p + tag] = clim
            if done % 12 == 0:
                say("   %d / %d requests" % (done, len(jobs)))
    keys = sorted(cells)
    need = [p + t for p in GRID_PARAMS for t in "AB"]
    keys = [k for k in keys if all(n in cells[k] and None not in cells[k][n] for n in need)]
    days = [31, 28.25, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    g = {"lat": [], "lon": [], "elev": [], "tA": [], "tB": [], "pA": [], "pB": [], "dA": [], "dB": []}
    for k in keys:
        c = cells[k]
        g["lat"].append(k[0]); g["lon"].append(k[1]); g["elev"].append(round(c["elev"] or 0))
        for t in "AB":
            g["t" + t].append([round((c["T2M_MAX" + t][m] + c["T2M_MIN" + t][m]) / 2 * 10) for m in range(12)])
            g["d" + t].append([round((c["T2M_MAX" + t][m] - c["T2M_MIN" + t][m]) * 10) for m in range(12)])
            g["p" + t].append([round(c["PRECTOTCORR" + t][m] * days[m]) for m in range(12)])
    say("   grid cells:", len(keys))
    g["units"] = "t*: monthly mean temp x10 C; d*: monthly mean diurnal range x10 C; p*: mm/month; A=%d-%d B=%d-%d" % (ERA_A + ERA_B)
    return g


# ---------------------------------------------------------------- analog matching
def analogs(g, lat, lon, n=6):
    N = len(g["lat"])
    i0 = min(range(N), key=lambda i: (g["lat"][i] - lat) ** 2 + (g["lon"][i] - lon) ** 2)
    sdT, sdP = [], []
    for m in range(12):
        tv = [g["tA"][i][m] / 10 for i in range(N)]
        pv = [math.sqrt(g["pA"][i][m]) for i in range(N)]
        mt, mp = sum(tv) / N, sum(pv) / N
        sdT.append(math.sqrt(sum((x - mt) ** 2 for x in tv) / N) or 1)
        sdP.append(math.sqrt(sum((x - mp) ** 2 for x in pv) / N) or 1)

    def dist(i, era_i, j, era_j):
        s = 0
        for m in range(12):
            s += ((g["t" + era_i][i][m] - g["t" + era_j][j][m]) / 10 / sdT[m]) ** 2
            s += ((math.sqrt(g["p" + era_i][i][m]) - math.sqrt(g["p" + era_j][j][m])) / sdP[m]) ** 2
        return math.sqrt(s / 24)
    cand = sorted(((dist(i0, "B", j, "A"), j) for j in range(N)
                   if abs(g["elev"][j] - g["elev"][i0]) < 500), key=lambda x: x[0])
    return i0, [{"lat": g["lat"][j], "lon": g["lon"][j], "elev": g["elev"][j], "d": round(d, 4)} for d, j in cand[:n]], \
        round(dist(i0, "B", i0, "A"), 4)


# ---------------------------------------------------------------- NASA POWER daily
DAILY = ["T2M_MAX", "T2M_MIN", "T2MDEW", "WS2M", "ALLSKY_SFC_SW_DWN", "PRECTOTCORR"]


def fetch_daily(lat, lon):
    out = {p: [] for p in DAILY}
    dates = []
    meta = {}
    for y0, y1 in ((1981, 1995), (1996, 2010), (2011, 2025)):
        url = ("%s/daily/point?parameters=%s&community=AG&longitude=%s&latitude=%s&start=%d0101&end=%d1231&format=JSON"
               % (POWER, ",".join(DAILY), lon, lat, y0, y1))
        j = json.loads(http(url, timeout=240))
        par = j["properties"]["parameter"]
        ds = sorted(par[DAILY[0]].keys())
        dates += ds
        for p in DAILY:
            for d in ds:
                v = par[p].get(d)
                out[p].append(None if v is None or v < -900 else round(v * 10))
        meta = {"coords": j["geometry"]["coordinates"], "units": {p: j.get("parameters", {}).get(p, {}).get("units") for p in DAILY}}
    return {"start": dates[0], "end": dates[-1], "n": len(dates), "scale": 10, "meta": meta, "v": out}


# ---------------------------------------------------------------- USDA CDL (CropScape)
def albers(lat, lon):
    a, f = 6378137.0, 1 / 298.257222101
    e2 = 2 * f - f * f
    e = math.sqrt(e2)
    r = math.radians

    def q(p):
        s = math.sin(p)
        return (1 - e2) * (s / (1 - e2 * s * s) - (1 / (2 * e)) * math.log((1 - e * s) / (1 + e * s)))

    def m(p):
        return math.cos(p) / math.sqrt(1 - e2 * math.sin(p) ** 2)
    p1, p2, p0, l0 = r(29.5), r(45.5), r(23.0), r(-96.0)
    n = (m(p1) ** 2 - m(p2) ** 2) / (q(p2) - q(p1))
    C = m(p1) ** 2 + n * q(p1)
    rho0 = a * math.sqrt(C - n * q(p0)) / n
    rho = a * math.sqrt(C - n * q(r(lat))) / n
    th = n * (r(lon) - l0)
    return rho * math.sin(th), rho0 - rho * math.cos(th)


def cdl_value(year, x, y):
    url = "https://nassgeodata.gmu.edu/axis2/services/CDLService/GetCDLValue?year=%d&x=%.1f&y=%.1f" % (year, x, y)
    txt = http(url, timeout=40, tries=3)
    mv = re.search(r'value\W{1,4}(\d+)', txt)
    mc = re.search(r'category\W{1,4}([A-Za-z0-9 /&,_\-\.\(\)]+?)\s*["\',}<]', txt)
    if not mv:
        raise RuntimeError("unparsed CDL response: " + txt[:200])
    return int(mv.group(1)), (mc.group(1).strip() if mc else "")


def fetch_cdl(lat, lon, years=CDL_YEARS, npts=6, step=700):
    x0, y0 = albers(lat, lon)
    pts = [(x0 + (i - (npts - 1) / 2) * step, y0 + (j - (npts - 1) / 2) * step) for i in range(npts) for j in range(npts)]
    try:
        v, c = cdl_value(years[max(0, len(years) - 2)], *pts[0])
    except Exception as ex_:
        LOG["errors"].append("cdl probe %s,%s: %s" % (lat, lon, ex_))
        return None
    seq = [[None] * len(years) for _ in pts]
    names = {}
    fails = 0
    with ThreadPoolExecutor(6) as ex:
        futs = {ex.submit(cdl_value, yr, px, py): (pi, yi) for pi, (px, py) in enumerate(pts) for yi, yr in enumerate(years)}
        for fu in as_completed(futs):
            pi, yi = futs[fu]
            try:
                v, c = fu.result()
                seq[pi][yi] = v
                if c:
                    names[str(v)] = c
            except Exception as ex_:
                fails += 1
                if fails <= 3:
                    LOG["errors"].append("cdl %s,%s: %s" % (lat, lon, str(ex_)[:160]))
    return {"years": years, "seq": seq, "names": names, "fails": fails, "step_m": step, "npts": npts}


# ---------------------------------------------------------------- USDA SSURGO (Soil Data Access)
def fetch_soil(lat, lon):
    sql = ("SELECT mu.mukey, mu.muname, c.compname, c.comppct_r, c.hydgrp, c.drainagecl, c.slope_r, c.taxorder, "
           "ch.hzdept_r, ch.hzdepb_r, ch.awc_r, ch.om_r, ch.sandtotal_r, ch.silttotal_r, ch.claytotal_r, ch.kwfact, ch.ph1to1h2o_r "
           "FROM mapunit mu INNER JOIN component c ON c.mukey = mu.mukey AND c.majcompflag = 'Yes' "
           "INNER JOIN chorizon ch ON ch.cokey = c.cokey "
           "WHERE mu.mukey IN (SELECT * FROM SDA_Get_Mukey_from_intersection_with_WktWgs84('point(%s %s)')) "
           "ORDER BY c.comppct_r DESC, c.cokey, ch.hzdept_r" % (lon, lat))
    body = json.dumps({"query": sql, "format": "JSON+COLUMNNAME"})
    txt = http("https://sdmdataaccess.sc.egov.usda.gov/Tabular/post.rest", data=body,
               headers={"Content-Type": "application/json"}, timeout=30, tries=2)
    j = json.loads(txt)
    return j.get("Table")


# ---------------------------------------------------------------- NASA MODIS NDVI (ORNL DAAC)
def fetch_ndvi(lat, lon):
    base = "https://modis.ornl.gov/rst/api/v1/MOD13Q1"
    dj = json.loads(http("%s/dates?latitude=%s&longitude=%s" % (base, lat, lon), timeout=30, tries=2))
    dates = [d["modis_date"] for d in dj["dates"]]
    chunks = [dates[i:i + 10] for i in range(0, len(dates), 10)]

    def one(ch):
        url = ("%s/subset?latitude=%s&longitude=%s&band=250m_16_days_NDVI&startDate=%s&endDate=%s&kmAboveBelow=0&kmLeftRight=0"
               % (base, lat, lon, ch[0], ch[-1]))
        j = json.loads(http(url, timeout=40, tries=2))
        return [(s["calendar_date"], s["data"][0]) for s in j["subset"]]
    res = one(chunks[0])  # probe: if the service is down this raises quickly and NDVI is skipped
    fails = 0
    done = 1
    ex = ThreadPoolExecutor(4)
    futs = [ex.submit(one, c) for c in chunks[1:]]
    for fu in as_completed(futs):
        done += 1
        if done % 10 == 0:
            say("   ndvi %d / %d" % (done, len(chunks)))
        try:
            res += fu.result()
        except Exception as ex_:
            fails += 1
            if fails <= 2:
                LOG["errors"].append("ndvi %s,%s: %s" % (lat, lon, str(ex_)[:160]))
            if fails >= 5:
                say("   ndvi: too many failures, keeping what we have")
                for f2 in futs:
                    f2.cancel()
                break
    ex.shutdown(wait=False)
    res.sort()
    return {"dates": [r[0] for r in res], "ndvi": [r[1] for r in res], "scale": 0.0001, "fails": fails,
            "product": "MOD13Q1 v061 250m 16-day NDVI (ORNL DAAC subset API)"}


def attempt(label, fn, *a):
    try:
        return fn(*a)
    except Exception as ex_:
        LOG["errors"].append("%s: %s" % (label, str(ex_)[:300]))
        say("   ! %s failed: %s" % (label, str(ex_)[:160]))
        return None


def main():
    t0 = time.time()
    data = {"meta": {"created": time.strftime("%Y-%m-%d %H:%M:%S"), "eraA": ERA_A, "eraB": ERA_B, "log": LOG}, "farms": []}
    g = fetch_grid()
    data["grid"] = g
    if not g["lat"]:
        say("Grid download failed completely - check your internet connection and re-run.")
    # Loudoun: scan candidate points and keep the one with the most grain cropland (corn, soybean, wheat)
    GRAIN = (1, 4, 5, 24, 26, 27, 28, 225, 236, 238)
    for f in FARMS:
        if not f.get("scan"):
            continue
        say("[1b] Scanning %d points in %s for a grain field..." % (len(f["scan"]), f["name"]))
        best = None
        for (la, lo) in f["scan"]:
            pr = attempt("scan", fetch_cdl, la, lo, CDL_YEARS[-4:-1], 4, 500)
            if pr is None:
                continue
            vals = [v for q in pr["seq"] for v in q if v is not None]
            sh = sum(1 for v in vals if v in GRAIN) / max(1, len(vals))
            say("   %.3f,%.3f grain share %.0f%%" % (la, lo, 100 * sh))
            if best is None or sh > best[0]:
                best = (sh, la, lo)
        if best and best[0] > 0:
            f["lat"], f["lon"] = best[1], best[2]
            LOG["notes"].append("%s point chosen by scan: %.3f,%.3f grain share %.0f%%" % (f["id"], best[1], best[2], 100 * best[0]))
            say("   chosen %.3f,%.3f" % (best[1], best[2]))
    say("[2/5] Matching climate analogs...")
    sites = []
    for f in FARMS:
        rec = dict(f)
        if g["lat"]:
            i0, cands, selfd = analogs(g, f["lat"], f["lon"])
            rec["cell"] = {"lat": g["lat"][i0], "lon": g["lon"][i0]}
            rec["analogs"] = cands
            rec["self_shift"] = selfd
            say("   %s -> best analog %s" % (f["name"], cands[0]))
        data["farms"].append(rec)
        sites.append((rec, "farm", f["lat"], f["lon"]))
    say("[3/5] USDA Cropland Data Layer crop histories (slowest step)...")
    for rec in data["farms"]:
        rec["cdl"] = attempt("cdl " + rec["id"], fetch_cdl, rec["lat"], rec["lon"])
        rec["analog_site"] = None
        for a in rec.get("analogs", [])[:6]:
            probe = attempt("cdl probe analog", fetch_cdl, a["lat"], a["lon"], CDL_YEARS[-3:-2], 6, 900)
            if probe is None:
                break
            vals = [s[0] for s in probe["seq"] if s[0] is not None]
            crop = [v for v in vals if (1 <= v <= 61 and v not in (37, 59, 60)) or 66 <= v <= 77 or 204 <= v <= 254]
            say("   analog %s,%s cropland share %.0f%%" % (a["lat"], a["lon"], 100 * len(crop) / max(1, len(vals))))
            if vals and len(crop) / len(vals) >= 0.12:
                rec["analog_site"] = dict(a)
                rec["analog_site"]["cdl"] = attempt("cdl analog " + rec["id"], fetch_cdl, a["lat"], a["lon"])
                break
        if rec["analog_site"] is None and rec.get("analogs"):
            rec["analog_site"] = dict(rec["analogs"][0])
            rec["analog_site"]["cdl"] = None
            LOG["notes"].append("no cropland-rich analog found for " + rec["id"])
    say("[4/5] NASA POWER daily weather 1981-2025...")
    for rec in data["farms"]:
        rec["daily"] = attempt("daily " + rec["id"], fetch_daily, rec["lat"], rec["lon"])
        if rec.get("analog_site"):
            a = rec["analog_site"]
            a["daily"] = attempt("daily analog " + rec["id"], fetch_daily, a["lat"], a["lon"])
    def save():
        with open(OUT, "w") as f:
            json.dump(data, f, separators=(",", ":"))
    save()
    say("   core data saved to %s (steps 1-4). Step 5 adds soil and greenness." % OUT)
    say("[5/5] USDA SSURGO soil + NASA MODIS NDVI...")
    for rec in data["farms"]:
        say("   soil", rec["id"])
        rec["soil"] = attempt("soil " + rec["id"], fetch_soil, rec["lat"], rec["lon"])
        if rec.get("analog_site"):
            a = rec["analog_site"]
            a["soil"] = attempt("soil analog " + rec["id"], fetch_soil, a["lat"], a["lon"])
        save()
    for rec in data["farms"]:
        say("   ndvi", rec["id"])
        rec["ndvi"] = attempt("ndvi " + rec["id"], fetch_ndvi, rec["lat"], rec["lon"])
        save()
    say("\nDONE in %.1f min -> %s (%.1f MB)" % ((time.time() - t0) / 60, OUT, os.path.getsize(OUT) / 1e6))
    say("Problems: %d" % len(LOG["errors"]))
    for e in LOG["errors"][:12]:
        say("  -", e[:200])
    say("\nNow attach %s to the chat with Claude." % OUT)


if __name__ == "__main__":
    main()
