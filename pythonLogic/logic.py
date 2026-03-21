"""
=============================================================================
  LPG DISPATCH LOGIC LIBRARY
  Used by:  route_plan_db.py  (backend, MongoDB mode)
            FULL_LPG_OPTIMIZATION.py  (standalone, Excel mode)

  WHAT'S NEW IN THIS VERSION (route-plan-update branch)
  ──────────────────────────────────────────────────────
  • transport_cost_calc  – boundary fix (<=100 km uses flat rate)
  • transport_cost_empty – deadhead cost (empty truck going to source)
  • build_journey        – reload-aware step builder (detects when tank
                           runs short and inserts a RELOAD leg)
  • get_road_info        – picks cheapest of all alternative routes
  • Parallel source selection helpers exposed for route_plan_db.py
=============================================================================
"""

import pandas as pd
import requests
import math
import time
import os
import sys
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  (imported by route_plan_db.py as lg.*)
# ═══════════════════════════════════════════════════════════════════════════

STATIONS_FILE      = "clean_stationss.xlsx"
SOURCES_FILE       = "sources.xlsx"
POSITIONS_FILE     = "truck_positions.json"
GOOGLE_API_KEY     = "AIzaSyA8oVRSa2W2IzX9hg4vnaKM6hwkGRGnsP4"

FLEET = [
    {"type": "12MT", "count": 23, "capacity_mt": 12, "capacity_lt": 12 * 1810},
    {"type": "7MT",  "count":  7, "capacity_mt":  7, "capacity_lt":  7 * 1810},
]

MT_TO_LITERS        = 1810
TRANSPORT_FLAT      = 1750      # Rs/MT  dist <= 100 km  (flat rate)
TRANSPORT_PER_KM    = 6.8       # Rs/MT/RTKM  dist > 100 km
MAX_STOPS_PER_TRUCK = 3
MAX_GROUPING_KM     = 80
MAX_WORKERS         = 8         # parallel API threads
SOURCE_PREFETCH_N   = 4         # top-N nearest sources to query per station

# ═══════════════════════════════════════════════════════════════════════════
#  BASIC UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    r = math.radians
    dlat = r(lat2 - lat1); dlon = r(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(r(lat1)) * math.cos(r(lat2)) * math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def parse_coords(s):
    """Parse 'lat,lon' string. Raises ValueError on bad input."""
    try:
        parts = str(s).split(',')
        return float(parts[0].strip()), float(parts[1].strip())
    except (ValueError, IndexError):
        raise ValueError(f"Invalid coordinates: {s!r}")

def calc_transport_cost(dist_km, qty_mt):
    """
    Legacy name kept for backward compatibility with route_plan_db.py.
    Uses <= 100 boundary (flat rate wins at exactly 100 km).
    """
    return transport_cost_calc(dist_km, qty_mt)

def transport_cost_calc(dist_km, qty_mt):
    """
    Flat Rs1750/MT for dist <= 100 km.
    RTKM formula (Rs6.8 × qty_mt × dist × 2) for dist > 100 km.
    Boundary fix: at exactly 100 km use flat (old code used '<' so 100 km
    got the cheaper RTKM rate of Rs1360 instead of the correct Rs1750).
    """
    if dist_km <= 100:
        return TRANSPORT_FLAT * qty_mt
    return TRANSPORT_PER_KM * qty_mt * dist_km * 2   # RTKM = round-trip km

def transport_cost_empty(dist_km):
    """
    Deadhead cost: truck drives to source empty.
    Charged at minimum 1 MT so the flat/RTKM formula still applies.
    """
    return transport_cost_calc(dist_km, 1.0)

# ═══════════════════════════════════════════════════════════════════════════
#  GOOGLE ROUTES API  (with alternative-route picking and in-memory cache)
# ═══════════════════════════════════════════════════════════════════════════

_route_cache = {}

def get_road_info(olat, olon, dlat, dlon):
    """
    Returns (road_dist_km, toll_rs).
    Picks the cheapest alternative route (lowest transport + toll).
    Falls back to haversine × 1.3 with toll=0 if API is unreachable.
    Thread-safe: each thread writes the same value for the same key.
    """
    key = (round(olat, 4), round(olon, 4), round(dlat, 4), round(dlon, 4))
    if key in _route_cache:
        return _route_cache[key]
    try:
        resp = requests.post(
            "https://routes.googleapis.com/directions/v2:computeRoutes",
            headers={
                "Content-Type":     "application/json",
                "X-Goog-Api-Key":   GOOGLE_API_KEY,
                "X-Goog-FieldMask": (
                    "routes.distanceMeters,routes.duration,"
                    "routes.travelAdvisory.tollInfo,routes.description"
                ),
            },
            json={
                "origin":      {"location": {"latLng": {"latitude": olat, "longitude": olon}}},
                "destination": {"location": {"latLng": {"latitude": dlat, "longitude": dlon}}},
                "travelMode":  "DRIVE",
                "computeAlternativeRoutes": True,
                "extraComputations": ["TOLLS"],
                "routeModifiers": {"vehicleInfo": {"emissionType": "DIESEL"}},
            },
            timeout=15,
        )
        data = resp.json()
        if "routes" in data and data["routes"]:
            # Among all alternatives pick the one with lowest (dist_km + toll×50)
            # The ×50 weight strongly prefers low-toll routes.
            def _score(r):
                d = r["distanceMeters"] / 1000
                t = sum(float(p.get("units", 0)) + float(p.get("nanos", 0)) / 1e9
                        for p in r.get("travelAdvisory", {})
                           .get("tollInfo", {}).get("estimatedPrice", []))
                return d + t * 50
            best    = min(data["routes"], key=_score)
            dist_km = best["distanceMeters"] / 1000.0
            toll    = sum(float(p.get("units", 0)) + float(p.get("nanos", 0)) / 1e9
                         for p in best.get("travelAdvisory", {})
                            .get("tollInfo", {}).get("estimatedPrice", []))
            _route_cache[key] = (round(dist_km, 1), round(toll, 2))
            return round(dist_km, 1), round(toll, 2)
    except Exception:
        pass
    dist = round(haversine(olat, olon, dlat, dlon) * 1.3, 1)
    _route_cache[key] = (dist, 0.0)
    return dist, 0.0

# ═══════════════════════════════════════════════════════════════════════════
#  DATA LOADING  (standalone / Excel mode)
# ═══════════════════════════════════════════════════════════════════════════

def output_dir():
    d = "/mnt/user-data/outputs"
    if os.path.isdir(d): return d
    return os.path.dirname(os.path.abspath(__file__))

def resolve_path(filename):
    """Find a data file alongside the script or in known upload paths."""
    base = os.path.dirname(os.path.abspath(__file__))
    for p in [
        os.path.join(base, filename),
        os.path.join(output_dir(), filename),
        f"/mnt/user-data/uploads/{filename}",
    ]:
        if os.path.exists(p): return p
    raise FileNotFoundError(f"Cannot find {filename}")

def load_data():
    """Load stations + sources from Excel files. Returns (stations_df, sources_df)."""
    stations = pd.read_excel(resolve_path(STATIONS_FILE))
    sources  = pd.read_excel(resolve_path(SOURCES_FILE))
    stations.columns = [c.strip() for c in stations.columns]
    sources.columns  = [c.strip() for c in sources.columns]
    if "Stations "  in stations.columns:
        stations.rename(columns={"Stations ":  "Stations"},  inplace=True)
    if "Source_ID " in sources.columns:
        sources.rename( columns={"Source_ID ": "Source_ID"}, inplace=True)
    stations["Stations"] = stations["Stations"].str.strip()

    def safe_parse(s):
        try: return parse_coords(s)
        except: return (None, None)

    stations[["lat", "lon"]] = pd.DataFrame(
        stations["Coordinates"].map(safe_parse).tolist(), index=stations.index)
    sources[["lat",  "lon"]] = pd.DataFrame(
        sources["Coordinates"].map(safe_parse).tolist(),  index=sources.index)

    bad_st  = stations[stations["lat"].isna()]
    bad_src = sources[sources["lat"].isna()]
    if not bad_st.empty:
        print(f"  ⚠  Skipping {len(bad_st)} stations with bad coordinates")
    if not bad_src.empty:
        print(f"  ⚠  Skipping {len(bad_src)} sources with bad coordinates")

    stations = stations.dropna(subset=["lat", "lon"])
    sources  = sources.dropna(subset=["lat", "lon"])

    # Skip Now=NO stations with zero usable litres
    bad_lt = stations[
        (stations["Now"].str.strip().str.upper() == "NO") &
        (stations["Usable Lt"] <= 0)]
    if not bad_lt.empty:
        print(f"  ⚠  Skipping {len(bad_lt)} stations with Usable Lt=0")
    stations = stations[~(
        (stations["Now"].str.strip().str.upper() == "NO") &
        (stations["Usable Lt"] <= 0))]

    # Deduplicate
    dupes = stations[stations["Stations"].duplicated(keep=False)]
    if not dupes.empty:
        print(f"  ⚠  Duplicate stations – keeping first: "
              f"{dupes['Stations'].unique().tolist()}")
    stations = stations.drop_duplicates(subset=["Stations"], keep="first")

    return stations, sources

# ═══════════════════════════════════════════════════════════════════════════
#  TRUCK POSITIONS  (standalone / Excel mode)
# ═══════════════════════════════════════════════════════════════════════════

def positions_path():
    outputs = "/mnt/user-data/outputs"
    if os.path.isdir(outputs):
        return os.path.join(outputs, POSITIONS_FILE)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), POSITIONS_FILE)

def load_saved_positions():
    p = positions_path()
    if os.path.exists(p):
        with open(p, "r") as f:
            data = json.load(f)
        print(f"      ✓ Loaded saved positions from: {p}")
        return data
    print(f"      ℹ  No saved positions – first run")
    return None

def save_positions(trucks):
    data = {
        t["truck_id"]: {
            "station": t["parked_station"],
            "lat":     t["parked_lat"],
            "lon":     t["parked_lon"],
            "type":    t["type"],
        }
        for t in trucks
    }
    p = positions_path()
    with open(p, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"      ✓ Positions saved → {p}")

# ═══════════════════════════════════════════════════════════════════════════
#  FLEET BUILDING  (used by both modes)
# ═══════════════════════════════════════════════════════════════════════════

def dispersion_indices(stations, n):
    """Greedy max-dispersion: pick n stations as spread out as possible."""
    coords = list(zip(stations["lat"], stations["lon"]))
    chosen = [0]
    while len(chosen) < n:
        best_i, best_d = -1, -1.0
        for i in range(len(coords)):
            if i in chosen: continue
            md = min(haversine(coords[i][0], coords[i][1],
                               coords[c][0],  coords[c][1]) for c in chosen)
            if md > best_d:
                best_d, best_i = md, i
        chosen.append(best_i)
    return chosen

def build_fleet(stations):
    """
    Build fleet list for standalone/Excel mode.
    Loads saved positions from truck_positions.json; falls back to dispersion.
    """
    saved = load_saved_positions()
    trucks = []
    num = 1
    for ft in FLEET:
        for _ in range(ft["count"]):
            trucks.append({
                "truck_id":       f"T{num:02d}",
                "type":           ft["type"],
                "capacity_mt":    ft["capacity_mt"],
                "capacity_lt":    ft["capacity_lt"],
                "parked_station": None,
                "parked_lat":     None,
                "parked_lon":     None,
            })
            num += 1

    if saved:
        for t in trucks:
            if t["truck_id"] in saved:
                pos = saved[t["truck_id"]]
                t["parked_station"] = pos["station"]
                t["parked_lat"]     = pos["lat"]
                t["parked_lon"]     = pos["lon"]
            else:
                coords = list(zip(stations["lat"], stations["lon"]))
                used   = {(round(v["lat"], 4), round(v["lon"], 4))
                          for v in saved.values()}
                bi, bd = 0, -1.0
                for i, (la, lo) in enumerate(coords):
                    d = min(haversine(la, lo, u[0], u[1]) for u in used) if used else 999.0
                    if d > bd: bd, bi = d, i
                row = stations.iloc[bi]
                t["parked_station"] = row["Stations"]
                t["parked_lat"]     = float(row["lat"])
                t["parked_lon"]     = float(row["lon"])
                used.add((round(row["lat"], 4), round(row["lon"], 4)))
        print("      ↩  Trucks restored from previous run positions")
    else:
        indices = dispersion_indices(stations, len(trucks))
        for t, idx in zip(trucks, indices):
            row = stations.iloc[idx]
            t["parked_station"] = row["Stations"]
            t["parked_lat"]     = float(row["lat"])
            t["parked_lon"]     = float(row["lon"])
        print(f"      🆕 First run – trucks spread across {len(stations)} stations")

    return trucks

# ═══════════════════════════════════════════════════════════════════════════
#  BALANCED PARTITION  (4→2+2, 6→2+2+2, 5→3+2, 7→3+2+2)
# ═══════════════════════════════════════════════════════════════════════════

def balanced_partition(stations_in_group, max_stops, max_grouping_km):
    """
    Split a list of stations (all sharing the same source) into delivery runs
    with balanced stop counts and proximity grouping.
    """
    if not stations_in_group:
        return []

    n = len(stations_in_group)
    ideal_size = max_stops
    for size in range(2, max_stops + 1):
        if n % size == 0:
            ideal_size = size
            break

    remaining = stations_in_group[:]
    runs = []

    while remaining:
        run    = [remaining.pop(0)]
        target = ideal_size if len(remaining) + 1 >= ideal_size else max_stops

        while len(run) < target and remaining:
            last = run[-1]
            ni = min(
                range(len(remaining)),
                key=lambda i: haversine(
                    last["station_lat"], last["station_lon"],
                    remaining[i]["station_lat"], remaining[i]["station_lon"])
            )
            d_next = haversine(
                last["station_lat"], last["station_lon"],
                remaining[ni]["station_lat"], remaining[ni]["station_lon"])
            if d_next <= max_grouping_km:
                run.append(remaining.pop(ni))
            else:
                break

        # Rebalance: if this run hit max_stops but leaving just 1 behind → split
        if len(run) == max_stops and len(remaining) == 1:
            remaining.insert(0, run.pop())

        runs.append(run)

    return runs

# ═══════════════════════════════════════════════════════════════════════════
#  RELOAD-AWARE JOURNEY BUILDER
#
#  Builds the complete step-by-step journey for ONE truck run.
#  Automatically detects when the tank falls short before the next stop
#  and inserts a RELOAD leg (station → source → next stop) with all costs.
#
#  Step types:  INITIAL_PARK | LOAD | DELIVER | RELOAD | FINAL_PARK
#
#  Used by:
#    • route_plan_db.py  via  lg.build_journey(...)
#    • FULL_LPG_OPTIMIZATION.py  directly
# ═══════════════════════════════════════════════════════════════════════════

def build_journey(truck, start_pos_dict, src_id, src_name, src_lat, src_lon,
                  ordered_stops, price_mt):
    """
    Parameters
    ----------
    truck          : truck dict (truck_id, type, capacity_lt, …)
    start_pos_dict : {"station", "lat", "lon"}  – where the truck is right now
    ordered_stops  : list of station dicts with needed_lt, station_lat, station_lon
    price_mt       : Rs per MT at the chosen source

    Returns
    -------
    (journey_steps, cost_summary)
    journey_steps  : list of step dicts (one per UI/Excel row)
    cost_summary   : dict with tot_purchase, tot_transport, tot_toll,
                     grand_total, n_reloads, pk_src_dist, pk_src_toll,
                     first_load_lt, total_lt, total_mt
    """
    cap_lt = truck["capacity_lt"]
    steps  = []

    tot_purchase  = 0.0
    tot_transport = 0.0
    tot_toll      = 0.0
    n_reloads     = 0
    cum           = 0.0
    total_lt      = sum(s["needed_lt"] for s in ordered_stops)

    # ── INITIAL PARK ──────────────────────────────────────────────────────
    steps.append({
        "step_type":      "INITIAL_PARK",
        "label":          "🚚 INITIAL PARK",
        "location":       start_pos_dict["station"],
        "qty_lt":         None, "qty_mt":         None,
        "dist_km":        None, "toll":            None,
        "transport_cost": None, "purchase_cost":   None,
        "leg_cost":       None, "cum_cost":        None,
        "tank_after_lt":  None,
        "note":           "Truck starting position",
    })

    # ── Drive to source (truck is EMPTY) + first LOAD ─────────────────────
    pk_dist, pk_toll = get_road_info(
        start_pos_dict["lat"], start_pos_dict["lon"], src_lat, src_lon)

    # Load as many stops as the tank allows (greedy fill)
    first_load_lt = 0.0
    for stop in ordered_stops:
        if first_load_lt + stop["needed_lt"] <= cap_lt:
            first_load_lt += stop["needed_lt"]
        else:
            break
    first_load_lt = min(first_load_lt, cap_lt)
    first_load_mt = first_load_lt / MT_TO_LITERS

    pk_tc        = transport_cost_empty(pk_dist)   # empty truck → minimum 1 MT charge
    first_purch  = first_load_mt * price_mt
    tot_purchase  += first_purch
    tot_transport += pk_tc
    tot_toll      += pk_toll
    cum           += pk_tc + pk_toll + first_purch
    tank_lt        = first_load_lt

    steps.append({
        "step_type":      "LOAD",
        "label":          "⛽ SOURCE – LOAD",
        "location":       f"{src_name}  ({src_id})",
        "qty_lt":         first_load_lt,
        "qty_mt":         round(first_load_mt, 3),
        "dist_km":        pk_dist,
        "toll":           pk_toll,
        "transport_cost": round(pk_tc, 2),
        "purchase_cost":  round(first_purch, 2),
        "leg_cost":       round(pk_tc + pk_toll + first_purch, 2),
        "cum_cost":       round(cum, 2),
        "tank_after_lt":  round(first_load_lt),
        "note":           f"Loaded {first_load_lt:,.0f} Lt  (capacity {cap_lt:,.0f} Lt)",
    })

    # ── Deliver to each stop, reload if tank is short ─────────────────────
    prev_lat, prev_lon = src_lat, src_lon
    stop_seq = 0

    for i, stop in enumerate(ordered_stops):
        stop_seq += 1
        s_lat   = stop["station_lat"]
        s_lon   = stop["station_lon"]
        s_name  = stop["station"]
        need    = stop["needed_lt"]
        need_mt = need / MT_TO_LITERS

        # ── RELOAD check ──────────────────────────────────────────────────
        if tank_lt < need - 0.01:   # 0.01 Lt tolerance for float precision
            n_reloads += 1

            back_dist, back_toll = get_road_info(prev_lat, prev_lon, src_lat, src_lon)

            remaining_needed = sum(s["needed_lt"] for s in ordered_stops[i:])
            reload_lt  = min(remaining_needed, cap_lt)
            reload_mt  = reload_lt / MT_TO_LITERS
            back_tc    = transport_cost_calc(back_dist, reload_mt)
            reload_pur = reload_mt * price_mt

            tot_transport += back_tc
            tot_toll      += back_toll
            tot_purchase  += reload_pur
            cum           += back_tc + back_toll + reload_pur
            tank_lt        = reload_lt

            steps.append({
                "step_type":      "RELOAD",
                "label":          f"🔄 RELOAD at Source (trip {n_reloads})",
                "location":       f"{src_name}  ({src_id})",
                "qty_lt":         reload_lt,
                "qty_mt":         round(reload_mt, 3),
                "dist_km":        back_dist,
                "toll":           back_toll,
                "transport_cost": round(back_tc, 2),
                "purchase_cost":  round(reload_pur, 2),
                "leg_cost":       round(back_tc + back_toll + reload_pur, 2),
                "cum_cost":       round(cum, 2),
                "tank_after_lt":  round(reload_lt),
                "note": (f"⚠ Tank insufficient for next stop "
                         f"({need:,.0f} Lt needed, {tank_lt - reload_lt + reload_lt:,.0f} Lt in tank) | "
                         f"Back: {back_dist} km | Reload: {reload_lt:,.0f} Lt | "
                         f"Extra purchase: ₹{reload_pur:,.0f}"),
            })
            prev_lat, prev_lon = src_lat, src_lon

        # ── Delivery leg ──────────────────────────────────────────────────
        del_dist, del_toll = get_road_info(prev_lat, prev_lon, s_lat, s_lon)
        del_tc        = transport_cost_calc(del_dist, need_mt)
        stop_purchase = need_mt * price_mt     # paid at source; shown for reference
        leg_c         = del_tc + del_toll
        tot_transport += del_tc
        tot_toll      += del_toll
        cum           += leg_c + stop_purchase
        tank_lt       -= need

        steps.append({
            "step_type":      "DELIVER",
            "label":          f"📍 STOP {stop_seq} – DELIVER",
            "location":       s_name,
            "qty_lt":         need,
            "qty_mt":         round(need_mt, 3),
            "dist_km":        del_dist,
            "toll":           del_toll,
            "transport_cost": round(del_tc, 2),
            "purchase_cost":  round(stop_purchase, 2),
            "leg_cost":       round(leg_c + stop_purchase, 2),
            "cum_cost":       round(cum, 2),
            "tank_after_lt":  round(max(tank_lt, 0)),
            "note":           f"Delivered {need:,.0f} Lt | Tank remaining: {max(tank_lt,0):,.0f} Lt",
        })
        prev_lat, prev_lon = s_lat, s_lon

    # ── FINAL PARK ────────────────────────────────────────────────────────
    steps.append({
        "step_type":      "FINAL_PARK",
        "label":          "🏁 FINAL PARK",
        "location":       ordered_stops[-1]["station"],
        "qty_lt":         None, "qty_mt":         None,
        "dist_km":        None, "toll":            None,
        "transport_cost": None, "purchase_cost":   None,
        "leg_cost":       None, "cum_cost":        None,
        "tank_after_lt":  0,
        "note":           "Truck parked here – next run starts from this location",
    })

    cost_summary = {
        "total_lt":       total_lt,
        "total_mt":       round(total_lt / MT_TO_LITERS, 3),
        "tot_purchase":   round(tot_purchase,  2),
        "tot_transport":  round(tot_transport, 2),
        "tot_toll":       round(tot_toll,      2),
        "grand_total":    round(tot_purchase + tot_transport + tot_toll, 2),
        "n_reloads":      n_reloads,
        "pk_src_dist":    pk_dist,
        "pk_src_toll":    pk_toll,
        "first_load_lt":  first_load_lt,
    }
    return steps, cost_summary

# ═══════════════════════════════════════════════════════════════════════════
#  PARALLEL SOURCE SELECTION HELPER
#  Used by route_plan_db.py to find the cheapest source for each station.
# ═══════════════════════════════════════════════════════════════════════════

def find_best_sources(needing_rows, sources_list, use_approx=True):
    """
    For each needing station, find the cheapest source using parallel API calls.

    Parameters
    ----------
    needing_rows  : list of station Series rows  (each has lat, lon, Usable Lt, Stations)
    sources_list  : list of source Series rows   (each has lat, lon, Price/MT, Source_ID …)
    use_approx    : if True, use haversine×1.3 for ranking; always re-fetch winner with real API

    Returns
    -------
    station_data : list of station dicts with best source assigned, ready for build_plan
    """
    tasks = []
    for si, srow in enumerate(needing_rows):
        slat    = float(srow["lat"])
        slon    = float(srow["lon"])
        need_mt = float(srow["Usable Lt"]) / MT_TO_LITERS
        ranked  = sorted(sources_list,
                         key=lambda s: haversine(slat, slon,
                                                  float(s["lat"]), float(s["lon"]))
                         )[:SOURCE_PREFETCH_N]
        for src in ranked:
            tasks.append((si, src, slat, slon, need_mt))

    def _fetch(task):
        si, src, slat, slon, need_mt = task
        if use_approx:
            dist = haversine(float(src["lat"]), float(src["lon"]), slat, slon) * 1.3
            d, t = dist, 0.0
        else:
            d, t = get_road_info(float(src["lat"]), float(src["lon"]), slat, slon)
        tc  = transport_cost_calc(d, need_mt)
        pc  = float(src["Price / MT Ex Terminal"]) * need_mt
        return si, src, d, t, tc, pc, tc + pc + t

    results_by = [[] for _ in needing_rows]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for si, src, d, t, tc, pc, tot in ex.map(_fetch, tasks):
            results_by[si].append((src, d, t, tc, pc, tot))

    station_data = []
    for si, srow in enumerate(needing_rows):
        if not results_by[si]:
            continue
        slat      = float(srow["lat"])
        slon      = float(srow["lon"])
        needed_lt = float(srow["Usable Lt"])
        need_mt   = needed_lt / MT_TO_LITERS
        best_src, best_dist, best_toll, best_tc, best_pc, best_total = min(
            results_by[si], key=lambda x: x[5])

        # Always re-fetch the winner with the real Google API
        if use_approx:
            best_dist, best_toll = get_road_info(
                float(best_src["lat"]), float(best_src["lon"]), slat, slon)
            best_tc    = transport_cost_calc(best_dist, need_mt)
            best_pc    = float(best_src["Price / MT Ex Terminal"]) * need_mt
            best_total = best_pc + best_tc + best_toll

        station_data.append({
            "station":        srow["Stations"],
            "station_lat":    slat,
            "station_lon":    slon,
            "needed_lt":      needed_lt,
            "needed_mt":      need_mt,
            "source_id":      best_src["Source_ID"],
            "source_name":    best_src["Source_Name"],
            "source_lat":     float(best_src["lat"]),
            "source_lon":     float(best_src["lon"]),
            "price_mt":       float(best_src["Price / MT Ex Terminal"]),
            "dist_km":        best_dist,
            "toll_cost":      best_toll,
            "transport_cost": best_tc,
            "purchase_cost":  best_pc,
            "total_cost":     best_total,
        })
    return station_data