"""
route_plan_db.py
────────────────
Backend entry point called by Node.js (dataRoutes.js) as a subprocess.
Reads stations/sources/trucks from MongoDB, runs the dispatch optimizer,
writes the plan back to MongoDB (delivery, truckPlanning, tentativeCost).

Called as:
    python route_plan_db.py
with environment variables:
    MONGO_URI      – required
    DB_NAME        – defaults to "operations_optimization"
    PLAN_ID        – UUID injected by Node.js
    FAST_ROUTE     – "0" to use real Google API for ranking (default "1" = approx)
    GOOGLE_API_KEY – overrides logic.py key if set
"""

import os
import sys
import json
import uuid
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

import logic as lg

# ── Environment ────────────────────────────────────────────────────────────
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or lg.GOOGLE_API_KEY
lg.GOOGLE_API_KEY = GOOGLE_API_KEY

MONGO_URI  = os.getenv("MONGO_URI")
DB_NAME    = os.getenv("DB_NAME") or "operations_optimization"
PLAN_ID    = os.getenv("PLAN_ID") or str(uuid.uuid4())
FAST_ROUTE = os.getenv("FAST_ROUTE", "1") != "0"

if not MONGO_URI:
    raise RuntimeError("MONGO_URI is required to run the route planner.")

CAPACITY_BY_TYPE = {ft["type"]: ft for ft in lg.FLEET}


# ═══════════════════════════════════════════════════════════════════════════
#  DATABASE I/O
# ═══════════════════════════════════════════════════════════════════════════

def _coord_string(lat, lon):
    return f"{lat},{lon}"


def load_from_db(db):
    sources_rows = []
    for src in db["source"].find({}):
        coords = src.get("coordinates") or {}
        lat = coords.get("lat")
        lon = coords.get("lng") if coords.get("lng") is not None else coords.get("lon")
        sources_rows.append({
            "Source_ID":              src.get("source_id"),
            "Source_Name":            src.get("source_name"),
            "Coordinates":            _coord_string(lat, lon),
            "Price / MT Ex Terminal": src.get("price_per_mt_ex_terminal"),
        })

    stations_rows = []
    for st in db["station"].find({}):
        coords = st.get("coordinates") or {}
        lat = coords.get("lat")
        lon = coords.get("lng") if coords.get("lng") is not None else coords.get("lon")
        stations_rows.append({
            "Stations":         st.get("station"),
            "Coordinates":      _coord_string(lat, lon),
            "Capacity in Lt":   st.get("capacity_in_lt"),
            "Dead stock in Lt": st.get("dead_stock_in_lt"),
            "Usable Lt":        st.get("usable_lt"),
            "Now":              st.get("sufficient_fuel", "NO"),
        })

    sources  = pd.DataFrame(sources_rows)
    stations = pd.DataFrame(stations_rows)

    if sources.empty or stations.empty:
        return stations, sources

    sources["Source_ID"]   = sources["Source_ID"].astype(str).str.strip()
    sources["Source_Name"] = sources["Source_Name"].astype(str).str.strip()
    stations["Stations"]   = stations["Stations"].astype(str).str.strip()

    stations["lat"], stations["lon"] = zip(
        *stations["Coordinates"].map(lg.parse_coords))
    sources["lat"],  sources["lon"]  = zip(
        *sources["Coordinates"].map(lg.parse_coords))

    return stations, sources


def build_fleet_from_db(stations, db):
    trucks_docs = list(db["truck"].find({}).sort("truck_id", 1))
    trucks = []

    if trucks_docs:
        for doc in trucks_docs:
            truck_type = str(doc.get("type") or "").strip()
            caps = CAPACITY_BY_TYPE.get(truck_type, {})
            trucks.append({
                "truck_id":       str(doc.get("truck_id")),
                "type":           truck_type,
                "capacity_mt":    caps.get("capacity_mt", 0),
                "capacity_lt":    caps.get("capacity_lt", 0),
                "parked_station": doc.get("station"),
                "parked_lat":     float(doc.get("lat")),
                "parked_lon":     float(doc.get("lon")),
            })
        return trucks

    num = 1
    for ft in lg.FLEET:
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

    if not stations.empty:
        indices = lg.dispersion_indices(stations, len(trucks))
        for t, idx in zip(trucks, indices):
            row = stations.iloc[idx]
            t["parked_station"] = row["Stations"]
            t["parked_lat"]     = float(row["lat"])
            t["parked_lon"]     = float(row["lon"])

    return trucks


def _build_cost_summary(delivery_plans):
    summary = []
    totals  = {"purchase": 0.0, "transport": 0.0, "toll": 0.0, "grand_total": 0.0}
    for dp in delivery_plans:
        summary.append({
            "truck_id":      dp["truck_id"],
            "source_id":     dp["source_id"],
            "stations":      [s["station"] for s in dp["stops"]],
            "n_reloads":     dp.get("n_reloads", 0),
            "tot_purchase":  dp["tot_purchase"],
            "tot_transport": dp["tot_transport"],
            "tot_toll":      dp["tot_toll"],
            "grand_total":   dp["grand_total"],
        })
        totals["purchase"]    += dp["tot_purchase"]
        totals["transport"]   += dp["tot_transport"]
        totals["toll"]        += dp["tot_toll"]
        totals["grand_total"] += dp["grand_total"]
    for k in totals:
        totals[k] = round(totals[k], 2)
    return summary, totals


def write_plan(db, delivery_plans, fleet_status, trucks, start_positions):
    cost_summary, totals = _build_cost_summary(delivery_plans)
    deficit_count  = sum(len(dp.get("stops", [])) for dp in delivery_plans)
    total_reloads  = sum(dp.get("n_reloads", 0) for dp in delivery_plans)

    db["delivery"].insert_one({
        "plan_id":        PLAN_ID,
        "created_at":     datetime.utcnow(),
        "delivery_plans": delivery_plans,
        "fleet_status":   fleet_status,
        "meta": {
            "stations_in_deficit": deficit_count,
            "total_reloads":       total_reloads,
            "start_positions":     start_positions,
        },
    })

    db["truckPlanning"].insert_one({
        "plan_id":         PLAN_ID,
        "created_at":      datetime.utcnow(),
        "truck_positions": [
            {
                "truck_id": t["truck_id"],
                "type":     t["type"],
                "station":  t["parked_station"],
                "lat":      t["parked_lat"],
                "lon":      t["parked_lon"],
            }
            for t in trucks
        ],
        "meta": {},
    })

    db["tentativeCost"].insert_one({
        "plan_id":      PLAN_ID,
        "created_at":   datetime.utcnow(),
        "cost_summary": cost_summary,
        "totals":       totals,
        "meta":         {"total_reloads": total_reloads},
    })


# ═══════════════════════════════════════════════════════════════════════════
#  CORE PLAN BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def build_plan(stations, sources, trucks):
    if stations.empty or sources.empty:
        return [], [], trucks, {}

    needing = stations[stations["Now"].str.strip().str.upper() == "NO"].copy()
    if needing.empty:
        return [], [], trucks, {}

    needing_rows = [row for _, row in needing.iterrows()]
    sources_list = [row for _, row in sources.iterrows()]

    # Parallel source selection using logic.py helper
    station_data = lg.find_best_sources(
        needing_rows, sources_list, use_approx=FAST_ROUTE)

    start_positions = {
        t["truck_id"]: {
            "station": t["parked_station"],
            "lat":     t["parked_lat"],
            "lon":     t["parked_lon"],
        }
        for t in trucks
    }

    delivery_plans  = []
    truck_available = {t["truck_id"]: True for t in trucks}
    truck_by_id     = {t["truck_id"]: t for t in trucks}

    by_source = {}
    for sd in station_data:
        by_source.setdefault(sd["source_id"], []).append(sd)

    for src_id, sds in by_source.items():
        src_lat  = sds[0]["source_lat"]
        src_lon  = sds[0]["source_lon"]
        src_name = sds[0]["source_name"]
        price_mt = sds[0]["price_mt"]

        for sd in sds:
            sd["_d_src"] = lg.haversine(
                src_lat, src_lon, sd["station_lat"], sd["station_lon"])
        sds.sort(key=lambda x: x["_d_src"])
        runs = lg.balanced_partition(sds, lg.MAX_STOPS_PER_TRUCK, lg.MAX_GROUPING_KM)

        for run in runs:
            total_lt = sum(r["needed_lt"] for r in run)
            total_mt = total_lt / lg.MT_TO_LITERS

            cands = [
                (lg.haversine(t["parked_lat"], t["parked_lon"], src_lat, src_lon), t)
                for t in trucks
                if truck_available[t["truck_id"]] and t["capacity_mt"] >= total_mt
            ]
            if not cands:
                cands = [
                    (lg.haversine(t["parked_lat"], t["parked_lon"], src_lat, src_lon), t)
                    for t in trucks if truck_available[t["truck_id"]]
                ]
            if not cands:
                print(f"No truck for: {[s['station'] for s in run]}", file=sys.stderr)
                continue

            cands.sort(key=lambda x: x[0])
            _, chosen = cands[0]
            truck_available[chosen["truck_id"]] = False

            # Reload-aware journey from logic.py
            start_pos = start_positions[chosen["truck_id"]]
            journey_steps, costs = lg.build_journey(
                chosen, start_pos, src_id, src_name,
                src_lat, src_lon, run, price_mt)

            final_park = run[-1]["station"]
            final_lat  = run[-1]["station_lat"]
            final_lon  = run[-1]["station_lon"]

            stops_detail = []
            for s in run:
                deliver_step = next(
                    (st for st in journey_steps
                     if st["step_type"] == "DELIVER"
                     and st["location"] == s["station"]), {})
                stops_detail.append({
                    "station":        s["station"],
                    "needed_lt":      s["needed_lt"],
                    "needed_mt":      s["needed_mt"],
                    "dist_km":        deliver_step.get("dist_km") or 0,
                    "toll":           deliver_step.get("toll")    or 0,
                    "transport_cost": round(s["transport_cost"], 2),
                    "purchase_cost":  round(s["purchase_cost"],  2),
                    "total_cost":     round(s["total_cost"],     2),
                    "station_lat":    s["station_lat"],
                    "station_lon":    s["station_lon"],
                })

            delivery_plans.append({
                "truck_id":         chosen["truck_id"],
                "truck_type":       chosen["type"],
                "capacity_lt":      chosen["capacity_lt"],
                "initial_park":     start_pos["station"],
                "initial_park_lat": start_pos["lat"],
                "initial_park_lon": start_pos["lon"],
                "source_id":        src_id,
                "source_name":      src_name,
                "source_lat":       src_lat,
                "source_lon":       src_lon,
                "tk_src_dist":      costs["pk_src_dist"],
                "tk_src_toll":      costs["pk_src_toll"],
                "first_load_lt":    costs["first_load_lt"],
                "stops":            stops_detail,        # UI cost cards
                "journey_steps":    journey_steps,       # full sequence with RELOAD
                "n_reloads":        costs["n_reloads"],
                "final_park":       final_park,
                "final_lat":        final_lat,
                "final_lon":        final_lon,
                "total_lt":         round(total_lt),
                "total_mt":         round(total_mt, 3),
                "tot_purchase":     costs["tot_purchase"],
                "tot_transport":    costs["tot_transport"],
                "tot_toll":         costs["tot_toll"],
                "grand_total":      costs["grand_total"],
            })

            truck_by_id[chosen["truck_id"]]["parked_station"] = final_park
            truck_by_id[chosen["truck_id"]]["parked_lat"]     = final_lat
            truck_by_id[chosen["truck_id"]]["parked_lon"]     = final_lon

    used_ids = {dp["truck_id"] for dp in delivery_plans}
    fleet_status = [
        {
            "truck_id":     t["truck_id"],
            "type":         t["type"],
            "status":       "DEPLOYED" if t["truck_id"] in used_ids else "STANDBY",
            "initial_park": start_positions[t["truck_id"]]["station"],
            "final_park":   truck_by_id[t["truck_id"]]["parked_station"],
        }
        for t in trucks
    ]

    return delivery_plans, fleet_status, trucks, start_positions


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main():
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    stations, sources = load_from_db(db)
    trucks            = build_fleet_from_db(stations, db)

    delivery_plans, fleet_status, trucks, start_positions = build_plan(
        stations, sources, trucks)

    write_plan(db, delivery_plans, fleet_status, trucks, start_positions)
    client.close()

    total_reloads = sum(dp.get("n_reloads", 0) for dp in delivery_plans)
    print(f"Plan {PLAN_ID} | {len(delivery_plans)} runs | "
          f"{total_reloads} reload trips", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
        print(json.dumps({"status": "ok"}))
        sys.exit(0)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        print(json.dumps({"status": "error", "message": str(exc)}))
        sys.exit(1)