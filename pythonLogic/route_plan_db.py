import os
import sys
import uuid
import re
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

import logic as lg

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or lg.GOOGLE_API_KEY
lg.GOOGLE_API_KEY = GOOGLE_API_KEY

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME") or "operations_optimization"
PLAN_ID = os.getenv("PLAN_ID") or str(uuid.uuid4())

if not MONGO_URI:
    raise RuntimeError("MONGO_URI is required to run the route planner.")

CAPACITY_BY_TYPE = {ft["type"]: ft for ft in lg.FLEET}
USE_GOOGLE = bool(GOOGLE_API_KEY)
FAST_ROUTE = os.getenv("FAST_ROUTE", "1") != "0"


def _coord_string(lat, lon):
    return f"{lat},{lon}"


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_capacity_mt(truck_type):
    if not truck_type:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)", str(truck_type))
    if not match:
        return None
    return _as_float(match.group(1))


def road_info(olat, olon, dlat, dlon):
    if USE_GOOGLE:
        return lg.get_road_info(olat, olon, dlat, dlon)
    dist = lg.haversine(olat, olon, dlat, dlon) * 1.3
    return dist, 0.0


def approx_road_info(olat, olon, dlat, dlon):
    dist = lg.haversine(olat, olon, dlat, dlon) * 1.3
    return dist, 0.0


def load_from_db(db):
    sources_rows = []
    for src in db["source"].find({}):
        coords = src.get("coordinates") or {}
        lat = coords.get("lat")
        lon = coords.get("lng") if coords.get("lng") is not None else coords.get("lon")
        sources_rows.append(
            {
                "Source_ID": src.get("source_id"),
                "Source_Name": src.get("source_name"),
                "Coordinates": _coord_string(lat, lon),
                "Price / MT Ex Terminal": src.get("price_per_mt_ex_terminal"),
            }
        )

    stations_rows = []
    for st in db["station"].find({}):
        coords = st.get("coordinates") or {}
        lat = coords.get("lat")
        lon = coords.get("lng") if coords.get("lng") is not None else coords.get("lon")
        stations_rows.append(
            {
                "Stations": st.get("station"),
                "Coordinates": _coord_string(lat, lon),
                "Capacity in Lt": st.get("capacity_in_lt"),
                "Dead stock in Lt": st.get("dead_stock_in_lt"),
                "Usable Lt": st.get("usable_lt"),
                "Now": st.get("sufficient_fuel", "NO"),
            }
        )

    sources = pd.DataFrame(sources_rows)
    stations = pd.DataFrame(stations_rows)

    if sources.empty or stations.empty:
        return stations, sources

    sources["Source_ID"] = sources["Source_ID"].astype(str).str.strip()
    sources["Source_Name"] = sources["Source_Name"].astype(str).str.strip()
    stations["Stations"] = stations["Stations"].astype(str).str.strip()

    stations["lat"], stations["lon"] = zip(
        *stations["Coordinates"].map(lg.parse_coords)
    )
    sources["lat"], sources["lon"] = zip(
        *sources["Coordinates"].map(lg.parse_coords)
    )

    return stations, sources


def build_fleet_from_db(stations, db):
    trucks_docs = list(db["truck"].find({}).sort("truck_id", 1))
    trucks = []

    if trucks_docs:
        for doc in trucks_docs:
            truck_type = str(doc.get("type") or "").strip()
            caps = CAPACITY_BY_TYPE.get(truck_type, {})
            capacity_mt = _as_float(doc.get("capacity_mt"))
            capacity_lt = _as_float(doc.get("capacity_lt"))
            if capacity_mt is None:
                capacity_mt = caps.get("capacity_mt")
            if capacity_lt is None:
                capacity_lt = caps.get("capacity_lt")
            if capacity_mt is None:
                parsed = _parse_capacity_mt(truck_type)
                if parsed is not None:
                    capacity_mt = parsed
                    capacity_lt = parsed * lg.MT_TO_LITERS
            if capacity_mt is None:
                capacity_mt = 0
            if capacity_lt is None:
                capacity_lt = 0
            trucks.append(
                {
                    "truck_id": str(doc.get("truck_id")),
                    "type": truck_type,
                    "capacity_mt": capacity_mt,
                    "capacity_lt": capacity_lt,
                    "parked_station": doc.get("station"),
                    "parked_lat": float(doc.get("lat")),
                    "parked_lon": float(doc.get("lon")),
                }
            )
        return trucks

    # Fallback: use the configured fleet and spread across stations
    num = 1
    for ft in lg.FLEET:
        for _ in range(ft["count"]):
            trucks.append(
                {
                    "truck_id": f"T{num:02d}",
                    "type": ft["type"],
                    "capacity_mt": ft["capacity_mt"],
                    "capacity_lt": ft["capacity_lt"],
                    "parked_station": None,
                    "parked_lat": None,
                    "parked_lon": None,
                }
            )
            num += 1

    if not stations.empty:
        indices = lg.dispersion_indices(stations, len(trucks))
        for t, idx in zip(trucks, indices):
            row = stations.iloc[idx]
            t["parked_station"] = row["Stations"]
            t["parked_lat"] = float(row["lat"])
            t["parked_lon"] = float(row["lon"])

    return trucks


def build_plan(stations, sources, trucks):
    if stations.empty or sources.empty:
        return [], [], trucks, {}

    needing = stations[stations["Now"].str.strip().str.upper() == "NO"].copy()
    if needing.empty:
        return [], [], trucks, {}

    station_data = []
    for _, srow in needing.iterrows():
        slat, slon = float(srow["lat"]), float(srow["lon"])
        sname = srow["Stations"]
        needed_lt = float(srow["Usable Lt"])
        needed_mt = needed_lt / lg.MT_TO_LITERS

        best_src, best_total = None, float("inf")
        best_dist = best_toll = best_tc = best_pc = None

        for _, src in sources.iterrows():
            if FAST_ROUTE:
                dist_km, toll = approx_road_info(
                    float(src["lat"]), float(src["lon"]), slat, slon
                )
            else:
                dist_km, toll = road_info(
                    float(src["lat"]), float(src["lon"]), slat, slon
                )
            tc = lg.calc_transport_cost(dist_km, needed_mt)
            pc = float(src["Price / MT Ex Terminal"]) * needed_mt
            tot = pc + tc + toll
            if tot < best_total:
                best_total = tot
                best_src = src.copy()
                best_dist, best_toll, best_tc, best_pc = dist_km, toll, tc, pc

        if best_src is None:
            continue

        if FAST_ROUTE:
            dist_km, toll = road_info(
                float(best_src["lat"]), float(best_src["lon"]), slat, slon
            )
            tc = lg.calc_transport_cost(dist_km, needed_mt)
            pc = float(best_src["Price / MT Ex Terminal"]) * needed_mt
            best_dist, best_toll, best_tc, best_pc = dist_km, toll, tc, pc
            best_total = pc + tc + toll
        station_data.append(
            {
                "station": sname,
                "station_lat": slat,
                "station_lon": slon,
                "needed_lt": needed_lt,
                "needed_mt": needed_mt,
                "source_id": best_src["Source_ID"],
                "source_name": best_src["Source_Name"],
                "source_lat": float(best_src["lat"]),
                "source_lon": float(best_src["lon"]),
                "price_mt": float(best_src["Price / MT Ex Terminal"]),
                "dist_km": best_dist,
                "toll_cost": best_toll,
                "transport_cost": best_tc,
                "purchase_cost": best_pc,
                "total_cost": best_total,
            }
        )

    # Start positions snapshot
    start_positions = {
        t["truck_id"]: {
            "station": t["parked_station"],
            "lat": t["parked_lat"],
            "lon": t["parked_lon"],
        }
        for t in trucks
    }

    delivery_plans = []
    truck_available = {t["truck_id"]: True for t in trucks}
    truck_by_id = {t["truck_id"]: t for t in trucks}

    by_source = {}
    for sd in station_data:
        by_source.setdefault(sd["source_id"], []).append(sd)

    for src_id, sds in by_source.items():
        src_lat = sds[0]["source_lat"]
        src_lon = sds[0]["source_lon"]

        for sd in sds:
            sd["_d_src"] = lg.haversine(
                src_lat, src_lon, sd["station_lat"], sd["station_lon"]
            )
        sds.sort(key=lambda x: x["_d_src"])

        runs = lg.balanced_partition(sds, lg.MAX_STOPS_PER_TRUCK, lg.MAX_GROUPING_KM)

        for run in runs:
            total_lt = sum(r["needed_lt"] for r in run)
            total_mt = total_lt / lg.MT_TO_LITERS

            candidates = [
                (
                    lg.haversine(
                        t["parked_lat"], t["parked_lon"], src_lat, src_lon
                    ),
                    t,
                )
                for t in trucks
                if truck_available[t["truck_id"]] and t["capacity_mt"] >= total_mt
            ]
            if not candidates:
                candidates = [
                    (
                        lg.haversine(
                            t["parked_lat"], t["parked_lon"], src_lat, src_lon
                        ),
                        t,
                    )
                    for t in trucks
                    if truck_available[t["truck_id"]]
                ]
            if not candidates:
                candidates = [
                    (
                        lg.haversine(
                            t["parked_lat"], t["parked_lon"], src_lat, src_lon
                        ),
                        t,
                    )
                    for t in trucks
                ]
            candidates.sort(key=lambda x: x[0])
            _, chosen = candidates[0]
            truck_available[chosen["truck_id"]] = False

            tk_src_dist, tk_src_toll = road_info(
                chosen["parked_lat"], chosen["parked_lon"], src_lat, src_lon
            )

            stops_detail = []
            prev_lat, prev_lon = src_lat, src_lon
            for stop in run:
                d, tl = road_info(
                    prev_lat, prev_lon, stop["station_lat"], stop["station_lon"]
                )
                stops_detail.append(
                    {
                        "station": stop["station"],
                        "needed_lt": stop["needed_lt"],
                        "needed_mt": stop["needed_mt"],
                        "dist_km": round(d, 1),
                        "toll": round(tl, 2),
                        "transport_cost": round(stop["transport_cost"], 2),
                        "purchase_cost": round(stop["purchase_cost"], 2),
                        "total_cost": round(stop["total_cost"], 2),
                        "station_lat": stop["station_lat"],
                        "station_lon": stop["station_lon"],
                    }
                )
                prev_lat, prev_lon = stop["station_lat"], stop["station_lon"]

            final_park = stops_detail[-1]["station"]
            final_lat = stops_detail[-1]["station_lat"]
            final_lon = stops_detail[-1]["station_lon"]

            tot_purchase = round(sum(s["purchase_cost"] for s in stops_detail), 2)
            tot_transport = round(sum(s["transport_cost"] for s in stops_detail), 2)
            tot_toll = round(
                sum(s["toll"] for s in stops_detail) + tk_src_toll, 2
            )
            grand_total = round(tot_purchase + tot_transport + tot_toll, 2)

            delivery_plans.append(
                {
                    "truck_id": chosen["truck_id"],
                    "truck_type": chosen["type"],
                    "capacity_lt": chosen["capacity_lt"],
                    "initial_park": start_positions[chosen["truck_id"]]["station"],
                    "initial_park_lat": start_positions[chosen["truck_id"]]["lat"],
                    "initial_park_lon": start_positions[chosen["truck_id"]]["lon"],
                    "source_id": src_id,
                    "source_name": run[0]["source_name"],
                    "source_lat": src_lat,
                    "source_lon": src_lon,
                    "tk_src_dist": round(tk_src_dist, 1),
                    "tk_src_toll": round(tk_src_toll, 2),
                    "stops": stops_detail,
                    "final_park": final_park,
                    "final_lat": final_lat,
                    "final_lon": final_lon,
                    "total_lt": round(total_lt),
                    "total_mt": round(total_mt, 3),
                    "tot_purchase": tot_purchase,
                    "tot_transport": tot_transport,
                    "tot_toll": tot_toll,
                    "grand_total": grand_total,
                }
            )

            truck_by_id[chosen["truck_id"]]["parked_station"] = final_park
            truck_by_id[chosen["truck_id"]]["parked_lat"] = final_lat
            truck_by_id[chosen["truck_id"]]["parked_lon"] = final_lon

    used_ids = {dp["truck_id"] for dp in delivery_plans}
    fleet_status = []
    for t in trucks:
        fleet_status.append(
            {
                "truck_id": t["truck_id"],
                "type": t["type"],
                "status": "DEPLOYED" if t["truck_id"] in used_ids else "STANDBY",
                "initial_park": start_positions[t["truck_id"]]["station"],
                "final_park": truck_by_id[t["truck_id"]]["parked_station"],
            }
        )

    return delivery_plans, fleet_status, trucks, start_positions


def build_cost_summary(delivery_plans):
    cost_summary = []
    totals = {"purchase": 0.0, "transport": 0.0, "toll": 0.0, "grand_total": 0.0}

    for dp in delivery_plans:
        stations = [s["station"] for s in dp["stops"]]
        cost_summary.append(
            {
                "truck_id": dp["truck_id"],
                "source_id": dp["source_id"],
                "stations": stations,
                "tot_purchase": dp["tot_purchase"],
                "tot_transport": dp["tot_transport"],
                "tot_toll": dp["tot_toll"],
                "grand_total": dp["grand_total"],
            }
        )
        totals["purchase"] += dp["tot_purchase"]
        totals["transport"] += dp["tot_transport"]
        totals["toll"] += dp["tot_toll"]
        totals["grand_total"] += dp["grand_total"]

    for key in totals:
        totals[key] = round(totals[key], 2)

    return cost_summary, totals


def write_plan(db, delivery_plans, fleet_status, trucks, start_positions):
    cost_summary, totals = build_cost_summary(delivery_plans)
    deficit_count = sum(len(dp.get("stops", [])) for dp in delivery_plans)

    db["delivery"].insert_one(
        {
            "plan_id": PLAN_ID,
            "created_at": datetime.utcnow(),
            "delivery_plans": delivery_plans,
            "fleet_status": fleet_status,
            "meta": {
                "stations_in_deficit": deficit_count,
                "start_positions": start_positions,
            },
        }
    )

    truck_positions = []
    for t in trucks:
        truck_positions.append(
            {
                "truck_id": t["truck_id"],
                "type": t["type"],
                "station": t["parked_station"],
                "lat": t["parked_lat"],
                "lon": t["parked_lon"],
            }
        )

    db["truckPlanning"].insert_one(
        {
            "plan_id": PLAN_ID,
            "created_at": datetime.utcnow(),
            "truck_positions": truck_positions,
            "meta": {},
        }
    )

    db["tentativeCost"].insert_one(
        {
            "plan_id": PLAN_ID,
            "created_at": datetime.utcnow(),
            "cost_summary": cost_summary,
            "totals": totals,
            "meta": {},
        }
    )


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    stations, sources = load_from_db(db)
    trucks = build_fleet_from_db(stations, db)

    delivery_plans, fleet_status, trucks, start_positions = build_plan(
        stations, sources, trucks
    )
    write_plan(db, delivery_plans, fleet_status, trucks, start_positions)
    client.close()


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as exc:
        print(f"Route plan failed: {exc}")
        sys.exit(1)
