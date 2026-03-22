import os
import sys
import uuid
import re
from datetime import datetime
import math

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

# Time estimation constants (from logic2)
AVG_SPEED_KMH = 40
UNLOAD_MIN_PER_STOP = 30
LOAD_MIN_AT_SOURCE = 45
WORK_DAY_HOURS = 8
WORK_DAY_MIN = WORK_DAY_HOURS * 60


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


def transport_cost_empty(dist_km):
    return lg.calc_transport_cost(dist_km, 1.0)


def build_journey(
    truck,
    start_pos_dict,
    src_id,
    src_name,
    src_lat,
    src_lon,
    ordered_stops,
    price_mt,
):
    cap_lt = truck["capacity_lt"]
    steps = []

    tot_purchase = 0.0
    tot_transport = 0.0
    tot_toll = 0.0
    n_reloads = 0
    cum = 0.0

    total_lt = sum(s["needed_lt"] for s in ordered_stops)

    steps.append(
        {
            "step_type": "INITIAL_PARK",
            "label": "INITIAL PARK",
            "location": start_pos_dict["station"],
            "qty_lt": None,
            "qty_mt": None,
            "dist_km": None,
            "toll": None,
            "transport_cost": None,
            "purchase_cost": None,
            "leg_cost": None,
            "cum_cost": None,
            "tank_after_lt": None,
            "note": "Truck starting position",
        }
    )

    pk_dist, pk_toll = road_info(
        start_pos_dict["lat"], start_pos_dict["lon"], src_lat, src_lon
    )

    remaining_stops = ordered_stops[:]
    first_load_lt = 0.0
    for stop in remaining_stops:
        if first_load_lt + stop["needed_lt"] <= cap_lt:
            first_load_lt += stop["needed_lt"]
        else:
            break
    first_load_lt = min(first_load_lt, cap_lt)
    first_load_mt = first_load_lt / lg.MT_TO_LITERS

    pk_tc = transport_cost_empty(pk_dist)
    first_purch = first_load_mt * price_mt
    tot_purchase += first_purch
    tot_transport += pk_tc
    tot_toll += pk_toll
    cum += pk_tc + pk_toll + first_purch

    tank_lt = first_load_lt

    steps.append(
        {
            "step_type": "LOAD",
            "label": "SOURCE - LOAD",
            "location": f"{src_name} ({src_id})",
            "qty_lt": round(first_load_lt),
            "qty_mt": round(first_load_mt, 3),
            "dist_km": round(pk_dist, 1),
            "toll": round(pk_toll, 2),
            "transport_cost": round(pk_tc, 2),
            "purchase_cost": round(first_purch, 2),
            "leg_cost": round(pk_tc + pk_toll + first_purch, 2),
            "cum_cost": round(cum, 2),
            "tank_after_lt": round(first_load_lt),
            "note": f"Loaded {first_load_lt:,.0f} Lt (capacity {cap_lt:,.0f} Lt)",
        }
    )

    prev_lat, prev_lon = src_lat, src_lon
    stop_seq = 0

    for i, stop in enumerate(ordered_stops):
        stop_seq += 1
        s_lat = stop["station_lat"]
        s_lon = stop["station_lon"]
        s_name = stop["station"]
        need = stop["needed_lt"]
        need_mt = need / lg.MT_TO_LITERS

        if tank_lt < need - 0.01:
            n_reloads += 1

            back_dist, back_toll = road_info(prev_lat, prev_lon, src_lat, src_lon)
            remaining_needed = sum(s["needed_lt"] for s in ordered_stops[i:])
            reload_lt = min(remaining_needed, cap_lt)
            reload_mt = reload_lt / lg.MT_TO_LITERS

            back_tc = lg.calc_transport_cost(back_dist, reload_mt)
            reload_purchase = reload_mt * price_mt
            tot_transport += back_tc
            tot_toll += back_toll
            tot_purchase += reload_purchase
            cum += back_tc + back_toll + reload_purchase
            tank_lt = reload_lt

            steps.append(
                {
                    "step_type": "RELOAD",
                    "label": f"RELOAD AT SOURCE (TRIP {n_reloads})",
                    "location": f"{src_name} ({src_id})",
                    "qty_lt": round(reload_lt),
                    "qty_mt": round(reload_mt, 3),
                    "dist_km": round(back_dist, 1),
                    "toll": round(back_toll, 2),
                    "transport_cost": round(back_tc, 2),
                    "purchase_cost": round(reload_purchase, 2),
                    "leg_cost": round(back_tc + back_toll + reload_purchase, 2),
                    "cum_cost": round(cum, 2),
                    "tank_after_lt": round(reload_lt),
                    "note": (
                        f"Back to source {back_dist:.1f} km, reload {reload_lt:,.0f} Lt"
                    ),
                }
            )
            prev_lat, prev_lon = src_lat, src_lon

        del_dist, del_toll = road_info(prev_lat, prev_lon, s_lat, s_lon)

        del_tc = lg.calc_transport_cost(del_dist, need_mt)
        tot_transport += del_tc
        tot_toll += del_toll
        stop_purchase = need_mt * price_mt
        leg_c = del_tc + del_toll
        cum += leg_c + stop_purchase
        tank_lt -= need

        steps.append(
            {
                "step_type": "DELIVER",
                "label": f"STOP {stop_seq} - DELIVER",
                "location": s_name,
                "qty_lt": round(need),
                "qty_mt": round(need_mt, 3),
                "dist_km": round(del_dist, 1),
                "toll": round(del_toll, 2),
                "transport_cost": round(del_tc, 2),
                "purchase_cost": round(stop_purchase, 2),
                "leg_cost": round(leg_c + stop_purchase, 2),
                "cum_cost": round(cum, 2),
                "tank_after_lt": round(max(tank_lt, 0)),
                "note": f"Delivered {need:,.0f} Lt",
            }
        )

        prev_lat, prev_lon = s_lat, s_lon

    final_station = ordered_stops[-1]["station"]
    steps.append(
        {
            "step_type": "FINAL_PARK",
            "label": "FINAL PARK",
            "location": final_station,
            "qty_lt": None,
            "qty_mt": None,
            "dist_km": None,
            "toll": None,
            "transport_cost": None,
            "purchase_cost": None,
            "leg_cost": None,
            "cum_cost": None,
            "tank_after_lt": 0,
            "note": "Truck parked here",
        }
    )

    cost_summary = {
        "total_lt": round(total_lt),
        "total_mt": round(total_lt / lg.MT_TO_LITERS, 3),
        "tot_purchase": round(tot_purchase, 2),
        "tot_transport": round(tot_transport, 2),
        "tot_toll": round(tot_toll, 2),
        "grand_total": round(tot_purchase + tot_transport + tot_toll, 2),
        "n_reloads": n_reloads,
        "pk_src_dist": round(pk_dist, 1),
        "pk_src_toll": round(pk_toll, 2),
        "first_load_lt": round(first_load_lt),
    }

    return steps, cost_summary


def priority_score(capacity_lt, usable_lt):
    max_cap = max(ft["capacity_lt"] for ft in lg.FLEET)
    urgency = min(usable_lt / capacity_lt, 1.0) if capacity_lt > 0 else 0.5
    volume = min(usable_lt / max_cap, 1.0)
    return round((urgency * 0.6 + volume * 0.4) * 100, 1)


def estimate_run_duration_min(journey_steps):
    total_min = 0.0
    for step in journey_steps:
        stype = step.get("step_type", "")
        dist = step.get("dist_km") or 0
        if stype in ("LOAD", "RELOAD"):
            total_min += (dist / AVG_SPEED_KMH) * 60 + LOAD_MIN_AT_SOURCE
        elif stype == "DELIVER":
            total_min += (dist / AVG_SPEED_KMH) * 60 + UNLOAD_MIN_PER_STOP
    return round(total_min, 1)


def resolve_unserved(unserved_stations, delivery_plans, all_stations_df, sources_df):
    resolutions = []

    station_priority = {}
    if all_stations_df is not None and not all_stations_df.empty:
        for _, row in all_stations_df.iterrows():
            name = str(row.get("Stations", "")).strip()
            cap = float(row.get("Capacity in Lt", 0) or 0)
            dead = float(row.get("Dead stock in Lt", 0) or 0)
            if cap > 0 and dead > 0:
                station_priority[name] = priority_score(cap, dead)

    source_by_id = {}
    if sources_df is not None and not sources_df.empty:
        for _, s in sources_df.iterrows():
            source_by_id[s["Source_ID"]] = s

    max_truck_cap = max(ft["capacity_lt"] for ft in lg.FLEET)

    remaining_unserved = list(unserved_stations)
    groups = []
    while remaining_unserved:
        base = remaining_unserved.pop(0)
        group = [base]
        still = []
        for s in remaining_unserved:
            if (
                lg.haversine(
                    base["station_lat"],
                    base["station_lon"],
                    s["station_lat"],
                    s["station_lon"],
                )
                <= lg.MAX_GROUPING_KM
            ):
                group.append(s)
            else:
                still.append(s)
        remaining_unserved = still
        groups.append(group)

    trucks_assigned_today = set()

    for group in groups:
        group_lt = sum(s["needed_lt"] for s in group)
        group_names = [s["station"] for s in group]

        centroid_lat = sum(s["station_lat"] for s in group) / len(group)
        centroid_lon = sum(s["station_lon"] for s in group) / len(group)

        oversize_note = ""
        if group_lt > max_truck_cap:
            n_loads = int(math.ceil(group_lt / max_truck_cap))
            oversize_note = (
                f" Needs {group_lt:,.0f} Lt > max truck {max_truck_cap:,.0f} Lt. "
                f"Requires {n_loads} loads or reload trips."
            )

        group_resolution = {
            "grouped_with": group_names if len(group) > 1 else None,
            "group_lt": round(group_lt),
            "oversize": oversize_note,
        }

        best_truck = None
        best_remain = -1
        best_finish = None
        best_time_needed = None

        for dp in delivery_plans:
            tid = dp["truck_id"]
            if tid in trucks_assigned_today:
                continue

            est_dur = estimate_run_duration_min(dp.get("journey_steps", []))
            remaining = WORK_DAY_MIN - est_dur
            if remaining <= 0:
                continue

            truck_cap = dp.get("capacity_lt", max_truck_cap)
            if truck_cap < min(s["needed_lt"] for s in group):
                continue

            if source_by_id:
                all_dists = [
                    (
                        sid,
                        lg.haversine(
                            centroid_lat,
                            centroid_lon,
                            float(s["lat"]),
                            float(s["lon"]),
                        )
                        * 1.3,
                    )
                    for sid, s in source_by_id.items()
                ]
                _, nearest_src_dist = min(all_dists, key=lambda x: x[1])
            else:
                nearest_src_dist = (
                    lg.haversine(
                        centroid_lat, centroid_lon, dp["source_lat"], dp["source_lon"]
                    )
                    * 1.3
                )

            park_lat = dp["final_lat"]
            park_lon = dp["final_lon"]
            park_to_centroid = (
                lg.haversine(park_lat, park_lon, centroid_lat, centroid_lon) * 1.3
            )
            n_stops = len(group)
            n_reloads_extra = max(0, int(math.ceil(group_lt / truck_cap)) - 1)
            time_needed = (
                (park_to_centroid / AVG_SPEED_KMH) * 60
                + LOAD_MIN_AT_SOURCE
                + (nearest_src_dist / AVG_SPEED_KMH) * 60
                + n_stops * UNLOAD_MIN_PER_STOP
                + n_stops * (15 / AVG_SPEED_KMH) * 60
                + n_reloads_extra * LOAD_MIN_AT_SOURCE
            )

            if remaining >= time_needed and remaining > best_remain:
                best_remain = remaining
                best_truck = dp
                best_finish = est_dur
                best_time_needed = time_needed

        if best_truck is None and delivery_plans:
            best_truck = min(
                delivery_plans,
                key=lambda dp: lg.haversine(
                    dp["final_lat"], dp["final_lon"], centroid_lat, centroid_lon
                ),
            )
            best_finish = estimate_run_duration_min(best_truck.get("journey_steps", []))

        best_swap = None
        best_swap_detail = None
        for u in group:
            u_score = station_priority.get(u["station"], 50.0)
            for dp in delivery_plans:
                if dp.get("n_reloads", 0) > 0:
                    continue
                for stop in dp.get("stops", []):
                    s_score = station_priority.get(stop["station"], 50.0)
                    if s_score < u_score:
                        truck_cap = dp.get("capacity_lt", max_truck_cap)
                        if u["needed_lt"] <= truck_cap:
                            best_swap_detail = {
                                "truck_id": dp["truck_id"],
                                "drop_station": stop["station"],
                                "add_station": u["station"],
                                "drop_score": round(s_score, 1),
                                "add_score": round(u_score, 1),
                            }
                            best_swap = (
                                f"Drop '{stop['station']}' (score {s_score:.0f}) "
                                f"from {dp['truck_id']} to serve '{u['station']}' "
                                f"(score {u_score:.0f}) instead."
                            )
                        break
                if best_swap:
                    break
            if best_swap:
                break

        for u in group:
            u_score = station_priority.get(u["station"], 50.0)

            if best_truck and best_remain > 0:
                trucks_assigned_today.add(best_truck["truck_id"])
                group_note = (
                    f" Grouped with: {[s['station'] for s in group if s is not u]}"
                    if len(group) > 1
                    else ""
                )
                action = "REASSIGN TODAY"
                action_detail = (
                    f"Truck {best_truck['truck_id']} finishes in ~{best_finish:.0f} min "
                    f"and has {best_remain:.0f} min left. "
                    f"Est extra run: {best_time_needed:.0f} min."
                    f"{group_note}{oversize_note}"
                )
                when = "TODAY"
                remaining_out = round(best_remain, 0)
                time_out = round(best_time_needed, 0)

            elif best_truck:
                dist_km = round(
                    lg.haversine(
                        best_truck["final_lat"],
                        best_truck["final_lon"],
                        u["station_lat"],
                        u["station_lon"],
                    )
                    * 1.3,
                    1,
                )
                group_note = (
                    f" Grouped with: {[s['station'] for s in group if s is not u]}"
                    if len(group) > 1
                    else ""
                )
                action = "SCHEDULE TOMORROW"
                action_detail = (
                    f"Truck {best_truck['truck_id']} parks {dist_km} km away. "
                    f"First assignment tomorrow morning."
                    f"{group_note}{oversize_note}"
                )
                when = "TOMORROW"
                remaining_out = round(max(0, WORK_DAY_MIN - best_finish), 0)
                time_out = None

            else:
                action = "MANUAL REVIEW"
                action_detail = f"No trucks available. Review fleet manually.{oversize_note}"
                when = "TBD"
                remaining_out = None
                time_out = None

            resolutions.append(
                {
                    "resolution_id": str(uuid.uuid4()),
                    "decision": "PENDING",
                    "unserved_id": u.get("unserved_id"),
                    "station": u["station"],
                    "station_lat": u["station_lat"],
                    "station_lon": u["station_lon"],
                    "needed_lt": u["needed_lt"],
                    "needed_mt": round(u["needed_mt"], 3),
                    "reason": u.get("reason"),
                    "priority_score": u_score,
                    "action": action,
                    "action_detail": action_detail,
                    "truck_id": best_truck["truck_id"] if best_truck else None,
                    "truck_type": best_truck.get("truck_type") if best_truck else None,
                    "est_finish_min": round(best_finish, 0) if best_finish else None,
                    "remaining_min": remaining_out,
                    "time_needed_min": time_out,
                    "when": when,
                    "swap_candidate": best_swap,
                    "swap_detail": best_swap_detail,
                    "grouped_with": group_resolution["grouped_with"],
                    "group_lt": group_resolution["group_lt"],
                    "oversize": group_resolution["oversize"],
                }
            )

    return resolutions


def build_unserved_summary(resolutions):
    summary = {
        "total": len(resolutions),
        "today": 0,
        "tomorrow": 0,
        "manual_review": 0,
        "swap_suggestions": 0,
        "pending": len(resolutions),
        "accepted": 0,
        "rejected": 0,
    }
    for r in resolutions:
        when = r.get("when")
        if when == "TODAY":
            summary["today"] += 1
        elif when == "TOMORROW":
            summary["tomorrow"] += 1
        else:
            summary["manual_review"] += 1
        if r.get("swap_candidate"):
            summary["swap_suggestions"] += 1
    return summary


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
        return [], [], trucks, {}, [], []

    needing = stations[stations["Now"].str.strip().str.upper() == "NO"].copy()
    if needing.empty:
        return [], [], trucks, {}, [], []

    station_data = []
    for _, srow in needing.iterrows():
        slat, slon = float(srow["lat"]), float(srow["lon"])
        sname = srow["Stations"]
        needed_lt = _as_float(srow.get("Dead stock in Lt"))
        if needed_lt is None or needed_lt <= 0:
            continue
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
    unserved_stations = []
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
            total_needed_lt = sum(r["needed_lt"] for r in run)

            cands_all = [
                (
                    lg.haversine(
                        t["parked_lat"], t["parked_lon"], src_lat, src_lon
                    ),
                    t,
                )
                for t in trucks
                if truck_available[t["truck_id"]]
            ]
            if not cands_all:
                for s in run:
                    unserved_stations.append(
                        {
                            "unserved_id": str(uuid.uuid4()),
                            "status": "UNSERVED",
                            "station": s["station"],
                            "station_lat": s["station_lat"],
                            "station_lon": s["station_lon"],
                            "needed_lt": s["needed_lt"],
                            "needed_mt": s["needed_mt"],
                            "source_id": s["source_id"],
                            "source_name": s["source_name"],
                            "reason": "Could not be delivered due to shortage of trucks",
                        }
                    )
                continue

            fitting = sorted(
                [
                    (d, t)
                    for d, t in cands_all
                    if t["capacity_lt"] >= total_needed_lt
                ],
                key=lambda x: x[0],
            )
            cands = fitting if fitting else sorted(cands_all, key=lambda x: x[0])
            _, chosen = cands[0]
            truck_available[chosen["truck_id"]] = False

            price_mt = run[0]["price_mt"]
            stops_detail = []
            for stop in run:
                stops_detail.append(
                    {
                        "station": stop["station"],
                        "needed_lt": round(stop["needed_lt"]),
                        "needed_mt": round(stop["needed_mt"], 3),
                        "station_lat": stop["station_lat"],
                        "station_lon": stop["station_lon"],
                    }
                )

            journey_steps, costs = build_journey(
                chosen,
                start_positions[chosen["truck_id"]],
                src_id,
                run[0]["source_name"],
                src_lat,
                src_lon,
                run,
                price_mt,
            )

            final_park = stops_detail[-1]["station"]
            final_lat = stops_detail[-1]["station_lat"]
            final_lon = stops_detail[-1]["station_lon"]

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
                    "pk_src_dist": costs["pk_src_dist"],
                    "pk_src_toll": costs["pk_src_toll"],
                    "tk_src_dist": costs["pk_src_dist"],
                    "tk_src_toll": costs["pk_src_toll"],
                    "first_load_lt": costs["first_load_lt"],
                    "stops": stops_detail,
                    "journey_steps": journey_steps,
                    "final_park": final_park,
                    "final_lat": final_lat,
                    "final_lon": final_lon,
                    "total_lt": costs["total_lt"],
                    "total_mt": costs["total_mt"],
                    "tot_purchase": costs["tot_purchase"],
                    "tot_transport": costs["tot_transport"],
                    "tot_toll": costs["tot_toll"],
                    "grand_total": costs["grand_total"],
                    "n_reloads": costs["n_reloads"],
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

    unserved_resolutions = resolve_unserved(
        unserved_stations, delivery_plans, stations, sources
    )

    return (
        delivery_plans,
        fleet_status,
        trucks,
        start_positions,
        unserved_stations,
        unserved_resolutions,
    )


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


def write_plan(
    db, delivery_plans, fleet_status, trucks, start_positions, unserved, resolutions
):
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

    db["unservedStations"].insert_one(
        {
            "plan_id": PLAN_ID,
            "created_at": datetime.utcnow(),
            "unserved": unserved,
            "resolutions": resolutions,
            "summary": build_unserved_summary(resolutions),
            "meta": {},
        }
    )


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    stations, sources = load_from_db(db)
    trucks = build_fleet_from_db(stations, db)

    (
        delivery_plans,
        fleet_status,
        trucks,
        start_positions,
        unserved,
        resolutions,
    ) = build_plan(stations, sources, trucks)
    write_plan(
        db, delivery_plans, fleet_status, trucks, start_positions, unserved, resolutions
    )
    client.close()


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as exc:
        print(f"Route plan failed: {exc}")
        sys.exit(1)
