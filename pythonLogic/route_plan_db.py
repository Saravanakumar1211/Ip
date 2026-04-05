import hashlib
import os
import re
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
from pymongo import MongoClient, UpdateOne

import logic3 as l3

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME") or "operations_optimization"
PLAN_ID = os.getenv("PLAN_ID") or str(uuid.uuid4())
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
USE_LIVE_ROUTES = str(os.getenv("USE_LIVE_ROUTES") or "1").strip().lower() in (
    "1",
    "true",
    "yes",
)

if not MONGO_URI:
    raise RuntimeError("MONGO_URI is required.")

if GOOGLE_API_KEY:
    l3.GOOGLE_API_KEY = GOOGLE_API_KEY


if not USE_LIVE_ROUTES:
    def _offline_get_road_info(olat, olon, dlat, dlon):
        dist = round(l3.haversine(olat, olon, dlat, dlon) * 1.3, 1)
        return dist, 0.0


    l3.get_road_info = _offline_get_road_info


def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_name(value):
    return str(value or "").replace("\u2013", "-").replace("\u2014", "-").strip().lower()


def parse_capacity_mt(value):
    match = re.search(r"(\d+(?:\.\d+)?)", str(value or ""))
    return to_float(match.group(1), None) if match else None


def get_truck_capacity_lt(doc):
    # Canonical backend conversion: 1 MT = 1810 L.
    # Prefer truck type / capacity_mt over stored capacity_lt to avoid stale values.
    cap_mt = parse_capacity_mt(doc.get("type"))
    if cap_mt is None:
        cap_mt = to_float(doc.get("capacity_mt"), None)
    if cap_mt is None:
        cap_lt = to_float(doc.get("capacity_lt"), None)
        if cap_lt is not None and cap_lt > 0:
            return cap_lt
        cap_mt = 7.0
    return cap_mt * l3.MT_TO_LITERS


def build_stations_df(stations):
    return pd.DataFrame(
        [{"Stations": s["station"], "Now": s["now"]} for s in stations]
    )


def build_sources_df(sources):
    return pd.DataFrame(
        [
            {
                "Source_ID": src["source_id"],
                "Source_Name": src["source_name"],
                "Price / MT Ex Terminal": src["price_mt"],
            }
            for src in sources
        ]
    )


def load_sales_context(db):
    monthly_docs = list(
        db["salesMonthly"].find(
            {},
            {
                "_id": 0,
                "month": 1,
                "station_name": 1,
                "total_sales_lt": 1,
                "avg_daily_sales_lt": 1,
                "days_recorded": 1,
            },
        )
    )
    if not monthly_docs:
        monthly_docs = list(
            db["monthlySales"].find(
                {},
                {
                    "_id": 0,
                    "month": 1,
                    "station_name": 1,
                    "total_sales_lt": 1,
                    "avg_daily_sales_lt": 1,
                    "days_recorded": 1,
                },
            )
        )

    daily_docs = list(
        db["salesDaily"].find(
            {},
            {
                "_id": 0,
                "date": 1,
                "month": 1,
                "station_name": 1,
                "sales_lt": 1,
            },
        )
    )

    sales_df = pd.DataFrame(monthly_docs) if monthly_docs else pd.DataFrame()
    if not sales_df.empty:
        sales_df["station_name"] = sales_df["station_name"].astype(str).str.strip()
        sales_df["month"] = sales_df["month"].astype(str).str.strip()
        sales_df["total_sales_lt"] = pd.to_numeric(sales_df["total_sales_lt"], errors="coerce").fillna(0.0)
        sales_df["avg_daily_sales_lt"] = pd.to_numeric(
            sales_df["avg_daily_sales_lt"], errors="coerce"
        ).fillna(0.0)
        sales_df["days_recorded"] = pd.to_numeric(sales_df["days_recorded"], errors="coerce").fillna(0.0)

    daily_df = pd.DataFrame(daily_docs) if daily_docs else pd.DataFrame()
    if not daily_df.empty:
        daily_df["station_name"] = daily_df["station_name"].astype(str).str.strip()
        daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
        daily_df["sales_lt"] = pd.to_numeric(daily_df["sales_lt"], errors="coerce").fillna(0.0)
        daily_df = daily_df.dropna(subset=["date"])
        daily_df["month"] = daily_df["date"].dt.strftime("%Y-%m")

    if sales_df.empty and daily_df.empty:
        return {}, pd.DataFrame(), pd.DataFrame()

    if sales_df.empty and not daily_df.empty:
        monthly_agg = (
            daily_df.groupby(["month", "station_name"], as_index=False)
            .agg(total_sales_lt=("sales_lt", "sum"), days_recorded=("sales_lt", "count"))
        )
        monthly_agg["avg_daily_sales_lt"] = monthly_agg.apply(
            lambda row: (row["total_sales_lt"] / row["days_recorded"]) if row["days_recorded"] else 0.0,
            axis=1,
        )
        sales_df = monthly_agg

    avg_sales = {}
    if not daily_df.empty:
        for station_name, group in daily_df.groupby("station_name"):
            avg_sales[station_name] = float(group["sales_lt"].mean())
    else:
        for station_name, group in sales_df.groupby("station_name"):
            avg_val = pd.to_numeric(group.get("avg_daily_sales_lt"), errors="coerce").mean()
            if pd.isna(avg_val) or avg_val <= 0:
                monthly = pd.to_numeric(group.get("total_sales_lt"), errors="coerce").mean()
                days = pd.to_numeric(group.get("days_recorded"), errors="coerce").mean()
                avg_val = (monthly / days) if days and not pd.isna(days) else 1000.0
            avg_sales[station_name] = float(avg_val)

    if not daily_df.empty:
        wide = daily_df.pivot_table(
            index="date", columns="station_name", values="sales_lt", aggfunc="sum"
        ).reset_index()
        wide = wide.rename(columns={"date": "Date"})
        wide["Date"] = pd.to_datetime(wide["Date"], errors="coerce")
    else:
        wide = sales_df.pivot_table(
            index="month", columns="station_name", values="total_sales_lt", aggfunc="sum"
        )
        wide = wide.reset_index().rename(columns={"month": "Date"})
        wide["Date"] = pd.to_datetime(wide["Date"].astype(str) + "-01", errors="coerce")

    return avg_sales, sales_df, wide


def load_from_db(db):
    source_docs = list(db["source"].find({}))
    station_docs = list(db["station"].find({}))

    sources = []
    source_by_name = {}
    source_by_id = {}
    for src in source_docs:
        coords = src.get("coordinates") or {}
        lat = to_float(coords.get("lat"), None)
        lon = to_float(coords.get("lng", coords.get("lon")), None)
        if lat is None or lon is None:
            continue
        row = {
            "source_id": str(src.get("source_id") or "").strip(),
            "source_name": str(src.get("source_name") or "").strip(),
            "source_lat": lat,
            "source_lon": lon,
            "price_mt": to_float(src.get("price_per_mt_ex_terminal"), 0.0),
        }
        if not row["source_id"]:
            continue
        sources.append(row)
        source_by_name[normalize_name(row["source_name"])] = row
        source_by_id[row["source_id"]] = row

    stations = []
    station_by_name = {}
    for st in station_docs:
        coords = st.get("coordinates") or {}
        lat = to_float(coords.get("lat"), None)
        lon = to_float(coords.get("lng", coords.get("lon")), None)
        if lat is None or lon is None:
            continue
        station_name = str(st.get("station") or "").strip()
        if not station_name:
            continue
        capacity_lt = to_float(st.get("capacity_in_lt"), 0.0)
        dead_stock_lt = max(to_float(st.get("dead_stock_in_lt"), 0.0), 0.0)
        stored_flag = str(st.get("sufficient_fuel") or "YES").strip().upper()
        if capacity_lt > 0:
            now_flag = "NO" if dead_stock_lt >= (0.6 * capacity_lt) else "YES"
        else:
            now_flag = "NO" if stored_flag == "NO" else "YES"
        row = {
            "station": station_name,
            "station_lat": lat,
            "station_lon": lon,
            "capacity_lt": capacity_lt,
            "dead_stock_lt": dead_stock_lt,
            "needed_lt": dead_stock_lt,
            "now": now_flag,
        }
        stations.append(row)
        station_by_name[normalize_name(station_name)] = row

    latest_truck_plan = db["truckPlanning"].find_one({}, sort=[("created_at", -1)])
    planned_positions = {}
    if latest_truck_plan:
        for position in latest_truck_plan.get("truck_positions", []):
            truck_id = str(position.get("truck_id") or "").strip()
            if truck_id:
                planned_positions[truck_id] = position

    trucks = []
    for truck in db["truck"].find({}).sort("truck_id", 1):
        truck_id = str(truck.get("truck_id") or "").strip()
        if not truck_id:
            continue

        planned = planned_positions.get(truck_id, {})
        maintenance_station = str(truck.get("maintenance_station") or "").strip()

        state = str(planned.get("state") or truck.get("state") or "").strip()
        source_name = str(planned.get("source") or truck.get("source") or "").strip()
        source_id = str(planned.get("source_id") or truck.get("source_id") or "").strip()
        station_name = str(planned.get("station") or truck.get("station") or "").strip()

        if state not in ("atSource", "atStation", "atMaintenance", "travelling"):
            if source_name:
                state = "atSource"
            elif station_name:
                state = "atStation"
            elif maintenance_station:
                state = "atMaintenance"
            else:
                state = "travelling"

        source_match = None
        if source_name:
            source_match = source_by_name.get(normalize_name(source_name))
        if not source_match and source_id:
            source_match = source_by_id.get(source_id)
        if source_match:
            source_name = source_match["source_name"]
            source_id = source_match["source_id"]

        station_match = station_by_name.get(normalize_name(station_name)) if station_name else None

        lat = to_float(planned.get("lat", planned.get("parked_lat")), None)
        lon = to_float(planned.get("lon", planned.get("parked_lon")), None)
        if lat is None or lon is None:
            lat = to_float(truck.get("lat"), None)
            lon = to_float(truck.get("lon"), None)
        if (lat is None or lon is None) and source_match and state == "atSource":
            lat, lon = source_match["source_lat"], source_match["source_lon"]
        elif (lat is None or lon is None) and station_match and state == "atStation":
            lat, lon = station_match["station_lat"], station_match["station_lon"]
        if lat is None or lon is None:
            lat, lon = 0.0, 0.0

        if state == "atSource":
            parked_label = source_name or (source_match["source_name"] if source_match else None)
        elif state == "atStation":
            parked_label = station_name or (station_match["station"] if station_match else None)
        elif state == "atMaintenance":
            parked_label = maintenance_station or station_name or "Maintenance"
        else:
            parked_label = station_name or source_name or "Travelling"

        trucks.append(
            {
                "truck_id": truck_id,
                "type": str(truck.get("type") or "").strip() or "7MT",
                "capacity_lt": get_truck_capacity_lt(truck),
                "capacity_mt": round(get_truck_capacity_lt(truck) / l3.MT_TO_LITERS, 3),
                "state": state,
                "parked_station": station_name or None,
                "parked_source": source_name or None,
                "parked_source_id": source_id or None,
                "parked_label": parked_label,
                "parked_lat": lat,
                "parked_lon": lon,
            }
        )

    return stations, sources, trucks


def allocate_delivery_quantities(stops, truck_capacity_lt, avg_sales):
    total_needed = sum(float(stop.get("needed_lt") or 0.0) for stop in stops)
    capacity = max(float(truck_capacity_lt or 0.0), 0.0)
    if not stops or capacity <= 0:
        for stop in stops:
            stop["deliver_lt"] = 0
            stop["deliver_mt"] = 0.0
        return False

    if total_needed <= capacity:
        for stop in stops:
            deliver = round(float(stop.get("needed_lt") or 0.0))
            stop["deliver_lt"] = max(deliver, 0)
            stop["deliver_mt"] = round(stop["deliver_lt"] / l3.MT_TO_LITERS, 3)
        return False

    if len(stops) == 1:
        deliver = min(round(float(stops[0].get("needed_lt") or 0.0)), round(capacity))
        stops[0]["deliver_lt"] = max(deliver, 0)
        stops[0]["deliver_mt"] = round(stops[0]["deliver_lt"] / l3.MT_TO_LITERS, 3)
        return False

    # Split should be used only when combined demand exceeds truck capacity.
    l3.compute_delivery_quantities(stops, capacity, avg_sales)

    for stop in stops:
        needed = max(round(float(stop.get("needed_lt") or 0.0)), 0)
        proposed = max(round(float(stop.get("deliver_lt") or 0.0)), 0)
        stop["deliver_lt"] = min(proposed, needed)

    delivered = sum(stop["deliver_lt"] for stop in stops)
    capacity_left = max(round(capacity) - delivered, 0)
    sales_weights = [
        max(float(l3.get_sales_avg(avg_sales, stop.get("station") or "")), 0.0)
        for stop in stops
    ]

    while capacity_left > 0:
        unmet = [
            (
                idx,
                max(round(float(stop.get("needed_lt") or 0.0)) - int(stop.get("deliver_lt") or 0), 0),
            )
            for idx, stop in enumerate(stops)
        ]
        unmet = [item for item in unmet if item[1] > 0]
        if not unmet:
            break
        idx, need_left = max(
            unmet,
            key=lambda item: (
                sales_weights[item[0]],
                item[1],
            ),
        )
        add = min(capacity_left, need_left)
        stops[idx]["deliver_lt"] += add
        capacity_left -= add

    for stop in stops:
        stop["deliver_mt"] = round(float(stop["deliver_lt"]) / l3.MT_TO_LITERS, 3)

    return True


def assign_cheapest_source(stations, sources):
    station_data = []
    for station in stations:
        needed_mt = station["needed_lt"] / l3.MT_TO_LITERS if station["needed_lt"] else 0.0
        if needed_mt <= 0:
            continue

        best = None
        best_total = float("inf")
        for src in sources:
            dist_km, toll = l3.get_road_info(
                src["source_lat"], src["source_lon"], station["station_lat"], station["station_lon"]
            )
            transport = l3.transport_cost_calc(dist_km, needed_mt)
            purchase = src["price_mt"] * needed_mt
            total = transport + purchase + toll
            if total < best_total:
                best_total = total
                best = (src, dist_km, toll)

        if not best:
            continue

        src, src_dist, src_toll = best
        station_data.append(
            {
                "station": station["station"],
                "station_lat": station["station_lat"],
                "station_lon": station["station_lon"],
                "needed_lt": station["needed_lt"],
                "needed_mt": round(station["needed_lt"] / l3.MT_TO_LITERS, 3),
                "source_id": src["source_id"],
                "source_name": src["source_name"],
                "source_lat": src["source_lat"],
                "source_lon": src["source_lon"],
                "price_mt": src["price_mt"],
                "source_station_dist_km": src_dist,
                "source_station_toll": src_toll,
            }
        )

    return station_data


def build_plan(stations, sources, trucks, avg_sales):
    needing = [s for s in stations if s["now"] == "NO" and s["needed_lt"] > 0]
    if not needing:
        return [], [], trucks, []

    station_data = assign_cheapest_source(needing, sources)

    by_source = defaultdict(list)
    for row in station_data:
        by_source[row["source_id"]].append(row)

    start_pos = {
        t["truck_id"]: {
            "station": t["parked_label"],
            "lat": t["parked_lat"],
            "lon": t["parked_lon"],
        }
        for t in trucks
    }

    truck_available = {t["truck_id"]: True for t in trucks}
    truck_by_id = {t["truck_id"]: t for t in trucks}
    delivery_plans = []
    unserved = []

    for src_id, source_rows in by_source.items():
        src_lat = source_rows[0]["source_lat"]
        src_lon = source_rows[0]["source_lon"]
        src_name = source_rows[0]["source_name"]
        price_mt = source_rows[0]["price_mt"]

        for row in source_rows:
            row["_d_src"] = l3.haversine(src_lat, src_lon, row["station_lat"], row["station_lon"])
        source_rows.sort(key=lambda item: item["_d_src"])
        runs = l3.balanced_partition(source_rows, l3.MAX_STOPS_PER_TRUCK, l3.MAX_GROUPING_KM)

        for run in runs:
            for stop in run:
                stop["deliver_lt"] = stop["needed_lt"]
                stop["deliver_mt"] = round(stop["needed_lt"] / l3.MT_TO_LITERS, 3)
                stop["avg_sales"] = round(float(l3.get_sales_avg(avg_sales, stop["station"])), 2)

            candidates_all = [
                (
                    l3.haversine(t["parked_lat"], t["parked_lon"], src_lat, src_lon),
                    t,
                )
                for t in trucks
                if truck_available[t["truck_id"]]
            ]
            if not candidates_all:
                for stop in run:
                    needed = round(stop["needed_lt"])
                    unserved.append(
                        {
                            "unserved_id": str(uuid.uuid4()),
                            "station": stop["station"],
                            "needed_lt": needed,
                            "needed_mt": round(needed / l3.MT_TO_LITERS, 3),
                            "station_lat": stop["station_lat"],
                            "station_lon": stop["station_lon"],
                            "source_id": stop["source_id"],
                            "source_name": stop["source_name"],
                            "reason": "No truck available",
                            "status": "UNSERVED",
                        }
                    )
                continue

            total_need_lt = sum(stop["needed_lt"] for stop in run)
            fitting = sorted(
                [(d, t) for d, t in candidates_all if t["capacity_lt"] >= total_need_lt],
                key=lambda item: item[0],
            )
            if fitting:
                _, chosen = fitting[0]
            else:
                # If no truck can fully satisfy combined demand, prefer the largest
                # available capacity first (maximise delivered litres), then nearest.
                best = sorted(
                    candidates_all,
                    key=lambda item: (-float(item[1].get("capacity_lt") or 0.0), item[0]),
                )
                _, chosen = best[0]
            truck_available[chosen["truck_id"]] = False

            split_used = allocate_delivery_quantities(run, chosen["capacity_lt"], avg_sales)
            journey_steps, costs = l3.build_journey(
                chosen,
                start_pos[chosen["truck_id"]],
                src_id,
                src_name,
                src_lat,
                src_lon,
                run,
                price_mt,
            )

            stops_payload = []
            for stop in run:
                needed_lt = round(float(stop["needed_lt"]))
                deliver_lt = round(float(stop["deliver_lt"]))
                needed_mt = round(needed_lt / l3.MT_TO_LITERS, 3)
                deliver_mt = round(deliver_lt / l3.MT_TO_LITERS, 3)
                shortfall_lt = max(0, needed_lt - deliver_lt)
                if shortfall_lt > 0:
                    unserved.append(
                        {
                            "unserved_id": str(uuid.uuid4()),
                            "station": stop["station"],
                            "needed_lt": shortfall_lt,
                            "needed_mt": round(shortfall_lt / l3.MT_TO_LITERS, 3),
                            "station_lat": stop["station_lat"],
                            "station_lon": stop["station_lon"],
                            "source_id": src_id,
                            "source_name": src_name,
                            "reason": "Truck capacity lower than combined demand",
                            "status": "UNSERVED",
                        }
                    )

                stops_payload.append(
                    {
                        "station": stop["station"],
                        "needed_lt": needed_lt,
                        "needed_mt": needed_mt,
                        "deliver_lt": deliver_lt,
                        "deliver_mt": deliver_mt,
                        "split_pct": 0.0,
                        "avg_sales": stop.get("avg_sales"),
                        "station_lat": stop["station_lat"],
                        "station_lon": stop["station_lon"],
                    }
                )

            total_load_lt = round(costs["total_deliver_lt"])
            total_load_mt = round(total_load_lt / l3.MT_TO_LITERS, 3)
            if total_load_lt > 0:
                for stop in stops_payload:
                    stop["split_pct"] = round((float(stop["deliver_lt"]) / total_load_lt) * 100, 1)
            else:
                for stop in stops_payload:
                    stop["split_pct"] = 0.0

            split_by_station = {stop["station"]: stop["split_pct"] for stop in stops_payload}
            for step in journey_steps:
                if step.get("step_type") == "DELIVER":
                    step["split_pct"] = split_by_station.get(step.get("location"), 0.0)

            delivery_plans.append(
                {
                    "truck_id": chosen["truck_id"],
                    "truck_type": chosen["type"],
                    "capacity_lt": round(chosen["capacity_lt"]),
                    "capacity_mt": round(chosen["capacity_lt"] / l3.MT_TO_LITERS, 3),
                    "initial_park": start_pos[chosen["truck_id"]]["station"],
                    "initial_park_lat": start_pos[chosen["truck_id"]]["lat"],
                    "initial_park_lon": start_pos[chosen["truck_id"]]["lon"],
                    "source_id": src_id,
                    "source_name": src_name,
                    "source_lat": src_lat,
                    "source_lon": src_lon,
                    "pk_src_dist": costs["pk_src_dist"],
                    "pk_src_toll": costs["pk_src_toll"],
                    "total_lt": total_load_lt,
                    "total_mt": total_load_mt,
                    "total_load_lt": total_load_lt,
                    "total_load_mt": total_load_mt,
                    "stops": stops_payload,
                    "journey_steps": journey_steps,
                    "last_delivery_stop": stops_payload[-1]["station"] if stops_payload else None,
                    "final_park": src_name,
                    "final_lat": src_lat,
                    "final_lon": src_lon,
                    "tot_purchase": round(costs["tot_purchase"], 2),
                    "tot_transport": round(costs["tot_transport"], 2),
                    "tot_toll": round(costs["tot_toll"], 2),
                    "grand_total": round(costs["grand_total"], 2),
                    "n_reloads": 0,
                    "split_used": split_used,
                }
            )

            truck_by_id[chosen["truck_id"]]["parked_station"] = None
            truck_by_id[chosen["truck_id"]]["parked_source"] = src_name
            truck_by_id[chosen["truck_id"]]["parked_source_id"] = src_id
            truck_by_id[chosen["truck_id"]]["parked_label"] = src_name
            truck_by_id[chosen["truck_id"]]["parked_lat"] = src_lat
            truck_by_id[chosen["truck_id"]]["parked_lon"] = src_lon
            truck_by_id[chosen["truck_id"]]["state"] = "atSource"

    used_ids = {plan["truck_id"] for plan in delivery_plans}
    fleet_status = []
    for truck in trucks:
        fleet_status.append(
            {
                "truck_id": truck["truck_id"],
                "type": truck["type"],
                "status": "DEPLOYED" if truck["truck_id"] in used_ids else "STANDBY",
                "initial_park": start_pos[truck["truck_id"]]["station"],
                "final_park": truck.get("parked_label"),
            }
        )

    return delivery_plans, fleet_status, trucks, unserved


def build_cost_summary(delivery_plans):
    totals = {"purchase": 0.0, "transport": 0.0, "toll": 0.0, "grand_total": 0.0}
    rows = []
    for plan in delivery_plans:
        row = {
            "truck_id": plan["truck_id"],
            "source_id": plan["source_id"],
            "stations": [stop["station"] for stop in plan.get("stops", [])],
            "tot_purchase": round(plan.get("tot_purchase", 0.0), 2),
            "tot_transport": round(plan.get("tot_transport", 0.0), 2),
            "tot_toll": round(plan.get("tot_toll", 0.0), 2),
            "grand_total": round(plan.get("grand_total", 0.0), 2),
        }
        rows.append(row)
        totals["purchase"] += row["tot_purchase"]
        totals["transport"] += row["tot_transport"]
        totals["toll"] += row["tot_toll"]
        totals["grand_total"] += row["grand_total"]

    totals = {k: round(v, 2) for k, v in totals.items()}
    return rows, totals


def build_source_comparison(sources_df, delivery_plans):
    source_rows = []
    if sources_df is not None and not sources_df.empty:
        source_rows = sources_df.to_dict("records")

    usage = defaultdict(
        lambda: {
            "runs": 0,
            "total_lt": 0.0,
            "total_cost": 0.0,
            "total_purchase": 0.0,
        }
    )
    for plan in delivery_plans:
        sid = plan.get("source_id")
        if not sid:
            continue
        usage[sid]["runs"] += 1
        usage[sid]["total_lt"] += float(plan.get("total_load_lt") or plan.get("total_lt") or 0.0)
        usage[sid]["total_cost"] += float(plan.get("grand_total") or 0.0)
        usage[sid]["total_purchase"] += float(plan.get("tot_purchase") or 0.0)

    if not source_rows:
        out = []
        for sid, row in usage.items():
            mt = row["total_lt"] / l3.MT_TO_LITERS if row["total_lt"] else 0.0
            out.append(
                {
                    "source_id": sid,
                    "source_name": sid,
                    "rank": None,
                    "price_per_mt": None,
                    "vs_cheapest_per_mt": None,
                    "recommendation": "",
                    "runs": row["runs"],
                    "total_lt": round(row["total_lt"], 0),
                    "total_mt": round(mt, 3),
                    "avg_cost_per_mt": round(row["total_cost"] / mt, 2) if mt else 0.0,
                    "total_purchase": round(row["total_purchase"], 2),
                }
            )
        out.sort(key=lambda item: item["avg_cost_per_mt"])
        return out

    df = pd.DataFrame(source_rows).copy()
    df["Source_ID"] = df["Source_ID"].astype(str)
    df["Source_Name"] = df["Source_Name"].astype(str)
    df["Price / MT Ex Terminal"] = pd.to_numeric(
        df["Price / MT Ex Terminal"], errors="coerce"
    ).fillna(0.0)
    df = df.sort_values("Price / MT Ex Terminal", ascending=True).reset_index(drop=True)
    cheapest = float(df["Price / MT Ex Terminal"].iloc[0]) if not df.empty else 0.0

    out = []
    for idx, row in df.iterrows():
        sid = str(row["Source_ID"])
        sname = str(row["Source_Name"])
        price = float(row["Price / MT Ex Terminal"])
        vs_cheapest = price - cheapest
        row_usage = usage[sid]
        mt = row_usage["total_lt"] / l3.MT_TO_LITERS if row_usage["total_lt"] else 0.0
        if idx == 0:
            rec = "Cheapest terminal - preferred"
        elif idx <= 2:
            rec = "Competitive option"
        else:
            rec = f"Costlier by Rs {vs_cheapest:,.0f}/MT"
        out.append(
            {
                "source_id": sid,
                "source_name": sname,
                "rank": int(idx + 1),
                "price_per_mt": round(price, 2),
                "vs_cheapest_per_mt": round(vs_cheapest, 2),
                "recommendation": rec,
                "runs": int(row_usage["runs"]),
                "total_lt": round(row_usage["total_lt"], 0),
                "total_mt": round(mt, 3),
                "avg_cost_per_mt": round(row_usage["total_cost"] / mt, 2) if mt else 0.0,
                "total_purchase": round(row_usage["total_purchase"], 2),
            }
        )
    return out


def build_station_intelligence(sales_df, sales_raw):
    if sales_df.empty and sales_raw.empty:
        return {
            "top_stations": [],
            "volatile_stations": [],
            "monthly_summary": [],
            "all_stations": [],
        }

    monthly_summary = []
    if not sales_df.empty:
        monthly_summary = (
            sales_df.groupby("month", as_index=False)["total_sales_lt"]
            .sum()
            .sort_values("month")
            .to_dict("records")
        )
    latest_month = monthly_summary[-1]["month"] if monthly_summary else ""

    station_stats = []
    if not sales_raw.empty:
        df = sales_raw.copy()
        if "Date" in df.columns:
            value_cols = [c for c in df.columns if c != "Date"]
        else:
            value_cols = list(df.columns)
        for col in value_cols:
            vals = pd.to_numeric(df[col], errors="coerce").dropna().values
            if len(vals) == 0:
                continue
            mean = float(vals.mean())
            std = float(vals.std(ddof=0))
            max_day = float(vals.max())
            min_day = float(vals.min())
            monthly_total = float(vals.sum())
            cv = (std / mean * 100) if mean else 0.0
            if mean > 3000:
                demand_level = "Very High"
            elif mean > 1500:
                demand_level = "High"
            elif mean > 800:
                demand_level = "Medium"
            else:
                demand_level = "Low"
            station_stats.append(
                {
                    "station_name": str(col).strip(),
                    "month": latest_month,
                    "avg_daily_sales_lt": round(mean, 2),
                    "std_dev_lt": round(std, 2),
                    "max_day_lt": round(max_day, 2),
                    "min_day_lt": round(min_day, 2),
                    "total_sales_lt": round(monthly_total, 2),
                    "cv_pct": round(cv, 2),
                    "demand_level": demand_level,
                }
            )
    else:
        monthly_group = sales_df.groupby("station_name")
        for station_name, group in monthly_group:
            avg_daily = float(pd.to_numeric(group["avg_daily_sales_lt"], errors="coerce").mean())
            vals = pd.to_numeric(group["total_sales_lt"], errors="coerce").dropna()
            std = float(vals.std(ddof=0)) if not vals.empty else 0.0
            cv = (std / avg_daily * 100) if avg_daily else 0.0
            station_stats.append(
                {
                    "station_name": station_name,
                    "month": latest_month,
                    "avg_daily_sales_lt": round(avg_daily, 2),
                    "std_dev_lt": round(std, 2),
                    "max_day_lt": 0.0,
                    "min_day_lt": 0.0,
                    "total_sales_lt": round(float(vals.sum()) if not vals.empty else 0.0, 2),
                    "cv_pct": round(cv, 2),
                    "demand_level": "Medium",
                }
            )

    station_stats.sort(key=lambda item: item["avg_daily_sales_lt"], reverse=True)
    top_stations = station_stats[:20]
    volatile_stations = sorted(station_stats, key=lambda item: item["cv_pct"], reverse=True)[:20]

    return {
        "top_stations": top_stations,
        "volatile_stations": volatile_stations,
        "monthly_summary": monthly_summary,
        "all_stations": station_stats,
    }


def build_kpi_cards(kpi_dict):
    return [
        {
            "title": "LPG To Be Delivered Today",
            "value": f"{kpi_dict.get('total_lt', 0):,.0f} Lt ({kpi_dict.get('total_mt', 0):,.2f} MT)",
            "description": f"{kpi_dict.get('total_runs', 0)} run(s) in the suggested delivery sequence.",
            "status": "OK" if kpi_dict.get("total_runs", 0) > 0 else "NO RUNS",
        },
        {
            "title": "Trucks To Be Deployed",
            "value": f"{kpi_dict.get('trucks_deployed', 0)} / {kpi_dict.get('trucks_total', 0)}",
            "description": (
                f"Standby {kpi_dict.get('trucks_standby', 0)} | Utilisation {kpi_dict.get('utilisation_pct', 0):.1f}%"
            ),
            "status": "OK" if kpi_dict.get("trucks_deployed", 0) > 0 else "LOW",
        },
        {
            "title": "Stations Fully Deliverable",
            "value": f"{kpi_dict.get('stations_served', 0)} / {kpi_dict.get('stations_needing', 0)}",
            "description": "",
            "status": "OK" if kpi_dict.get("stations_unserved", 0) == 0 else "ALERT",
        },
        {
            "title": "Total Cost Estimate For Today",
            "value": f"Rs {kpi_dict.get('grand_total', 0):,.0f}",
            "description": (
                f"Purchase Rs {kpi_dict.get('tot_purchase', 0):,.0f} | "
                f"Transport Rs {kpi_dict.get('tot_transport', 0):,.0f} | "
                f"Toll Rs {kpi_dict.get('tot_toll', 0):,.0f}"
            ),
            "status": "OK",
        },
        {
            "title": "Cost Estimate Per MT",
            "value": f"Rs {kpi_dict.get('cost_per_mt', 0):,.0f}",
            "description": f"Cost per litre: Rs {kpi_dict.get('cost_per_lt', 0):,.4f}",
            "status": "OK",
        },
        {
            "title": "Purchase vs Transport",
            "value": (
                f"Purchase {kpi_dict.get('purchase_pct', 0):.1f}% | "
                f"Transport {kpi_dict.get('transport_pct', 0):.1f}% | "
                f"Toll {kpi_dict.get('toll_pct', 0):.1f}%"
            ),
            "description": "Cost split for today's plan",
            "status": "INFO",
        },
        {
            "title": "Sources Used",
            "value": f"{kpi_dict.get('sources_used', 0)}",
            "description": "Unique source terminals selected by optimizer",
            "status": "OK",
        },
        {
            "title": "Monthly Sales",
            "value": f"{kpi_dict.get('monthly_total_lt', 0):,.0f} Lt",
            "description": "",
            "status": "INFO",
        },
    ]


def refresh_news(db):
    try:
        articles, fetched_at, _errors = l3.fetch_lpg_news()
    except Exception:
        return

    if not articles:
        return

    ops = []
    for article in articles[:60]:
        title = str(article.get("title") or "").strip()
        url = str(article.get("url") or "").strip()
        if not title:
            continue
        dedupe_key = hashlib.sha1(f"{title.lower()}|{url}".encode("utf-8")).hexdigest()
        summary = str(article.get("description") or "").strip()
        source = str(article.get("source") or "Google News").strip() or "Google News"
        published = article.get("published_dt")
        ops.append(
            UpdateOne(
                {"dedupe_key": dedupe_key},
                {
                    "$set": {
                        "title": title,
                        "url": url,
                        "source": source,
                        "summary": summary[:600],
                        "category": "LPG Industry",
                        "published_at": published,
                        "fetched_at": fetched_at,
                        "dedupe_key": dedupe_key,
                    }
                },
                upsert=True,
            )
        )

    if ops:
        db["lpgNews"].bulk_write(ops, ordered=False)


def write_plan(db, delivery_plans, fleet_status, trucks, unserved, stations_df, sources_df, sales_df, sales_raw):
    created_at = datetime.now(timezone.utc)
    cost_summary, totals = build_cost_summary(delivery_plans)

    db["delivery"].insert_one(
        {
            "plan_id": PLAN_ID,
            "created_at": created_at,
            "delivery_plans": delivery_plans,
            "fleet_status": fleet_status,
            "meta": {
                "rules": {
                    "max_stops_per_truck": int(l3.MAX_STOPS_PER_TRUCK),
                    "no_refueling": True,
                    "return_to_same_source": True,
                    "alternating_split": True,
                    "source": "pythonLogic/logic3.py",
                }
            },
        }
    )

    db["truckPlanning"].insert_one(
        {
            "plan_id": PLAN_ID,
            "created_at": created_at,
            "truck_positions": [
                {
                    "truck_id": truck["truck_id"],
                    "type": truck["type"],
                    "station": truck.get("parked_station"),
                    "source": truck.get("parked_source"),
                    "source_id": truck.get("parked_source_id"),
                    "lat": truck.get("parked_lat"),
                    "lon": truck.get("parked_lon"),
                    "state": truck.get("state"),
                }
                for truck in trucks
            ],
            "meta": {"source": "pythonLogic/logic3.py"},
        }
    )

    db["tentativeCost"].insert_one(
        {
            "plan_id": PLAN_ID,
            "created_at": created_at,
            "cost_summary": cost_summary,
            "totals": totals,
            "meta": {"source": "pythonLogic/logic3.py"},
        }
    )

    db["unservedStations"].insert_one(
        {
            "plan_id": PLAN_ID,
            "created_at": created_at,
            "unserved": unserved,
            "resolutions": [],
            "summary": {
                "total": len(unserved),
                "today": len(unserved),
                "tomorrow": 0,
                "manual_review": 0,
                "swap_suggestions": 0,
                "pending": len(unserved),
                "accepted": 0,
                "rejected": 0,
            },
            "meta": {"source": "pythonLogic/logic3.py"},
        }
    )

    kpi_dict = l3.compute_kpis(delivery_plans, fleet_status, stations_df, sources_df, sales_raw)
    analytics_doc = {
        "plan_id": PLAN_ID,
        "created_at": created_at,
        "kpi_dashboard": build_kpi_cards(kpi_dict),
        "kpi_summary": kpi_dict,
        "source_comparison": build_source_comparison(sources_df, delivery_plans),
        "station_intelligence": build_station_intelligence(sales_df, sales_raw),
        "meta": {"source": "pythonLogic/logic3.py"},
    }
    db["analyticsDashboard"].insert_one(analytics_doc)

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    stations, sources, trucks = load_from_db(db)
    avg_sales, sales_df, sales_raw = load_sales_context(db)
    stations_df = build_stations_df(stations)
    sources_df = build_sources_df(sources)

    delivery_plans, fleet_status, trucks, unserved = build_plan(stations, sources, trucks, avg_sales)
    write_plan(
        db,
        delivery_plans,
        fleet_status,
        trucks,
        unserved,
        stations_df,
        sources_df,
        sales_df,
        sales_raw,
    )
    refresh_news(db)

    client.close()


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as exc:
        print(f"Route plan failed: {exc}")
        sys.exit(1)
