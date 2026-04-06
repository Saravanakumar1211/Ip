import { StationChangeLog } from "../models/stationChangeLog.js";

const TRACKED_FIELDS = [
  "station",
  "coordinates.lat",
  "coordinates.lng",
  "capacity_in_lt",
  "dead_stock_in_lt",
  "usable_lt",
  "sufficient_fuel"
];

const normalizeKey = (value) => String(value || "").trim().toLowerCase();

const resolveFieldValue = (obj, path) => {
  const segments = String(path || "").split(".");
  let cursor = obj;
  for (const segment of segments) {
    if (cursor === null || cursor === undefined) {
      return null;
    }
    cursor = cursor[segment];
  }
  return cursor ?? null;
};

const valuesEqual = (left, right) => {
  if (left === right) return true;
  if (left === null || left === undefined) return right === null || right === undefined;
  if (right === null || right === undefined) return false;
  if (typeof left === "number" && typeof right === "number") {
    return Number(left) === Number(right);
  }
  return String(left) === String(right);
};

const dayStartUtc = (dateInput) => {
  const date = dateInput instanceof Date ? dateInput : new Date(dateInput);
  if (Number.isNaN(date.getTime())) return null;
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
};

const dayEndUtc = (dateInput) => {
  const start = dayStartUtc(dateInput);
  if (!start) return null;
  return new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
};

export const deriveRegionFromStation = (stationName) => {
  const station = String(stationName || "").trim();
  if (!station) return "";
  const prefix = station.split("-")[0] || station;
  return String(prefix).trim();
};

export const computeStationChanges = (beforeObj, afterObj) => {
  const before = beforeObj || {};
  const after = afterObj || {};

  const changes = [];
  for (const field of TRACKED_FIELDS) {
    const previous = resolveFieldValue(before, field);
    const current = resolveFieldValue(after, field);
    if (!valuesEqual(previous, current)) {
      changes.push({
        field,
        before: previous ?? null,
        after: current ?? null
      });
    }
  }
  return changes;
};

export const recordStationAttributeChange = async ({
  beforeStation,
  afterStation,
  actor,
  source
}) => {
  const changes = computeStationChanges(beforeStation, afterStation);
  if (!changes.length) {
    return null;
  }

  const stationName = String(afterStation?.station || beforeStation?.station || "").trim();
  const region = deriveRegionFromStation(stationName);

  return StationChangeLog.create({
    station_id: afterStation?._id || beforeStation?._id,
    station_name: stationName,
    station_name_normalized: normalizeKey(stationName),
    region,
    region_normalized: normalizeKey(region),
    actor_role: String(actor?.role || "").trim() || "admin",
    actor_user_id: String(actor?.sub || "").trim() || "unknown",
    actor_name: String(actor?.name || "").trim(),
    actor_station: String(actor?.station || "").trim(),
    source: source || "admin_dashboard",
    changed_fields: changes.map((item) => item.field),
    changes,
    created_at: new Date()
  });
};

export const buildChangeDateRange = ({ preset, from, to }) => {
  const normalizedPreset = String(preset || "last_week").trim().toLowerCase();
  const now = new Date();

  if (normalizedPreset === "custom") {
    const start = dayStartUtc(from);
    const end = dayEndUtc(to);
    if (!start || !end) {
      throw new Error("Custom date range requires valid from and to dates.");
    }
    if (start.getTime() > end.getTime()) {
      throw new Error("From date cannot be after to date.");
    }
    return { $gte: start, $lte: end };
  }

  const end = now;
  const start = new Date(end);
  if (normalizedPreset === "last_month") {
    start.setDate(start.getDate() - 30);
  } else {
    start.setDate(start.getDate() - 7);
  }
  return { $gte: start, $lte: end };
};

export const fetchStationChangeReport = async ({
  station,
  region,
  actorRole,
  actorUserId,
  preset,
  from,
  to,
  limit = 1000
}) => {
  const query = {};

  if (station) {
    query.station_name_normalized = normalizeKey(station);
  }
  if (region) {
    query.region_normalized = normalizeKey(region);
  }
  if (actorRole) {
    query.actor_role = String(actorRole).trim();
  }
  if (actorUserId) {
    query.actor_user_id = String(actorUserId).trim();
  }

  query.created_at = buildChangeDateRange({ preset, from, to });

  return StationChangeLog.find(query)
    .sort({ created_at: -1 })
    .limit(Math.max(1, Number(limit) || 1000))
    .lean();
};
