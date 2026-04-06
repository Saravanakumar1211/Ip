import { Router } from "express";
import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import { randomUUID } from "crypto";

import { Source } from "../models/source.js";
import { Station } from "../models/station.js";
import { Truck } from "../models/truck.js";
import { Delivery } from "../models/delivery.js";
import { TruckPlanning } from "../models/truckPlanning.js";
import { TentativeCost } from "../models/tentativeCost.js";
import { UnservedStations } from "../models/unservedStations.js";
import { AnalyticsDashboard } from "../models/analyticsDashboard.js";
import { runImport } from "../services/importData.js";
import {
  fetchStationChangeReport,
  recordStationAttributeChange
} from "../services/stationChangeLogService.js";
import { authenticate, requireRole } from "../middleware/auth.js";

const router = Router();

const MT_TO_LITERS = 1810;

const parseNumber = (value) => {
  if (value === "" || value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const roundTo = (value, digits = 2) => {
  const factor = 10 ** digits;
  return Math.round((Number(value) + Number.EPSILON) * factor) / factor;
};

const buildSourceComparisonFromCollections = ({ sourceDocs, deliveryPlans }) => {
  const usage = new Map();
  for (const plan of deliveryPlans || []) {
    const sourceId = String(plan?.source_id || "").trim();
    if (!sourceId) continue;
    const entry = usage.get(sourceId) || {
      runs: 0,
      totalLt: 0,
      totalCost: 0,
      totalPurchase: 0
    };
    entry.runs += 1;
    entry.totalLt += Number(plan?.total_load_lt ?? plan?.total_lt ?? 0) || 0;
    entry.totalCost += Number(plan?.grand_total ?? 0) || 0;
    entry.totalPurchase += Number(plan?.tot_purchase ?? 0) || 0;
    usage.set(sourceId, entry);
  }

  const rows = (sourceDocs || [])
    .map((item) => ({
      source_id: String(item?.source_id || "").trim(),
      source_name: String(item?.source_name || "").trim(),
      price_per_mt: Number(item?.price_per_mt_ex_terminal ?? 0) || 0
    }))
    .filter((item) => item.source_id)
    .sort((a, b) => a.price_per_mt - b.price_per_mt);

  if (!rows.length) {
    return [...usage.entries()]
      .map(([sourceId, row]) => {
        const totalMt = row.totalLt / MT_TO_LITERS;
        return {
          source_id: sourceId,
          source_name: sourceId,
          rank: null,
          price_per_mt: null,
          vs_cheapest_per_mt: null,
          recommendation: "",
          runs: row.runs,
          total_lt: roundTo(row.totalLt, 0),
          total_mt: roundTo(totalMt, 3),
          avg_cost_per_mt: totalMt ? roundTo(row.totalCost / totalMt, 2) : 0,
          total_purchase: roundTo(row.totalPurchase, 2)
        };
      })
      .sort((a, b) => a.avg_cost_per_mt - b.avg_cost_per_mt);
  }

  const cheapest = rows[0]?.price_per_mt || 0;
  return rows.map((row, index) => {
    const rowUsage = usage.get(row.source_id) || {
      runs: 0,
      totalLt: 0,
      totalCost: 0,
      totalPurchase: 0
    };
    const totalMt = rowUsage.totalLt / MT_TO_LITERS;
    const vsCheapest = row.price_per_mt - cheapest;
    const recommendation =
      index === 0
        ? "Cheapest terminal - preferred"
        : index <= 2
          ? "Competitive option"
          : `Costlier by Rs ${Math.round(vsCheapest).toLocaleString("en-IN")}/MT`;

    return {
      source_id: row.source_id,
      source_name: row.source_name || row.source_id,
      rank: index + 1,
      price_per_mt: roundTo(row.price_per_mt, 2),
      vs_cheapest_per_mt: roundTo(vsCheapest, 2),
      recommendation,
      runs: rowUsage.runs,
      total_lt: roundTo(rowUsage.totalLt, 0),
      total_mt: roundTo(totalMt, 3),
      avg_cost_per_mt: totalMt ? roundTo(rowUsage.totalCost / totalMt, 2) : 0,
      total_purchase: roundTo(rowUsage.totalPurchase, 2)
    };
  });
};

const normalizeStationStock = ({ capacity, deadStock, usable }) => {
  const errors = [];

  if (capacity === null) {
    errors.push("capacity_in_lt");
  }

  if (deadStock === null && usable === null) {
    errors.push("dead_stock_in_lt");
  }

  let nextDead = deadStock;
  let nextUsable = usable;

  if (capacity !== null) {
    if (deadStock !== null && usable === null) {
      nextUsable = capacity - deadStock;
    } else if (usable !== null && deadStock === null) {
      nextDead = capacity - usable;
    } else if (deadStock !== null && usable !== null) {
      nextUsable = capacity - deadStock;
    }
  }

  if (nextDead === null) {
    errors.push("dead_stock_in_lt");
  }
  if (nextUsable === null) {
    errors.push("usable_lt");
  }

  let sufficientFuel = "NO";
  if (capacity !== null && nextDead !== null) {
    sufficientFuel = nextDead >= 0.6 * capacity ? "NO" : "YES";
  }

  return {
    deadStock: nextDead,
    usable: nextUsable,
    sufficientFuel,
    errors
  };
};

const parseCoordinates = (coordinates) => {
  if (!coordinates || typeof coordinates !== "object") {
    return null;
  }
  const lat = parseNumber(coordinates.lat);
  const lng = parseNumber(coordinates.lng);
  if (lat === null || lng === null) {
    return null;
  }
  return { lat, lng };
};

const buildSourcePayload = (body) => {
  const sourceId = String(body.source_id || "").trim();
  const sourceName = String(body.source_name || "").trim();
  const coordinates = parseCoordinates(body.coordinates);
  const pricePerMt = parseNumber(body.price_per_mt_ex_terminal);
  const errors = [];

  if (!sourceId) errors.push("source_id");
  if (!sourceName) errors.push("source_name");
  if (!coordinates) errors.push("coordinates");
  if (pricePerMt === null) errors.push("price_per_mt_ex_terminal");

  return {
    payload: {
      source_id: sourceId,
      source_name: sourceName,
      coordinates,
      price_per_mt_ex_terminal: pricePerMt
    },
    errors
  };
};

const buildStationPayload = (body) => {
  const station = String(body.station || "").trim();
  const coordinates = parseCoordinates(body.coordinates);
  const capacity = parseNumber(body.capacity_in_lt);
  const deadStock = parseNumber(body.dead_stock_in_lt);
  const usable = parseNumber(body.usable_lt);
  const errors = [];

  if (!station) errors.push("station");
  if (!coordinates) errors.push("coordinates");
  const stock = normalizeStationStock({ capacity, deadStock, usable });
  errors.push(...stock.errors);

  return {
    payload: {
      station,
      coordinates,
      capacity_in_lt: capacity,
      dead_stock_in_lt: stock.deadStock,
      usable_lt: stock.usable,
      sufficient_fuel: stock.sufficientFuel
    },
    errors
  };
};

const escapeRegex = (value) =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const deriveTruckState = ({ station, source, maintenanceStation }) => {
  if (source) return "atSource";
  if (station) return "atStation";
  if (maintenanceStation) return "atMaintenance";
  return "travelling";
};

const findStationByName = async (stationName) => {
  if (!stationName) return null;
  return Station.findOne({
    station: { $regex: `^${escapeRegex(stationName)}$`, $options: "i" }
  })
    .select("station coordinates")
    .lean();
};

const findSource = async ({ sourceName, sourceId }) => {
  if (sourceId) {
    const byId = await Source.findOne({
      source_id: { $regex: `^${escapeRegex(sourceId)}$`, $options: "i" }
    })
      .select("source_id source_name coordinates")
      .lean();
    if (byId) return byId;
  }
  if (!sourceName) return null;
  return Source.findOne({
    source_name: { $regex: `^${escapeRegex(sourceName)}$`, $options: "i" }
  })
    .select("source_id source_name coordinates")
    .lean();
};

const buildTruckPayload = async (body, { existingTruck = null } = {}) => {
  const truckId = String(body.truck_id || "").trim();
  const stationInput = String(body.station || "").trim();
  const sourceInput = String(body.source || body.source_name || "").trim();
  const sourceIdInput = String(body.source_id || "").trim();
  const type = String(body.type || "").trim();
  let lat = parseNumber(body.lat);
  let lon = parseNumber(body.lon);
  const errors = [];

  if (!truckId) errors.push("truck_id");
  if (!type) errors.push("type");
  if (stationInput && (sourceInput || sourceIdInput)) {
    errors.push("station_or_source");
  }

  let stationDoc = null;
  let sourceDoc = null;

  if (stationInput) {
    stationDoc = await findStationByName(stationInput);
    if (!stationDoc) {
      errors.push("station");
    } else if (lat === null || lon === null) {
      lat = parseNumber(stationDoc.coordinates?.lat);
      lon = parseNumber(stationDoc.coordinates?.lng ?? stationDoc.coordinates?.lon);
    }
  }

  if (sourceInput || sourceIdInput) {
    sourceDoc = await findSource({ sourceName: sourceInput, sourceId: sourceIdInput });
    if (!sourceDoc) {
      errors.push("source");
    } else if (lat === null || lon === null) {
      lat = parseNumber(sourceDoc.coordinates?.lat);
      lon = parseNumber(sourceDoc.coordinates?.lng ?? sourceDoc.coordinates?.lon);
    }
  }

  const stationName = stationDoc?.station || "";
  const sourceName = sourceDoc?.source_name || "";
  const sourceId = sourceDoc?.source_id || "";
  const existingMaintenanceStation = String(existingTruck?.maintenance_station || "").trim();
  const maintenanceStation =
    stationName || sourceName ? "" : existingMaintenanceStation;

  const state = deriveTruckState({
    station: stationName,
    source: sourceName,
    maintenanceStation
  });

  if ((state === "atStation" || state === "atSource") && (lat === null || lon === null)) {
    errors.push("coordinates");
  }

  return {
    payload: {
      truck_id: truckId,
      station: state === "atStation" ? stationName : null,
      source: state === "atSource" ? sourceName : null,
      source_id: state === "atSource" ? sourceId || null : null,
      type,
      lat: state === "atStation" || state === "atSource" ? lat : null,
      lon: state === "atStation" || state === "atSource" ? lon : null,
      maintenance_station: state === "atMaintenance" ? maintenanceStation : null,
      state
    },
    errors
  };
};

const handleDuplicateError = (error, res, label) => {
  if (error?.code === 11000) {
    return res.status(409).json({ message: `${label} already exists.` });
  }
  return res.status(500).json({ message: `Failed to save ${label}.`, error: error.message });
};

const runRoutePlanner = (planId) =>
  new Promise((resolve, reject) => {
    const pythonPath = process.env.PYTHON_PATH || "python";
    const directPath = path.resolve(process.cwd(), "pythonLogic/route_plan_db.py");
    const fallbackPath = path.resolve(
      process.cwd(),
      "../pythonLogic/route_plan_db.py"
    );
    const scriptPath = fs.existsSync(directPath) ? directPath : fallbackPath;
    const child = spawn(pythonPath, [scriptPath], {
      env: {
        ...process.env,
        PLAN_ID: planId
      }
    });

    let stderr = "";
    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(stderr || "Route planner failed."));
      }
    });
  });

const fetchPlanBundle = async (planId) => {
  const [delivery, truckPlanning, tentativeCost, unserved, analytics] = await Promise.all([
    Delivery.findOne({ plan_id: planId }).lean(),
    TruckPlanning.findOne({ plan_id: planId }).lean(),
    TentativeCost.findOne({ plan_id: planId }).lean(),
    UnservedStations.findOne({ plan_id: planId }).lean(),
    AnalyticsDashboard.findOne({ plan_id: planId }).lean()
  ]);

  return { delivery, truckPlanning, tentativeCost, unserved, analytics };
};

const normalizeDecision = (value) => {
  const parsed = String(value || "")
    .trim()
    .toUpperCase();
  if (parsed === "ACCEPTED" || parsed === "REJECTED") {
    return parsed;
  }
  return "PENDING";
};

const recomputeUnservedSummary = (doc) => {
  const resolutions = Array.isArray(doc?.resolutions) ? doc.resolutions : [];
  const pending = resolutions.filter(
    (item) => normalizeDecision(item?.decision) === "PENDING"
  );
  const accepted = resolutions.filter(
    (item) => normalizeDecision(item?.decision) === "ACCEPTED"
  ).length;
  const rejected = resolutions.filter(
    (item) => normalizeDecision(item?.decision) === "REJECTED"
  ).length;

  const summary = {
    total: pending.length,
    today: 0,
    tomorrow: 0,
    manual_review: 0,
    swap_suggestions: 0,
    pending: pending.length,
    accepted,
    rejected
  };

  pending.forEach((item) => {
    const when = String(item?.when || "").trim().toUpperCase();
    if (when === "TODAY") {
      summary.today += 1;
    } else if (when === "TOMORROW") {
      summary.tomorrow += 1;
    } else {
      summary.manual_review += 1;
    }
    if (item?.swap_candidate) {
      summary.swap_suggestions += 1;
    }
  });

  return summary;
};

router.use(authenticate);
router.use(requireRole(["admin"]));

router.get("/sources", async (_req, res) => {
  try {
    const sources = await Source.find().sort({ source_id: 1 });
    res.json(sources);
  } catch (error) {
    res.status(500).json({ message: "Failed to fetch sources.", error: error.message });
  }
});

router.post("/sources", async (req, res) => {
  const { payload, errors } = buildSourcePayload(req.body);
  if (errors.length) {
    return res
      .status(400)
      .json({ message: "Missing or invalid fields.", fields: errors });
  }

  try {
    const created = await Source.create(payload);
    return res.status(201).json(created);
  } catch (error) {
    return handleDuplicateError(error, res, "source");
  }
});

router.patch("/sources/:id", async (req, res) => {
  const { payload, errors } = buildSourcePayload(req.body);
  if (errors.length) {
    return res
      .status(400)
      .json({ message: "Missing or invalid fields.", fields: errors });
  }

  try {
    const updated = await Source.findByIdAndUpdate(req.params.id, payload, {
      new: true,
      runValidators: true
    });
    if (!updated) {
      return res.status(404).json({ message: "Source not found." });
    }
    return res.json(updated);
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid source id." });
    }
    return handleDuplicateError(error, res, "source");
  }
});

router.delete("/sources/:id", async (req, res) => {
  try {
    const deleted = await Source.findByIdAndDelete(req.params.id);
    if (!deleted) {
      return res.status(404).json({ message: "Source not found." });
    }
    return res.json({ message: "Source deleted." });
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid source id." });
    }
    return res.status(500).json({ message: "Failed to delete source.", error: error.message });
  }
});

router.get("/stations", async (_req, res) => {
  try {
    const stations = await Station.find().sort({ station: 1 });
    res.json(stations);
  } catch (error) {
    res.status(500).json({ message: "Failed to fetch stations.", error: error.message });
  }
});

router.post("/stations", async (req, res) => {
  const { payload, errors } = buildStationPayload(req.body);
  if (errors.length) {
    return res
      .status(400)
      .json({ message: "Missing or invalid fields.", fields: errors });
  }

  try {
    const created = await Station.create(payload);
    return res.status(201).json(created);
  } catch (error) {
    return handleDuplicateError(error, res, "station");
  }
});

router.patch("/stations/:id", async (req, res) => {
  const { payload, errors } = buildStationPayload(req.body);
  if (errors.length) {
    return res
      .status(400)
      .json({ message: "Missing or invalid fields.", fields: errors });
  }

  try {
    const existing = await Station.findById(req.params.id);
    if (!existing) {
      return res.status(404).json({ message: "Station not found." });
    }

    const beforeStation = existing.toObject();
    existing.set(payload);
    await existing.save();

    await recordStationAttributeChange({
      beforeStation,
      afterStation: existing.toObject(),
      actor: req.user,
      source: "admin_dashboard"
    });

    return res.json(existing);
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid station id." });
    }
    return handleDuplicateError(error, res, "station");
  }
});

router.get("/reports/station-changes", async (req, res) => {
  const searchBy = String(req.query.search_by || "").trim().toLowerCase();
  const searchTerm = String(req.query.search_term || "").trim();
  const station = String(
    req.query.station || (searchBy === "station" ? searchTerm : "")
  ).trim();
  const region = String(
    req.query.region || (searchBy === "region" ? searchTerm : "")
  ).trim();
  const actorRoleRaw = String(req.query.actor_role || "station_manager")
    .trim()
    .toLowerCase();

  if (!["station_manager", "admin", "all"].includes(actorRoleRaw)) {
    return res.status(400).json({
      message: "actor_role must be station_manager, admin, or all."
    });
  }

  try {
    const records = await fetchStationChangeReport({
      station,
      region,
      actorRole: actorRoleRaw === "all" ? null : actorRoleRaw,
      preset: req.query.preset,
      from: req.query.from,
      to: req.query.to,
      limit: 2000
    });
    return res.json({
      count: records.length,
      records
    });
  } catch (error) {
    if (String(error?.message || "").toLowerCase().includes("date")) {
      return res.status(400).json({ message: error.message });
    }
    return res.status(500).json({
      message: "Failed to fetch station change report.",
      error: error.message
    });
  }
});

router.delete("/stations/:id", async (req, res) => {
  try {
    const deleted = await Station.findByIdAndDelete(req.params.id);
    if (!deleted) {
      return res.status(404).json({ message: "Station not found." });
    }
    return res.json({ message: "Station deleted." });
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid station id." });
    }
    return res
      .status(500)
      .json({ message: "Failed to delete station.", error: error.message });
  }
});

router.get("/trucks", async (_req, res) => {
  try {
    const trucks = await Truck.find().sort({ truck_id: 1 });
    res.json(trucks);
  } catch (error) {
    res.status(500).json({ message: "Failed to fetch trucks.", error: error.message });
  }
});

router.post("/trucks", async (req, res) => {
  const { payload, errors } = await buildTruckPayload(req.body);
  if (errors.length) {
    return res
      .status(400)
      .json({ message: "Missing or invalid fields.", fields: errors });
  }

  try {
    const created = await Truck.create(payload);
    return res.status(201).json(created);
  } catch (error) {
    return handleDuplicateError(error, res, "truck");
  }
});

router.patch("/trucks/:id", async (req, res) => {
  let existing = null;
  try {
    existing = await Truck.findById(req.params.id);
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid truck id." });
    }
    return res
      .status(500)
      .json({ message: "Failed to load truck.", error: error.message });
  }

  if (!existing) {
    return res.status(404).json({ message: "Truck not found." });
  }

  const { payload, errors } = await buildTruckPayload(req.body, {
    existingTruck: existing
  });
  if (errors.length) {
    return res
      .status(400)
      .json({ message: "Missing or invalid fields.", fields: errors });
  }

  try {
    existing.set(payload);
    await existing.save();
    return res.json(existing);
  } catch (error) {
    return handleDuplicateError(error, res, "truck");
  }
});

router.delete("/trucks/:id", async (req, res) => {
  try {
    const deleted = await Truck.findByIdAndDelete(req.params.id);
    if (!deleted) {
      return res.status(404).json({ message: "Truck not found." });
    }
    return res.json({ message: "Truck deleted." });
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid truck id." });
    }
    return res
      .status(500)
      .json({ message: "Failed to delete truck.", error: error.message });
  }
});

router.post("/route-plan", async (_req, res) => {
  const planId = randomUUID();

  try {
    await runRoutePlanner(planId);
    const bundle = await fetchPlanBundle(planId);
    return res.json({ plan_id: planId, ...bundle });
  } catch (error) {
    return res.status(500).json({
      message: "Route plan generation failed.",
      error: error.message
    });
  }
});

router.get("/route-plan/latest", async (_req, res) => {
  try {
    const latest = await Delivery.findOne().sort({ created_at: -1 }).lean();
    if (!latest) {
      return res.status(404).json({ message: "No route plan found." });
    }
    const bundle = await fetchPlanBundle(latest.plan_id);
    return res.json({ plan_id: latest.plan_id, ...bundle });
  } catch (error) {
    return res.status(500).json({ message: "Failed to load route plan.", error: error.message });
  }
});

router.get("/route-plan/:planId", async (req, res) => {
  try {
    const bundle = await fetchPlanBundle(req.params.planId);
    if (!bundle.delivery) {
      return res.status(404).json({ message: "Route plan not found." });
    }
    return res.json({ plan_id: req.params.planId, ...bundle });
  } catch (error) {
    return res.status(500).json({ message: "Failed to load route plan.", error: error.message });
  }
});

router.get("/analytics/latest", async (_req, res) => {
  try {
    const latest = await AnalyticsDashboard.findOne().sort({ created_at: -1 }).lean();
    if (!latest) {
      return res.status(404).json({ message: "No analytics snapshot found." });
    }

    let deliveryDoc = null;
    if (latest.plan_id) {
      deliveryDoc = await Delivery.findOne({ plan_id: latest.plan_id }).lean();
    }
    if (!deliveryDoc) {
      deliveryDoc = await Delivery.findOne().sort({ created_at: -1 }).lean();
    }

    const sourceDocs = await Source.find()
      .select("source_id source_name price_per_mt_ex_terminal")
      .lean();

    const sourceComparison = buildSourceComparisonFromCollections({
      sourceDocs,
      deliveryPlans: deliveryDoc?.delivery_plans || []
    });

    return res.json({
      ...latest,
      source_comparison: sourceComparison
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to load analytics dashboard.",
      error: error.message
    });
  }
});

router.post("/route-plan/:planId/unserved/:resolutionId/decision", async (req, res) => {
  const { planId, resolutionId } = req.params;
  const decisionRaw = String(req.body?.decision || "").trim().toUpperCase();
  if (!["ACCEPT", "REJECT"].includes(decisionRaw)) {
    return res
      .status(400)
      .json({ message: "Decision must be ACCEPT or REJECT." });
  }

  try {
    const unservedDoc = await UnservedStations.findOne({ plan_id: planId });
    if (!unservedDoc) {
      return res.status(404).json({ message: "Unserved plan not found." });
    }

    const resolution = unservedDoc.resolutions?.find(
      (item) => item.resolution_id === resolutionId
    );
    if (!resolution) {
      return res.status(404).json({ message: "Resolution not found." });
    }

    if (normalizeDecision(resolution.decision) !== "PENDING") {
      return res.status(409).json({ message: "This suggestion was already decided." });
    }

    resolution.decision = decisionRaw === "ACCEPT" ? "ACCEPTED" : "REJECTED";
    resolution.decided_at = new Date();

    const matchedUnserved = unservedDoc.unserved?.find((item) => {
      if (resolution.unserved_id && item?.unserved_id) {
        return item.unserved_id === resolution.unserved_id;
      }
      return (
        String(item?.station || "").trim() === String(resolution.station || "").trim() &&
        Number(item?.needed_lt || 0) === Number(resolution.needed_lt || 0)
      );
    });

    if (decisionRaw === "ACCEPT") {
      const swap = resolution.swap_detail;
      if (!swap?.truck_id || !swap?.drop_station || !resolution.station) {
        return res
          .status(400)
          .json({ message: "No swap details available for this suggestion." });
      }

      const deliveryDoc = await Delivery.findOne({ plan_id: planId });
      if (!deliveryDoc) {
        return res.status(404).json({ message: "Delivery plan not found." });
      }

      const plan = deliveryDoc.delivery_plans?.find(
        (item) => item.truck_id === swap.truck_id
      );
      if (!plan) {
        return res.status(400).json({ message: "Truck plan not found." });
      }

      const dropIndex = (plan.stops || []).findIndex(
        (stop) => stop.station === swap.drop_station
      );
      if (dropIndex === -1) {
        return res.status(400).json({ message: "Drop station not found in plan." });
      }

      const droppedStop = plan.stops[dropIndex];
      const newStop = {
        station: resolution.station,
        needed_lt: resolution.needed_lt,
        needed_mt: resolution.needed_mt,
        station_lat: resolution.station_lat,
        station_lon: resolution.station_lon
      };

      plan.stops.splice(dropIndex, 1, newStop);
      const totalLt = plan.stops.reduce(
        (sum, stop) => sum + (Number(stop.needed_lt) || 0),
        0
      );
      plan.total_lt = Math.round(totalLt);
      plan.total_mt = Number((plan.total_lt / MT_TO_LITERS).toFixed(3));

      const finalStop = plan.stops[plan.stops.length - 1];
      if (finalStop?.station) {
        plan.final_park = finalStop.station;
        plan.final_lat = finalStop.station_lat ?? plan.final_lat;
        plan.final_lon = finalStop.station_lon ?? plan.final_lon;
      }

      if (Array.isArray(plan.journey_steps)) {
        const step = plan.journey_steps.find(
          (item) => item.step_type === "DELIVER" && item.location === swap.drop_station
        );
        if (step) {
          step.location = resolution.station;
          step.qty_lt = resolution.needed_lt;
          step.qty_mt = resolution.needed_mt;
          step.note = "Updated by swap suggestion";
        }

        const finalStep = plan.journey_steps.find(
          (item) => item.step_type === "FINAL_PARK"
        );
        if (finalStep && finalStop?.station) {
          finalStep.location = finalStop.station;
          finalStep.note = "Updated final park after swap suggestion";
        }
      }

      plan.swap_applied = {
        resolution_id: resolutionId,
        dropped_station: swap.drop_station,
        added_station: resolution.station,
        dropped_station_needed_lt: droppedStop?.needed_lt ?? null,
        decided_at: new Date()
      };

      const fleetRow = deliveryDoc.fleet_status?.find(
        (item) => item.truck_id === plan.truck_id
      );
      if (fleetRow && finalStop?.station) {
        fleetRow.final_park = finalStop.station;
        deliveryDoc.markModified("fleet_status");
      }

      deliveryDoc.markModified("delivery_plans");
      await deliveryDoc.save();

      const truckPlanningDoc = await TruckPlanning.findOne({ plan_id: planId });
      if (truckPlanningDoc) {
        const truck = truckPlanningDoc.truck_positions?.find(
          (item) => item.truck_id === plan.truck_id
        );
        if (truck && finalStop?.station) {
          truck.station = finalStop.station;
          truck.lat = finalStop.station_lat ?? truck.lat;
          truck.lon = finalStop.station_lon ?? truck.lon;
          truckPlanningDoc.markModified("truck_positions");
          await truckPlanningDoc.save();
        }
      }

      const tentativeDoc = await TentativeCost.findOne({ plan_id: planId });
      if (tentativeDoc) {
        const row = tentativeDoc.cost_summary?.find(
          (item) => item.truck_id === plan.truck_id && item.source_id === plan.source_id
        );
        if (row) {
          row.stations = plan.stops.map((stop) => stop.station);
          tentativeDoc.markModified("cost_summary");
          await tentativeDoc.save();
        }
      }

      resolution.applied_swap = {
        truck_id: swap.truck_id,
        dropped_station: swap.drop_station,
        added_station: resolution.station,
        applied_at: new Date()
      };
      resolution.summary =
        `Swapped '${swap.drop_station}' out from truck ${swap.truck_id} and ` +
        `added '${resolution.station}' into the same sequence.`;

      if (matchedUnserved) {
        matchedUnserved.status = "RESOLVED_BY_SWAP";
        matchedUnserved.resolution_id = resolutionId;
        matchedUnserved.decided_at = new Date();
      }
    } else if (matchedUnserved) {
      matchedUnserved.status = "UNSERVED";
      matchedUnserved.resolution_id = resolutionId;
      matchedUnserved.decided_at = new Date();
    }

    unservedDoc.summary = recomputeUnservedSummary(unservedDoc);
    unservedDoc.markModified("summary");
    unservedDoc.markModified("unserved");
    unservedDoc.markModified("resolutions");
    await unservedDoc.save();

    const bundle = await fetchPlanBundle(planId);
    return res.json({ plan_id: planId, ...bundle });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to update swap decision.",
      error: error.message
    });
  }
});

router.post("/import", async (_req, res) => {
  try {
    const result = await runImport();
    res.json({ message: "Import completed.", result });
  } catch (error) {
    res.status(500).json({ message: "Import failed.", error: error.message });
  }
});

export default router;
