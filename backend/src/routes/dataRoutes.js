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
import { runImport } from "../services/importData.js";
import { authenticate, requireRole } from "../middleware/auth.js";

const router = Router();

const parseNumber = (value) => {
  if (value === "" || value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
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

const buildTruckPayload = (body) => {
  const truckId = String(body.truck_id || "").trim();
  const station = String(body.station || "").trim();
  const type = String(body.type || "").trim();
  const lat = parseNumber(body.lat);
  const lon = parseNumber(body.lon);
  const errors = [];

  if (!truckId) errors.push("truck_id");
  if (!station) errors.push("station");
  if (!type) errors.push("type");
  if (lat === null || lon === null) errors.push("coordinates");

  return {
    payload: {
      truck_id: truckId,
      station,
      type,
      lat,
      lon
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
  const [delivery, truckPlanning, tentativeCost] = await Promise.all([
    Delivery.findOne({ plan_id: planId }).lean(),
    TruckPlanning.findOne({ plan_id: planId }).lean(),
    TentativeCost.findOne({ plan_id: planId }).lean()
  ]);

  return { delivery, truckPlanning, tentativeCost };
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
    const updated = await Station.findByIdAndUpdate(req.params.id, payload, {
      new: true,
      runValidators: true
    });
    if (!updated) {
      return res.status(404).json({ message: "Station not found." });
    }
    return res.json(updated);
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid station id." });
    }
    return handleDuplicateError(error, res, "station");
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
  const { payload, errors } = buildTruckPayload(req.body);
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
  const { payload, errors } = buildTruckPayload(req.body);
  if (errors.length) {
    return res
      .status(400)
      .json({ message: "Missing or invalid fields.", fields: errors });
  }

  try {
    const updated = await Truck.findByIdAndUpdate(req.params.id, payload, {
      new: true,
      runValidators: true
    });
    if (!updated) {
      return res.status(404).json({ message: "Truck not found." });
    }
    return res.json(updated);
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid truck id." });
    }
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

router.post("/import", async (_req, res) => {
  try {
    const result = await runImport();
    res.json({ message: "Import completed.", result });
  } catch (error) {
    res.status(500).json({ message: "Import failed.", error: error.message });
  }
});

export default router;
