import { Router } from "express";

import { authenticate, requireRole } from "../middleware/auth.js";
import { Station } from "../models/station.js";
import { Truck } from "../models/truck.js";

const router = Router();

const parseNumber = (value) => {
  if (value === "" || value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const resolveStationName = (req) => String(req.user?.station || "").trim();

const normalizeStationKey = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const computeStockUpdate = ({ capacity, deadStock, usable }) => {
  let nextDead = deadStock;
  let nextUsable = usable;

  if (deadStock !== null && usable === null) {
    nextUsable = capacity - deadStock;
  } else if (usable !== null && deadStock === null) {
    nextDead = capacity - usable;
  } else if (deadStock !== null && usable !== null) {
    nextUsable = capacity - deadStock;
  }

  return { deadStock: nextDead, usable: nextUsable };
};

router.use(authenticate);
router.use(requireRole(["station_manager"]));

router.get("/overview", async (req, res) => {
  const stationName = resolveStationName(req);
  if (!stationName) {
    return res.status(400).json({ message: "Station assignment missing." });
  }

  try {
    const station = await Station.findOne({ station: stationName });
    if (!station) {
      return res.status(404).json({ message: "Station not found." });
    }
    const stationKey = normalizeStationKey(stationName);
    const allTrucks = await Truck.find().sort({ truck_id: 1 });
    const matching = allTrucks.filter(
      (truck) => normalizeStationKey(truck.station) === stationKey
    );

    const updates = matching
      .filter((truck) => truck.station !== stationName)
      .map((truck) => ({
        updateOne: {
          filter: { _id: truck._id },
          update: { $set: { station: stationName } }
        }
      }));

    if (updates.length) {
      await Truck.bulkWrite(updates);
      matching.forEach((truck) => {
        truck.station = stationName;
      });
    }

    return res.json({ station, trucks: matching });
  } catch (error) {
    return res
      .status(500)
      .json({ message: "Unable to load station overview.", error: error.message });
  }
});

router.patch("/station", async (req, res) => {
  const stationName = resolveStationName(req);
  if (!stationName) {
    return res.status(400).json({ message: "Station assignment missing." });
  }

  try {
    const station = await Station.findOne({ station: stationName });
    if (!station) {
      return res.status(404).json({ message: "Station not found." });
    }

    const capacity = station.capacity_in_lt;
    const deadStock = parseNumber(req.body?.dead_stock_in_lt);
    const usable = parseNumber(req.body?.usable_lt);

    if (deadStock === null && usable === null) {
      return res
        .status(400)
        .json({ message: "Dead stock or usable liters are required." });
    }

    const computed = computeStockUpdate({ capacity, deadStock, usable });
    if (computed.deadStock === null || computed.usable === null) {
      return res
        .status(400)
        .json({ message: "Unable to compute stock values." });
    }

    const sufficientFuel =
      computed.deadStock >= 0.6 * capacity ? "NO" : "YES";

    station.dead_stock_in_lt = computed.deadStock;
    station.usable_lt = computed.usable;
    station.sufficient_fuel = sufficientFuel;

    await station.save();
    return res.json(station);
  } catch (error) {
    return res
      .status(500)
      .json({ message: "Unable to update station stock.", error: error.message });
  }
});

router.post("/trucks", async (req, res) => {
  const stationName = resolveStationName(req);
  if (!stationName) {
    return res.status(400).json({ message: "Station assignment missing." });
  }

  const truckId = String(req.body?.truck_id || "").trim();
  const type = String(req.body?.type || "").trim();

  if (!truckId) {
    return res.status(400).json({ message: "Truck id is required." });
  }

  try {
    const station = await Station.findOne({ station: stationName });
    if (!station) {
      return res.status(404).json({ message: "Station not found." });
    }

    const lat = station.coordinates?.lat;
    const lon = station.coordinates?.lng;

    const existing = await Truck.findOne({ truck_id: truckId });
    if (existing) {
      existing.station = stationName;
      existing.lat = lat;
      existing.lon = lon;
      if (type) {
        existing.type = type;
      }
      await existing.save();
      return res.json(existing);
    }

    if (!type) {
      return res.status(400).json({ message: "Truck type is required." });
    }

    const created = await Truck.create({
      truck_id: truckId,
      station: stationName,
      lat,
      lon,
      type
    });

    return res.status(201).json(created);
  } catch (error) {
    if (error?.code === 11000) {
      return res.status(409).json({ message: "Truck already exists." });
    }
    return res
      .status(500)
      .json({ message: "Unable to save truck.", error: error.message });
  }
});

router.delete("/trucks/:id", async (req, res) => {
  const stationName = resolveStationName(req);
  if (!stationName) {
    return res.status(400).json({ message: "Station assignment missing." });
  }

  try {
    const truck = await Truck.findById(req.params.id);
    if (!truck) {
      return res.status(404).json({ message: "Truck not found." });
    }
    if (truck.station !== stationName) {
      return res.status(403).json({ message: "Forbidden." });
    }
    await Truck.findByIdAndDelete(req.params.id);
    return res.json({ message: "Truck removed." });
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid truck id." });
    }
    return res
      .status(500)
      .json({ message: "Unable to remove truck.", error: error.message });
  }
});

export default router;
