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

const resolveTruckState = (truck) => {
  const state = String(truck?.state || "").trim();
  if (state === "atStation" || state === "maintenance" || state === "travelling") {
    return state;
  }
  return truck?.station ? "atStation" : "travelling";
};

const resolveMaintenanceStation = (truck) =>
  String(truck?.maintenance_station || "").trim();

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
      .filter(
        (truck) => truck.station !== stationName || resolveTruckState(truck) !== "atStation"
      )
      .map((truck) => ({
        updateOne: {
          filter: { _id: truck._id },
          update: { $set: { station: stationName, state: "atStation" } }
        }
      }));

    if (updates.length) {
      await Truck.bulkWrite(updates);
      matching.forEach((truck) => {
        truck.station = stationName;
        truck.state = "atStation";
      });
    }

    const availableTrucks = allTrucks.filter((truck) => {
      if (resolveTruckState(truck) === "atStation") {
        return false;
      }
      if (resolveTruckState(truck) !== "maintenance") {
        return true;
      }
      const maintStation = resolveMaintenanceStation(truck);
      return !maintStation || normalizeStationKey(maintStation) === stationKey;
    });

    return res.json({ station, trucks: matching, available_trucks: availableTrucks });
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
    const stationKey = normalizeStationKey(stationName);

    const existing = await Truck.findOne({ truck_id: truckId });
    if (!existing) {
      return res.status(404).json({ message: "Truck not found." });
    }

    const currentState = resolveTruckState(existing);
    const maintenanceStation = resolveMaintenanceStation(existing);
    if (
      currentState === "maintenance" &&
      maintenanceStation &&
      normalizeStationKey(maintenanceStation) !== stationKey
    ) {
      return res
        .status(403)
        .json({ message: "Truck is in maintenance for another station." });
    }
    if (
      currentState === "atStation" &&
      normalizeStationKey(existing.station) !== stationKey
    ) {
      return res
        .status(409)
        .json({ message: "Truck is already parked at another station." });
    }

    existing.station = stationName;
    existing.lat = lat;
    existing.lon = lon;
    existing.state = "atStation";
    existing.maintenance_station = null;
    await existing.save();
    return res.json(existing);
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
    const stationKey = normalizeStationKey(stationName);
    if (
      resolveTruckState(truck) !== "atStation" ||
      normalizeStationKey(truck.station) !== stationKey
    ) {
      return res.status(403).json({ message: "Forbidden." });
    }
    truck.station = null;
    truck.state = "travelling";
    truck.maintenance_station = null;
    await truck.save();
    return res.json(truck);
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid truck id." });
    }
    return res
      .status(500)
      .json({ message: "Unable to remove truck.", error: error.message });
  }
});

router.patch("/trucks/:id/maintenance", async (req, res) => {
  const stationName = resolveStationName(req);
  if (!stationName) {
    return res.status(400).json({ message: "Station assignment missing." });
  }

  try {
    const truck = await Truck.findById(req.params.id);
    if (!truck) {
      return res.status(404).json({ message: "Truck not found." });
    }
    const stationKey = normalizeStationKey(stationName);
    if (
      resolveTruckState(truck) !== "atStation" ||
      normalizeStationKey(truck.station) !== stationKey
    ) {
      return res.status(403).json({ message: "Forbidden." });
    }
    truck.station = null;
    truck.state = "maintenance";
    truck.maintenance_station = stationName;
    await truck.save();
    return res.json(truck);
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid truck id." });
    }
    return res
      .status(500)
      .json({ message: "Unable to update truck state.", error: error.message });
  }
});

export default router;
