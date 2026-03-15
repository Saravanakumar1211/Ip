import { Router } from "express";

import { Source } from "../models/source.js";
import { Station } from "../models/station.js";
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
  const priceInLt = parseNumber(body.price_in_lt);
  const errors = [];

  if (!sourceId) errors.push("source_id");
  if (!sourceName) errors.push("source_name");
  if (!coordinates) errors.push("coordinates");
  if (priceInLt === null) errors.push("price_in_lt");

  return {
    payload: {
      source_id: sourceId,
      source_name: sourceName,
      coordinates,
      price_in_lt: priceInLt
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
  if (capacity === null) errors.push("capacity_in_lt");
  if (deadStock === null) errors.push("dead_stock_in_lt");
  if (usable === null) errors.push("usable_lt");

  return {
    payload: {
      station,
      coordinates,
      capacity_in_lt: capacity,
      dead_stock_in_lt: deadStock,
      usable_lt: usable
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

router.post("/import", async (_req, res) => {
  try {
    const result = await runImport();
    res.json({ message: "Import completed.", result });
  } catch (error) {
    res.status(500).json({ message: "Import failed.", error: error.message });
  }
});

export default router;
