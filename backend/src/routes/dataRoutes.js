import { Router } from "express";

import { Source } from "../models/source.js";
import { Station } from "../models/station.js";
import { runImport } from "../services/importData.js";

const router = Router();

router.get("/sources", async (_req, res) => {
  try {
    const sources = await Source.find().sort({ source_id: 1 });
    res.json(sources);
  } catch (error) {
    res.status(500).json({ message: "Failed to fetch sources.", error: error.message });
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

router.post("/import", async (_req, res) => {
  try {
    const result = await runImport();
    res.json({ message: "Import completed.", result });
  } catch (error) {
    res.status(500).json({ message: "Import failed.", error: error.message });
  }
});

export default router;
