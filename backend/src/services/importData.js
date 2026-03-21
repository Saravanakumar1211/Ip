import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import xlsx from "xlsx";

import { connectDB } from "../db.js";
import { Source } from "../models/source.js";
import { Station } from "../models/station.js";
import { Truck } from "../models/truck.js";
import { seedStationManagers } from "./seedStationManagers.js";

dotenv.config({ path: path.resolve(process.cwd(), "../.env") });

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_ROOT = path.resolve(__dirname, "../../../");
const DATA_DIR = path.join(PROJECT_ROOT, "data");
const PYTHON_DIR = path.join(PROJECT_ROOT, "pythonLogic");

const resolveDataFile = (filename) => {
  const pythonPath = path.join(PYTHON_DIR, filename);
  if (fs.existsSync(pythonPath)) {
    return pythonPath;
  }
  return path.join(DATA_DIR, filename);
};

const SOURCES_FILE = resolveDataFile("sources.xlsx");
const STATIONS_FILE = resolveDataFile("clean_stationss.xlsx");
const TRUCKS_FILE = resolveDataFile("truck_positions.json");

const parseCoordinates = (value) => {
  if (!value || typeof value !== "string") {
    return null;
  }

  const [latRaw, lngRaw] = value.split(",").map((item) => item.trim());
  const lat = Number(latRaw);
  const lng = Number(lngRaw);

  if (Number.isNaN(lat) || Number.isNaN(lng)) {
    return null;
  }

  return { lat, lng };
};

const readSheetRows = (filePath) => {
  const workbook = xlsx.readFile(filePath);
  const firstSheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[firstSheetName];
  return xlsx.utils.sheet_to_json(sheet, { defval: "" });
};

const normalizeYesNo = (value) => {
  const normalized = String(value || "").trim().toUpperCase();
  if (normalized === "YES" || normalized === "NO") {
    return normalized;
  }
  return "";
};

const importSources = async () => {
  const rows = readSheetRows(SOURCES_FILE);
  const operations = [];

  for (const row of rows) {
    const sourceId = String(row["Source_ID "] || row["Source_ID"] || "").trim();
    const sourceName = String(row["Source_Name"] || "").trim();
    const coordinates = parseCoordinates(String(row["Coordinates"] || "").trim());
    const priceRaw =
      row["Price / MT Ex Terminal"] ?? row["Price in Lt"] ?? row["Price per Lt"];
    const pricePerMt = Number(String(priceRaw || "").trim());

    if (!sourceId || !sourceName || !coordinates || Number.isNaN(pricePerMt)) {
      continue;
    }

    operations.push({
      updateOne: {
        filter: { source_id: sourceId },
        update: {
          $set: {
            source_id: sourceId,
            source_name: sourceName,
            coordinates,
            price_per_mt_ex_terminal: pricePerMt
          },
          $unset: {
            price_in_lt: ""
          }
        },
        upsert: true
      }
    });
  }

  if (!operations.length) {
    return { matched: 0, modified: 0, upserted: 0 };
  }

  const result = await Source.bulkWrite(operations);
  return {
    matched: result.matchedCount,
    modified: result.modifiedCount,
    upserted: result.upsertedCount
  };
};

const importStations = async () => {
  const rows = readSheetRows(STATIONS_FILE);
  const operations = [];

  for (const row of rows) {
    const station = String(row["Stations "] || row["Stations"] || "").trim();
    const coordinates = parseCoordinates(String(row["Coordinates"] || "").trim());
    const capacity = Number(String(row["Capacity in Lt"] || "").trim());
    const deadStock = Number(String(row["Dead stock in Lt"] || "").trim());
    const usable = Number(String(row["Usable Lt"] || "").trim());
    const sufficientFuel = normalizeYesNo(row["Now"] || row["Sufficient Fuel"]);

    if (
      !station ||
      !coordinates ||
      Number.isNaN(capacity) ||
      Number.isNaN(deadStock) ||
      Number.isNaN(usable)
    ) {
      continue;
    }

    operations.push({
      updateOne: {
        filter: { station },
        update: {
          $set: {
            station,
            coordinates,
            capacity_in_lt: capacity,
            dead_stock_in_lt: deadStock,
            usable_lt: usable,
            sufficient_fuel: sufficientFuel || "NO"
          }
        },
        upsert: true
      }
    });
  }

  if (!operations.length) {
    return { matched: 0, modified: 0, upserted: 0 };
  }

  const result = await Station.bulkWrite(operations);
  return {
    matched: result.matchedCount,
    modified: result.modifiedCount,
    upserted: result.upsertedCount
  };
};

const importTrucks = async () => {
  if (!fs.existsSync(TRUCKS_FILE)) {
    return { matched: 0, modified: 0, upserted: 0 };
  }

  const raw = JSON.parse(fs.readFileSync(TRUCKS_FILE, "utf-8"));
  const operations = [];

  for (const [truckId, data] of Object.entries(raw || {})) {
    const station = String(data?.station || "").trim();
    const lat = Number(data?.lat);
    const lon = Number(data?.lon);
    const type = String(data?.type || "").trim();

    if (!truckId || !station || Number.isNaN(lat) || Number.isNaN(lon) || !type) {
      continue;
    }

    operations.push({
      updateOne: {
        filter: { truck_id: truckId },
        update: {
          $set: {
            truck_id: truckId,
            station,
            lat,
            lon,
            type,
            state: "atStation"
          }
        },
        upsert: true
      }
    });
  }

  if (!operations.length) {
    return { matched: 0, modified: 0, upserted: 0 };
  }

  const result = await Truck.bulkWrite(operations);
  return {
    matched: result.matchedCount,
    modified: result.modifiedCount,
    upserted: result.upsertedCount
  };
};

export const runImport = async () => {
  const sourceResult = await importSources();
  const stationResult = await importStations();
  const truckResult = await importTrucks();
  await seedStationManagers();

  return {
    source: sourceResult,
    station: stationResult,
    truck: truckResult
  };
};

if (process.argv[1] === __filename) {
  try {
    await connectDB();
    const result = await runImport();
    console.log("Data import completed:", result);
    process.exit(0);
  } catch (error) {
    console.error("Data import failed:", error.message);
    process.exit(1);
  }
}
