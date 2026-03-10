import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import xlsx from "xlsx";

import { connectDB } from "../db.js";
import { Source } from "../models/source.js";
import { Station } from "../models/station.js";

dotenv.config({ path: path.resolve(process.cwd(), "../.env") });

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.resolve(__dirname, "../../../data");
const SOURCES_FILE = path.join(DATA_DIR, "sources.xlsx");
const STATIONS_FILE = path.join(DATA_DIR, "clean_stationss.xlsx");

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

const importSources = async () => {
  const rows = readSheetRows(SOURCES_FILE);
  const operations = [];

  for (const row of rows) {
    const sourceId = String(row["Source_ID "] || row["Source_ID"] || "").trim();
    const sourceName = String(row["Source_Name"] || "").trim();
    const coordinates = parseCoordinates(String(row["Coordinates"] || "").trim());
    const priceInLt = Number(String(row["Price in Lt"] || "").trim());

    if (!sourceId || !sourceName || !coordinates || Number.isNaN(priceInLt)) {
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
            price_in_lt: priceInLt
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
            usable_lt: usable
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

export const runImport = async () => {
  const sourceResult = await importSources();
  const stationResult = await importStations();

  return {
    source: sourceResult,
    station: stationResult
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
