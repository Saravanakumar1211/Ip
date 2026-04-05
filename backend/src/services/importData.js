import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import xlsx from "xlsx";

import { connectDB } from "../db.js";
import { Source } from "../models/source.js";
import { Station } from "../models/station.js";
import { Truck } from "../models/truck.js";
import { SalesDaily } from "../models/salesDaily.js";
import { SalesMonthly } from "../models/salesMonthly.js";
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
const TRUCKS_FILE = fs.existsSync(path.join(DATA_DIR, "truck_positions.json"))
  ? path.join(DATA_DIR, "truck_positions.json")
  : path.join(PYTHON_DIR, "truck_positions.json");
const SALES_FILE = path.join(DATA_DIR, "sales_data.xlsx");

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

const normalizeName = (value) =>
  String(value || "")
    .replace(/[–—]/g, "-")
    .replace(/\.\d+$/, "")
    .replace(/\s+/g, " ")
    .trim();

const toNumber = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toMonthKey = (value) => {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  let date = null;
  if (value instanceof Date) {
    date = value;
  } else if (typeof value === "number" && Number.isFinite(value)) {
    const parsedExcelDate = xlsx.SSF.parse_date_code(value);
    if (parsedExcelDate?.y && parsedExcelDate?.m) {
      date = new Date(parsedExcelDate.y, parsedExcelDate.m - 1, parsedExcelDate.d || 1);
    } else {
      date = new Date(value);
    }
  } else {
    const parsed = new Date(value);
    date = Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  if (!date) {
    return null;
  }

  if (Number.isNaN(date.getTime())) {
    return null;
  }
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
};

const toDateValue = (value) => {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const parsedExcelDate = xlsx.SSF.parse_date_code(value);
    if (parsedExcelDate?.y && parsedExcelDate?.m) {
      const date = new Date(parsedExcelDate.y, parsedExcelDate.m - 1, parsedExcelDate.d || 1);
      return Number.isNaN(date.getTime()) ? null : date;
    }
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
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
    const sufficientFuelComputed = deadStock >= 0.6 * capacity ? "NO" : "YES";

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
            sufficient_fuel: sufficientFuelComputed
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

  const sourceDocs = await Source.find(
    {},
    { source_id: 1, source_name: 1, coordinates: 1 }
  ).lean();

  const sourceByName = new Map(
    sourceDocs.map((item) => [normalizeName(item.source_name), item])
  );

  const raw = JSON.parse(fs.readFileSync(TRUCKS_FILE, "utf-8"));
  const operations = [];

  for (const [truckId, data] of Object.entries(raw || {})) {
    const type = String(data?.type || "").trim();
    const sourceCandidateRaw = String(
      data?.source || data?.source_name || data?.station || data?.station_name || ""
    ).trim();
    const sourceCandidate = normalizeName(sourceCandidateRaw);
    const lat = toNumber(data?.lat);
    const lon = toNumber(data?.lon);

    if (!truckId || !type) {
      continue;
    }

    const sourceMatch = sourceByName.get(sourceCandidate) || null;
    const sourceName = sourceMatch?.source_name || sourceCandidateRaw || null;
    const sourceId = sourceMatch?.source_id || null;

    const coordLat =
      lat ??
      sourceMatch?.coordinates?.lat ??
      null;
    const coordLon =
      lon ??
      sourceMatch?.coordinates?.lng ??
      sourceMatch?.coordinates?.lon ??
      null;

    operations.push({
      updateOne: {
        filter: { truck_id: truckId },
        update: {
          $set: {
            truck_id: truckId,
            station: null,
            source: sourceName,
            source_id: sourceId,
            lat: coordLat,
            lon: coordLon,
            type,
            state: "atSource",
            maintenance_station: null
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

const importSalesMonthly = async () => {
  if (!fs.existsSync(SALES_FILE)) {
    return { matched: 0, modified: 0, upserted: 0 };
  }

  const rows = readSheetRows(SALES_FILE);
  if (!rows.length) {
    return { matched: 0, modified: 0, upserted: 0 };
  }

  const aggregate = new Map();
  const dailyOperations = [];

  for (const row of rows) {
    const dateRaw = row.Date ?? row.date ?? row.Month ?? row.month;
    const month = toMonthKey(dateRaw);
    const dateValue = toDateValue(dateRaw);
    if (!month) {
      continue;
    }

    for (const [column, value] of Object.entries(row)) {
      const columnLower = String(column).toLowerCase();
      if (columnLower === "date" || columnLower === "month") {
        continue;
      }
      const sales = toNumber(value);
      if (sales === null) {
        continue;
      }

      const stationName = normalizeName(column);
      if (!stationName) {
        continue;
      }

      if (dateValue) {
        dailyOperations.push({
          updateOne: {
            filter: { date: dateValue, station_name: stationName },
            update: {
              $set: {
                date: dateValue,
                month,
                station_name: stationName,
                sales_lt: Number(sales.toFixed(2)),
                source_file: "sales_data.xlsx"
              }
            },
            upsert: true
          }
        });
      }

      const key = `${month}__${stationName}`;
      const current = aggregate.get(key) || {
        month,
        station_name: stationName,
        total_sales_lt: 0,
        days_recorded: 0
      };
      current.total_sales_lt += sales;
      current.days_recorded += 1;
      aggregate.set(key, current);
    }
  }

  const operations = [];
  for (const item of aggregate.values()) {
    const avgDaily =
      item.days_recorded > 0 ? item.total_sales_lt / item.days_recorded : 0;

    operations.push({
      updateOne: {
        filter: { month: item.month, station_name: item.station_name },
        update: {
          $set: {
            month: item.month,
            station_name: item.station_name,
            total_sales_lt: Number(item.total_sales_lt.toFixed(2)),
            avg_daily_sales_lt: Number(avgDaily.toFixed(2)),
            days_recorded: item.days_recorded,
            source_file: "sales_data.xlsx"
          }
        },
        upsert: true
      }
    });
  }

  if (!operations.length) {
    return { matched: 0, modified: 0, upserted: 0 };
  }

  await SalesDaily.deleteMany({ source_file: "sales_data.xlsx" });
  if (dailyOperations.length) {
    await SalesDaily.bulkWrite(dailyOperations);
  }
  await SalesMonthly.deleteMany({ source_file: "sales_data.xlsx" });
  const result = await SalesMonthly.bulkWrite(operations);
  return {
    matched: result.matchedCount,
    modified: result.modifiedCount,
    upserted: result.upsertedCount,
    daily_rows: dailyOperations.length
  };
};

export const runImport = async () => {
  const sourceResult = await importSources();
  const stationResult = await importStations();
  const truckResult = await importTrucks();
  const salesResult = await importSalesMonthly();
  await seedStationManagers();

  return {
    source: sourceResult,
    station: stationResult,
    truck: truckResult,
    sales_monthly: salesResult
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
