import mongoose from "mongoose";

const salesMonthlySchema = new mongoose.Schema(
  {
    month: { type: String, required: true, trim: true, index: true },
    station_name: { type: String, required: true, trim: true, index: true },
    total_sales_lt: { type: Number, required: true, default: 0 },
    avg_daily_sales_lt: { type: Number, required: true, default: 0 },
    days_recorded: { type: Number, required: true, default: 0 },
    source_file: { type: String, trim: true, default: "sales_data.xlsx" }
  },
  { timestamps: true }
);

salesMonthlySchema.index({ month: 1, station_name: 1 }, { unique: true });

export const SalesMonthly = mongoose.model(
  "SalesMonthly",
  salesMonthlySchema,
  "salesMonthly"
);
