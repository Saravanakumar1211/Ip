import mongoose from "mongoose";

const salesDailySchema = new mongoose.Schema(
  {
    date: { type: Date, required: true, index: true },
    month: { type: String, required: true, trim: true, index: true },
    station_name: { type: String, required: true, trim: true, index: true },
    sales_lt: { type: Number, required: true, default: 0 },
    source_file: { type: String, trim: true, default: "sales_data.xlsx" }
  },
  { timestamps: true }
);

salesDailySchema.index({ date: 1, station_name: 1 }, { unique: true });

export const SalesDaily = mongoose.model("SalesDaily", salesDailySchema, "salesDaily");

