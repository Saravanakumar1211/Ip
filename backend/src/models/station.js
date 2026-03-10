import mongoose from "mongoose";

const stationSchema = new mongoose.Schema(
  {
    station: { type: String, required: true, unique: true, trim: true },
    coordinates: {
      lat: { type: Number, required: true },
      lng: { type: Number, required: true }
    },
    capacity_in_lt: { type: Number, required: true },
    dead_stock_in_lt: { type: Number, required: true },
    usable_lt: { type: Number, required: true }
  },
  {
    timestamps: true
  }
);

export const Station = mongoose.model("Station", stationSchema, "station");
