import mongoose from "mongoose";

const truckSchema = new mongoose.Schema(
  {
    truck_id: { type: String, required: true, unique: true, trim: true },
    station: { type: String, trim: true, default: null },
    lat: { type: Number, default: null },
    lon: { type: Number, default: null },
    type: { type: String, required: true, trim: true },
    maintenance_station: { type: String, trim: true, default: null },
    state: {
      type: String,
      enum: ["atStation", "maintenance", "travelling"],
      default: "travelling"
    }
  },
  {
    timestamps: true
  }
);

export const Truck = mongoose.model("Truck", truckSchema, "truck");
