import mongoose from "mongoose";

const truckSchema = new mongoose.Schema(
  {
    truck_id: { type: String, required: true, unique: true, trim: true },
    station: { type: String, required: true, trim: true },
    lat: { type: Number, required: true },
    lon: { type: Number, required: true },
    type: { type: String, required: true, trim: true }
  },
  {
    timestamps: true
  }
);

export const Truck = mongoose.model("Truck", truckSchema, "truck");
