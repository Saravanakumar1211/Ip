import mongoose from "mongoose";

const sourceSchema = new mongoose.Schema(
  {
    source_id: { type: String, required: true, unique: true, trim: true },
    source_name: { type: String, required: true, trim: true },
    coordinates: {
      lat: { type: Number, required: true },
      lng: { type: Number, required: true }
    },
    price_per_mt_ex_terminal: { type: Number, required: true }
  },
  {
    timestamps: true
  }
);

export const Source = mongoose.model("Source", sourceSchema, "source");
