import mongoose from "mongoose";

const { Schema } = mongoose;

const unservedStationsSchema = new Schema(
  {
    plan_id: { type: String, required: true, index: true },
    created_at: { type: Date, default: Date.now },
    unserved: { type: [Schema.Types.Mixed], default: [] },
    resolutions: { type: [Schema.Types.Mixed], default: [] },
    summary: { type: Schema.Types.Mixed, default: {} },
    meta: { type: Schema.Types.Mixed, default: {} }
  },
  {
    timestamps: true
  }
);

export const UnservedStations = mongoose.model(
  "UnservedStations",
  unservedStationsSchema,
  "unservedStations"
);
