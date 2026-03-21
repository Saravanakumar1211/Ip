import mongoose from "mongoose";

const { Schema } = mongoose;

const tentativeCostSchema = new Schema(
  {
    plan_id: { type: String, required: true, index: true },
    created_at: { type: Date, default: Date.now },
    cost_summary: { type: [Schema.Types.Mixed], default: [] },
    totals: { type: Schema.Types.Mixed, default: {} },
    meta: { type: Schema.Types.Mixed, default: {} }
  },
  {
    timestamps: true
  }
);

export const TentativeCost = mongoose.model(
  "TentativeCost",
  tentativeCostSchema,
  "tentativeCost"
);
