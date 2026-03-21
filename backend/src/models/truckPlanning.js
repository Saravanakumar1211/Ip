import mongoose from "mongoose";

const { Schema } = mongoose;

const truckPlanningSchema = new Schema(
  {
    plan_id: { type: String, required: true, index: true },
    created_at: { type: Date, default: Date.now },
    truck_positions: { type: [Schema.Types.Mixed], default: [] },
    meta: { type: Schema.Types.Mixed, default: {} }
  },
  {
    timestamps: true
  }
);

export const TruckPlanning = mongoose.model(
  "TruckPlanning",
  truckPlanningSchema,
  "truckPlanning"
);
