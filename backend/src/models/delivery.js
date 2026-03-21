import mongoose from "mongoose";

const { Schema } = mongoose;

const deliverySchema = new Schema(
  {
    plan_id: { type: String, required: true, index: true },
    created_at: { type: Date, default: Date.now },
    delivery_plans: { type: [Schema.Types.Mixed], default: [] },
    fleet_status: { type: [Schema.Types.Mixed], default: [] },
    meta: { type: Schema.Types.Mixed, default: {} }
  },
  {
    timestamps: true
  }
);

export const Delivery = mongoose.model("Delivery", deliverySchema, "delivery");
