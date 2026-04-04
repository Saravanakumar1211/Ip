import mongoose from "mongoose";

const { Schema } = mongoose;

const analyticsDashboardSchema = new Schema(
  {
    plan_id: { type: String, required: true, index: true },
    created_at: { type: Date, default: Date.now },
    kpi_dashboard: { type: [Schema.Types.Mixed], default: [] },
    source_comparison: { type: [Schema.Types.Mixed], default: [] },
    station_intelligence: { type: Schema.Types.Mixed, default: {} },
    meta: { type: Schema.Types.Mixed, default: {} }
  },
  { timestamps: true }
);

export const AnalyticsDashboard = mongoose.model(
  "AnalyticsDashboard",
  analyticsDashboardSchema,
  "analyticsDashboard"
);
