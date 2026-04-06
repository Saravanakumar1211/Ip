import mongoose from "mongoose";

const { Schema } = mongoose;

const stationChangeItemSchema = new Schema(
  {
    field: { type: String, required: true, trim: true },
    before: { type: Schema.Types.Mixed, default: null },
    after: { type: Schema.Types.Mixed, default: null }
  },
  { _id: false }
);

const stationChangeLogSchema = new Schema(
  {
    station_id: { type: Schema.Types.ObjectId, required: true, index: true },
    station_name: { type: String, required: true, trim: true },
    station_name_normalized: { type: String, required: true, trim: true, index: true },
    region: { type: String, required: true, trim: true },
    region_normalized: { type: String, required: true, trim: true, index: true },
    actor_role: {
      type: String,
      required: true,
      enum: ["admin", "station_manager"],
      index: true
    },
    actor_user_id: { type: String, required: true, trim: true, index: true },
    actor_name: { type: String, default: "", trim: true },
    actor_station: { type: String, default: "", trim: true },
    source: {
      type: String,
      required: true,
      enum: ["admin_dashboard", "station_manager_dashboard"]
    },
    changed_fields: { type: [String], default: [] },
    changes: { type: [stationChangeItemSchema], default: [] },
    created_at: { type: Date, default: Date.now, index: true }
  },
  { timestamps: false }
);

export const StationChangeLog = mongoose.model(
  "StationChangeLog",
  stationChangeLogSchema,
  "stationChangeLog"
);
