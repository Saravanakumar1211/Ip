import mongoose from "mongoose";

const lpgNewsSchema = new mongoose.Schema(
  {
    title: { type: String, required: true, trim: true },
    url: { type: String, trim: true, default: "" },
    source: { type: String, trim: true, default: "Google News" },
    summary: { type: String, trim: true, default: "" },
    category: { type: String, trim: true, default: "LPG Industry" },
    published_at: { type: Date, default: null },
    fetched_at: { type: Date, default: Date.now, index: true },
    dedupe_key: { type: String, required: true, unique: true, trim: true }
  },
  { timestamps: true }
);

export const LpgNews = mongoose.model("LpgNews", lpgNewsSchema, "lpgNews");
