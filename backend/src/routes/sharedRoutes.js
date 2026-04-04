import { Router } from "express";

import { authenticate } from "../middleware/auth.js";
import { refreshLpgNews } from "../services/fetchLpgNews.js";
import { LpgNews } from "../models/lpgNews.js";

const router = Router();

router.use(authenticate);

router.get("/news/feed", async (_req, res) => {
  try {
    const news = await refreshLpgNews();
    res.json({
      fetched_at: news[0]?.fetched_at || new Date(),
      items: news.map((item) => ({
        id: item._id.toString(),
        title: item.title,
        url: item.url,
        source: item.source,
        summary: item.summary,
        category: item.category,
        published_at: item.published_at
      }))
    });
  } catch (error) {
    res.status(500).json({ message: "Failed to fetch LPG news.", error: error.message });
  }
});

router.get("/news/:id", async (req, res) => {
  try {
    const item = await LpgNews.findById(req.params.id).lean();
    if (!item) {
      return res.status(404).json({ message: "News item not found." });
    }
    return res.json({
      id: item._id.toString(),
      title: item.title,
      url: item.url,
      source: item.source,
      summary: item.summary,
      category: item.category,
      published_at: item.published_at,
      fetched_at: item.fetched_at
    });
  } catch (error) {
    if (error?.name === "CastError") {
      return res.status(400).json({ message: "Invalid news id." });
    }
    return res.status(500).json({ message: "Failed to fetch news item.", error: error.message });
  }
});

export default router;
