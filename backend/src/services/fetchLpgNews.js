import crypto from "crypto";

import { LpgNews } from "../models/lpgNews.js";

const NEWS_QUERIES = [
  "Auto LPG India price",
  "LPG terminal price hike India",
  "HPCL BPCL LPG pricing India",
  "Auto LPG Tamil Nadu"
];

const NEWS_TIMEOUT_MS = 12000;
const NEWS_MAX_AGE_DAYS = 30;

const normalizeWhitespace = (value) =>
  String(value || "").replace(/\s+/g, " ").trim();

const decodeHtml = (value) =>
  normalizeWhitespace(value)
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'");

const stripHtml = (value) => decodeHtml(String(value || "").replace(/<[^>]+>/g, " "));

const getTagText = (xml, tag) => {
  const match = xml.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i"));
  return match ? stripHtml(match[1]) : "";
};

const getTagAttr = (xml, tag, attr) => {
  const match = xml.match(new RegExp(`<${tag}[^>]*\\s${attr}=\"([^\"]+)\"[^>]*>`, "i"));
  return match ? normalizeWhitespace(match[1]) : "";
};

const dedupeKeyFor = (title, url) =>
  crypto
    .createHash("sha1")
    .update(`${normalizeWhitespace(title).toLowerCase()}|${normalizeWhitespace(url)}`)
    .digest("hex");

const parseItems = (xml, category) => {
  const items = [];
  const now = Date.now();
  const regex = /<item>([\s\S]*?)<\/item>/gi;
  let match;
  while ((match = regex.exec(xml)) !== null) {
    const raw = match[1];
    const title = getTagText(raw, "title");
    const url = getTagText(raw, "link");
    const description = getTagText(raw, "description");
    const source = getTagText(raw, "source") || "Google News";
    const pubDateRaw = getTagText(raw, "pubDate");
    const publishedAt = pubDateRaw ? new Date(pubDateRaw) : null;
    const publishedOk =
      publishedAt && !Number.isNaN(publishedAt.getTime())
        ? publishedAt
        : null;

    if (!title || !url) {
      continue;
    }

    if (publishedOk) {
      const ageDays = (now - publishedOk.getTime()) / (1000 * 60 * 60 * 24);
      if (ageDays > NEWS_MAX_AGE_DAYS) {
        continue;
      }
    }

    items.push({
      title,
      url,
      source,
      summary: description.slice(0, 600),
      category,
      published_at: publishedOk,
      dedupe_key: dedupeKeyFor(title, url)
    });
  }
  return items;
};

const fetchNewsByQuery = async (query) => {
  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(
    query
  )}&hl=en-IN&gl=IN&ceid=IN:en`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), NEWS_TIMEOUT_MS);
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        "user-agent": "KRFuelsNewsBot/1.0"
      }
    });
    if (!response.ok) {
      throw new Error(`RSS request failed: ${response.status}`);
    }
    const xml = await response.text();
    return parseItems(xml, "LPG Industry");
  } finally {
    clearTimeout(timeout);
  }
};

const storeNews = async (items) => {
  if (!items.length) {
    return;
  }
  const now = new Date();
  const operations = items.map((item) => ({
    updateOne: {
      filter: { dedupe_key: item.dedupe_key },
      update: {
        $set: {
          ...item,
          fetched_at: now
        }
      },
      upsert: true
    }
  }));
  await LpgNews.bulkWrite(operations);
};

export const refreshLpgNews = async ({ maxAgeMinutes = 120 } = {}) => {
  const latest = await LpgNews.findOne().sort({ fetched_at: -1 }).lean();
  const cutoff = Date.now() - maxAgeMinutes * 60 * 1000;
  if (latest?.fetched_at && new Date(latest.fetched_at).getTime() >= cutoff) {
    return LpgNews.find().sort({ published_at: -1, fetched_at: -1 }).limit(40).lean();
  }

  let fetchedItems = [];
  for (const query of NEWS_QUERIES) {
    try {
      // eslint-disable-next-line no-await-in-loop
      const queryItems = await fetchNewsByQuery(query);
      fetchedItems.push(...queryItems);
    } catch (_error) {
      // Continue with other queries and fallback to cached docs.
    }
  }

  const seen = new Set();
  const deduped = [];
  for (const item of fetchedItems) {
    if (seen.has(item.dedupe_key)) {
      continue;
    }
    seen.add(item.dedupe_key);
    deduped.push(item);
  }

  if (deduped.length) {
    await storeNews(deduped);
  }

  return LpgNews.find().sort({ published_at: -1, fetched_at: -1 }).limit(40).lean();
};
