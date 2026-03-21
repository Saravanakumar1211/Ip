import { Router } from "express";
import { User } from "../models/user.js";
import { authenticate, requireRole } from "../middleware/auth.js";

const router = Router();

router.use(authenticate);
router.use(requireRole(["admin"]));

router.post("/station-managers", async (req, res) => {
  const { name, username, password, station } = req.body || {};

  if (!name || !username || !password || !station) {
    return res
      .status(400)
      .json({ message: "Name, email, password, and station are required." });
  }

  const normalized = String(username).trim().toLowerCase();

  try {
    const existing = await User.findOne({ username_normalized: normalized });
    if (existing) {
      return res.status(409).json({ message: "User already exists." });
    }

    const user = await User.create({
      username: String(username).trim(),
      username_normalized: normalized,
      name: String(name).trim(),
      type: "station_manager",
      password_hash: String(password),
      station: String(station).trim()
    });

    return res.status(201).json({
      id: user._id.toString(),
      name: user.name,
      username: user.username,
      type: user.type
    });
  } catch (error) {
    if (error?.code === 11000) {
      return res.status(409).json({ message: "User already exists." });
    }
    return res
      .status(500)
      .json({ message: "Unable to create station manager.", error: error.message });
  }
});

export default router;
