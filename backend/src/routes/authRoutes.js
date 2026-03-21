import { Router } from "express";
import bcrypt from "bcryptjs";

import { User } from "../models/user.js";
import { signToken } from "../middleware/auth.js";

const router = Router();

const normalizeUsername = (value) => String(value || "").trim().toLowerCase();

router.post("/login", (req, res) => {
  const { username, password } = req.body || {};

  if (!username || !password) {
    return res.status(400).json({ message: "Username and password are required." });
  }

  return User.findOne({ username_normalized: normalizeUsername(username) })
    .then(async (user) => {
      if (!user) {
        return res.status(401).json({ message: "Invalid credentials." });
      }

      const rawPassword = String(password);
      const isStationManager = user.type === "station_manager";
      const isValid = isStationManager
        ? rawPassword === String(user.password_hash || "")
        : await bcrypt.compare(rawPassword, user.password_hash);
      if (!isValid) {
        return res.status(401).json({ message: "Invalid credentials." });
      }

      const token = signToken({
        sub: user._id.toString(),
        role: user.type,
        name: user.name,
        station: user.station || ""
      });

      return res.json({
        token,
        role: user.type,
        name: user.name,
        station: user.station || ""
      });
    })
    .catch((error) =>
      res.status(500).json({ message: "Unable to sign in.", error: error.message })
    );
});

export default router;
