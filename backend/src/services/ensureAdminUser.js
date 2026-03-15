import bcrypt from "bcryptjs";

import { User } from "../models/user.js";

export const ensureAdminUser = async () => {
  const adminEmail = process.env.ADMIN_EMAIL;
  const adminPassword = process.env.ADMIN_PASSWORD;
  const adminName = process.env.ADMIN_NAME || "Admin";

  if (!adminEmail || !adminPassword) {
    console.warn("Admin seed skipped: ADMIN_EMAIL or ADMIN_PASSWORD is missing.");
    return;
  }

  const normalized = String(adminEmail).trim().toLowerCase();
  const existing = await User.findOne({
    $or: [{ type: "admin" }, { username_normalized: normalized }]
  });

  if (existing) {
    return;
  }

  const passwordHash = await bcrypt.hash(String(adminPassword), 10);

  await User.create({
    username: String(adminEmail).trim(),
    username_normalized: normalized,
    name: String(adminName).trim(),
    type: "admin",
    password_hash: passwordHash
  });
};
