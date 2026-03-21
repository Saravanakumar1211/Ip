import { Station } from "../models/station.js";
import { User } from "../models/user.js";

const MANAGER_NAMES = ["Arun", "Rajaram", "Ram", "Ragul", "Ragav", "Barath"];

const slugify = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const buildPassword = (name, index) =>
  `${String(name).toLowerCase()}${String(index + 1).padStart(2, "0")}`;

export const seedStationManagers = async () => {
  const stations = await Station.find().sort({ station: 1 });
  if (!stations.length) {
    return;
  }

  for (let index = 0; index < stations.length; index += 1) {
    const stationName = stations[index].station;
    const existing = await User.findOne({
      type: "station_manager",
      station: stationName
    });
    if (existing) {
      continue;
    }

    const name = MANAGER_NAMES[index % MANAGER_NAMES.length];
    const stationSlug = slugify(stationName) || `station-${index + 1}`;
    let username = `${name.toLowerCase()}.${stationSlug}@krfuels.com`;
    let normalized = username.toLowerCase();
    let counter = 1;

    while (await User.findOne({ username_normalized: normalized })) {
      counter += 1;
      username = `${name.toLowerCase()}.${stationSlug}${counter}@krfuels.com`;
      normalized = username.toLowerCase();
    }

    const password = buildPassword(name, index);

    await User.create({
      username,
      username_normalized: normalized,
      name,
      type: "station_manager",
      password_hash: password,
      station: stationName
    });
  }
};
