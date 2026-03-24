import { Truck } from "../models/truck.js";

const VALID_STATES = ["atStation", "maintenance", "travelling"];

export const ensureTruckStates = async () => {
  const invalidStateFilter = {
    $or: [{ state: { $exists: false } }, { state: { $nin: VALID_STATES } }]
  };

  await Truck.updateMany(
    {
      ...invalidStateFilter,
      station: { $exists: true, $nin: [null, ""] }
    },
    { $set: { state: "atStation" } }
  );

  await Truck.updateMany(
    {
      ...invalidStateFilter,
      $or: [{ station: { $exists: false } }, { station: null }, { station: "" }]
    },
    { $set: { state: "travelling" } }
  );
};
