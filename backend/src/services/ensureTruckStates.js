import { Truck } from "../models/truck.js";

const VALID_STATES = ["atStation", "atSource", "atMaintenance", "travelling"];

export const ensureTruckStates = async () => {
  await Truck.updateMany({ state: "maintenance" }, { $set: { state: "atMaintenance" } });

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
      source: { $exists: true, $nin: [null, ""] },
      $or: [{ station: { $exists: false } }, { station: null }, { station: "" }]
    },
    { $set: { state: "atSource" } }
  );

  await Truck.updateMany(
    {
      ...invalidStateFilter,
      maintenance_station: { $exists: true, $nin: [null, ""] }
    },
    { $set: { state: "atMaintenance" } }
  );

  await Truck.updateMany(
    {
      ...invalidStateFilter,
      $or: [
        { station: { $exists: false } },
        { station: null },
        { station: "" }
      ],
      $and: [
        { $or: [{ source: { $exists: false } }, { source: null }, { source: "" }] },
        {
          $or: [
            { maintenance_station: { $exists: false } },
            { maintenance_station: null },
            { maintenance_station: "" }
          ]
        }
      ]
    },
    { $set: { state: "travelling" } }
  );
};
