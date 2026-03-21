import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import logoUrl from "../assets/Logo.avif";

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:5000/api";
const STORAGE_KEY = "krfuels_auth";
const FORM_STORAGE_KEY = "krfuels_login_form";

const numberFmt = new Intl.NumberFormat("en-IN");

const emptyForm = { role: "admin", username: "", password: "" };
const emptySourceForm = {
  source_id: "",
  source_name: "",
  lat: "",
  lng: "",
  price_per_mt_ex_terminal: ""
};
const emptyStationForm = {
  station: "",
  lat: "",
  lng: "",
  capacity_in_lt: "",
  dead_stock_in_lt: "",
  usable_lt: "",
  sufficient_fuel: "YES"
};
const emptyManagerForm = {
  name: "",
  username: "",
  password: "",
  station: ""
};
const emptyTruckForm = {
  truck_id: "",
  type: "",
  station: "",
  lat: "",
  lon: ""
};

const parseNumber = (value) => {
  if (value === "" || value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toMapUrl = (coordinates) => {
  if (!coordinates) return "";
  const lat = parseNumber(coordinates.lat);
  const lng = parseNumber(coordinates.lng ?? coordinates.lon);
  if (lat === null || lng === null) return "";
  return `https://www.google.com/maps?q=${lat},${lng}`;
};

const buildDirectionsUrl = ({ origin, destination, waypoints }) => {
  if (!origin || !destination) return "";
  const params = new URLSearchParams({
    api: "1",
    origin: `${origin.lat},${origin.lng}`,
    destination: `${destination.lat},${destination.lng}`
  });
  if (waypoints?.length) {
    params.set(
      "waypoints",
      waypoints.map((point) => `${point.lat},${point.lng}`).join("|")
    );
  }
  return `https://www.google.com/maps/dir/?${params.toString()}`;
};

const VIEW_KEYS = ["menu", "stations", "sources", "trucks", "deficit", "managers"];


const computeSufficientFuel = (capacity, deadStock) => {
  if (capacity === null || deadStock === null) return "YES";
  return deadStock >= 0.6 * capacity ? "NO" : "YES";
};

const computeStockFields = ({ capacity, deadStock, usable, changedField }) => {
  if (capacity === null) {
    return { deadStock, usable };
  }

  if (changedField === "dead_stock_in_lt" && deadStock !== null) {
    return { deadStock, usable: capacity - deadStock };
  }
  if (changedField === "usable_lt" && usable !== null) {
    return { deadStock: capacity - usable, usable };
  }
  if (changedField === "capacity_in_lt") {
    if (deadStock !== null) {
      return { deadStock, usable: capacity - deadStock };
    }
    if (usable !== null) {
      return { deadStock: capacity - usable, usable };
    }
  }

  return { deadStock, usable };
};

function App() {
  const [auth, setAuth] = useState({ token: "", role: "", name: "", station: "" });
  const [form, setForm] = useState(emptyForm);
  const [showPassword, setShowPassword] = useState(false);
  const [loginError, setLoginError] = useState("");
  const [loginLoading, setLoginLoading] = useState(false);

  const [sources, setSources] = useState([]);
  const [stations, setStations] = useState([]);
  const [trucks, setTrucks] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [activeView, setActiveView] = useState("menu");
  const [routePlanOpen, setRoutePlanOpen] = useState(false);
  const [routePlanLoading, setRoutePlanLoading] = useState(false);
  const [routePlanError, setRoutePlanError] = useState("");
  const [routePlanData, setRoutePlanData] = useState(null);

  const [sourceForm, setSourceForm] = useState(emptySourceForm);
  const [sourceEditingId, setSourceEditingId] = useState("");
  const [sourceAddOpen, setSourceAddOpen] = useState(false);
  const [sourceEditOpen, setSourceEditOpen] = useState(false);
  const [sourceSaving, setSourceSaving] = useState(false);
  const [sourceNotice, setSourceNotice] = useState("");
  const [sourceError, setSourceError] = useState("");

  const [stationForm, setStationForm] = useState(emptyStationForm);
  const [stationEditingId, setStationEditingId] = useState("");
  const [stationAddOpen, setStationAddOpen] = useState(false);
  const [stationEditOpen, setStationEditOpen] = useState(false);
  const [stationSaving, setStationSaving] = useState(false);
  const [stationNotice, setStationNotice] = useState("");
  const [stationError, setStationError] = useState("");
  const [confirmState, setConfirmState] = useState({
    open: false,
    type: "",
    id: "",
    name: ""
  });
  const [managerForm, setManagerForm] = useState(emptyManagerForm);
  const [managerSaving, setManagerSaving] = useState(false);
  const [managerNotice, setManagerNotice] = useState("");
  const [managerError, setManagerError] = useState("");
  const [showManagerPassword, setShowManagerPassword] = useState(false);

  const [truckForm, setTruckForm] = useState(emptyTruckForm);
  const [truckEditingId, setTruckEditingId] = useState("");
  const [truckAddOpen, setTruckAddOpen] = useState(false);
  const [truckEditOpen, setTruckEditOpen] = useState(false);
  const [truckSaving, setTruckSaving] = useState(false);
  const [truckNotice, setTruckNotice] = useState("");
  const [truckError, setTruckError] = useState("");

  const [managerStation, setManagerStation] = useState(null);
  const [managerTrucks, setManagerTrucks] = useState([]);
  const [managerLoading, setManagerLoading] = useState(false);
  const [managerErrorMsg, setManagerErrorMsg] = useState("");
  const [managerNoticeMsg, setManagerNoticeMsg] = useState("");
  const [managerAlertMsg, setManagerAlertMsg] = useState("");
  const [managerTruckForm, setManagerTruckForm] = useState({ truck_id: "", type: "" });
  const [managerStockForm, setManagerStockForm] = useState({
    dead_stock_in_lt: "",
    usable_lt: ""
  });
  const [managerTruckSaving, setManagerTruckSaving] = useState(false);
  const [managerStockSaving, setManagerStockSaving] = useState(false);

  const totals = useMemo(() => {
    const capacity = stations.reduce((sum, item) => sum + (item.capacity_in_lt || 0), 0);
    const usable = stations.reduce((sum, item) => sum + (item.usable_lt || 0), 0);
    return { capacity, usable };
  }, [stations]);

  const deficitStations = useMemo(
    () =>
      stations.filter(
        (station) => String(station.sufficient_fuel || "").toUpperCase() === "NO"
      ),
    [stations]
  );



  useEffect(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (!stored) return;

    try {
      const parsed = JSON.parse(stored);
      if (parsed?.token) {
        setAuth({
          token: parsed.token,
          role: parsed.role || "",
          name: parsed.name || "",
          station: parsed.station || ""
        });
      }
    } catch (parseError) {
      localStorage.removeItem(STORAGE_KEY);
    }
  }, []);

  useEffect(() => {
    const storedForm = localStorage.getItem(FORM_STORAGE_KEY);
    if (!storedForm) return;

    try {
      const parsed = JSON.parse(storedForm);
      if (parsed?.username || parsed?.password || parsed?.role) {
        setForm({ ...emptyForm, ...parsed });
      }
    } catch (parseError) {
      localStorage.removeItem(FORM_STORAGE_KEY);
    }
  }, []);

  useEffect(() => {
    localStorage.setItem(FORM_STORAGE_KEY, JSON.stringify(form));
  }, [form]);

  useEffect(() => {
    const syncView = () => {
      const hash = window.location.hash.replace("#", "").trim();
      const nextView = VIEW_KEYS.includes(hash) ? hash : "menu";
      setActiveView(nextView);
    };
    syncView();
    window.addEventListener("hashchange", syncView);
    return () => window.removeEventListener("hashchange", syncView);
  }, []);

  useEffect(() => {
    if (auth.token && auth.role === "admin" && !window.location.hash) {
      window.history.replaceState(null, "", "#menu");
      setActiveView("menu");
    }
  }, [auth.token, auth.role]);

  const updateForm = (field, value) => {
    setLoginError("");
    setForm((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const navigateView = (view) => {
    const nextView = VIEW_KEYS.includes(view) ? view : "menu";
    window.location.hash = nextView;
  };

  const clearSourceMessages = () => {
    setSourceNotice("");
    setSourceError("");
  };

  const clearStationMessages = () => {
    setStationNotice("");
    setStationError("");
  };

  const clearTruckMessages = () => {
    setTruckNotice("");
    setTruckError("");
  };

  const clearManagerMessages = () => {
    setManagerNotice("");
    setManagerError("");
  };

  const clearManagerDashboardMessages = () => {
    setManagerNoticeMsg("");
    setManagerErrorMsg("");
    setManagerAlertMsg("");
  };

  const updateSourceForm = (field, value) => {
    clearSourceMessages();
    setSourceForm((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const updateStationForm = (field, value) => {
    clearStationMessages();
    setStationForm((prev) => {
      const next = { ...prev, [field]: value };
      if (["capacity_in_lt", "dead_stock_in_lt", "usable_lt"].includes(field)) {
        const capacity = parseNumber(next.capacity_in_lt);
        const deadStock = parseNumber(next.dead_stock_in_lt);
        const usable = parseNumber(next.usable_lt);
        const computed = computeStockFields({
          capacity,
          deadStock,
          usable,
          changedField: field
        });
        if (field === "dead_stock_in_lt" && computed.usable !== null) {
          next.usable_lt = computed.usable;
        }
        if (field === "usable_lt" && computed.deadStock !== null) {
          next.dead_stock_in_lt = computed.deadStock;
        }
        if (field === "capacity_in_lt") {
          if (computed.usable !== null) {
            next.usable_lt = computed.usable;
          }
          if (computed.deadStock !== null) {
            next.dead_stock_in_lt = computed.deadStock;
          }
        }
        next.sufficient_fuel = computeSufficientFuel(capacity, parseNumber(next.dead_stock_in_lt));
      }
      return next;
    });
  };

  const updateTruckForm = (field, value) => {
    clearTruckMessages();
    setTruckForm((prev) => {
      const next = { ...prev, [field]: value };
      if (field === "station") {
        const match = stations.find(
          (item) =>
            String(item.station || "").trim().toLowerCase() ===
            String(value || "").trim().toLowerCase()
        );
        if (match?.coordinates) {
          next.lat = match.coordinates.lat ?? "";
          next.lon = match.coordinates.lng ?? match.coordinates.lon ?? "";
        }
      }
      return next;
    });
  };

  const updateManagerStockFormField = (field, value) => {
    clearManagerDashboardMessages();
    setManagerStockForm((prev) => {
      const next = { ...prev, [field]: value };
      const capacity = parseNumber(managerStation?.capacity_in_lt);
      const deadStock = parseNumber(next.dead_stock_in_lt);
      const usable = parseNumber(next.usable_lt);
      const computed = computeStockFields({
        capacity,
        deadStock,
        usable,
        changedField: field
      });
      if (field === "dead_stock_in_lt" && computed.usable !== null) {
        next.usable_lt = computed.usable;
      }
      if (field === "usable_lt" && computed.deadStock !== null) {
        next.dead_stock_in_lt = computed.deadStock;
      }
      if (field === "capacity_in_lt") {
        if (computed.usable !== null) {
          next.usable_lt = computed.usable;
        }
        if (computed.deadStock !== null) {
          next.dead_stock_in_lt = computed.deadStock;
        }
      }
      return next;
    });
  };

  const updateManagerTruckFormField = (field, value) => {
    clearManagerDashboardMessages();
    setManagerTruckForm((prev) => ({ ...prev, [field]: value }));
  };

  const resetSourceForm = () => {
    setSourceForm(emptySourceForm);
    setSourceEditingId("");
  };

  const resetStationForm = () => {
    setStationForm(emptyStationForm);
    setStationEditingId("");
  };

  const resetTruckForm = () => {
    setTruckForm(emptyTruckForm);
    setTruckEditingId("");
  };

  const resetManagerForm = () => {
    setManagerForm(emptyManagerForm);
    setShowManagerPassword(false);
  };

  const closeConfirm = () => {
    setConfirmState({ open: false, type: "", id: "", name: "" });
  };

  const upsertSource = (item) => {
    setSources((prev) => {
      const next = prev.filter((source) => source._id !== item._id);
      next.push(item);
      return next.sort((a, b) =>
        String(a.source_id || "").localeCompare(String(b.source_id || ""))
      );
    });
  };

  const upsertStation = (item) => {
    setStations((prev) => {
      const next = prev.filter((station) => station._id !== item._id);
      next.push(item);
      return next.sort((a, b) =>
        String(a.station || "").localeCompare(String(b.station || ""))
      );
    });
  };

  const upsertTruck = (item) => {
    setTrucks((prev) => {
      const next = prev.filter((truck) => truck._id !== item._id);
      next.push(item);
      return next.sort((a, b) =>
        String(a.truck_id || "").localeCompare(String(b.truck_id || ""))
      );
    });
  };

  const openAddSource = () => {
    clearSourceMessages();
    resetSourceForm();
    setSourceEditOpen(false);
    setSourceAddOpen(true);
  };

  const openAddStation = () => {
    clearStationMessages();
    resetStationForm();
    setStationEditOpen(false);
    setStationAddOpen(true);
  };

  const openAddTruck = () => {
    clearTruckMessages();
    resetTruckForm();
    setTruckEditOpen(false);
    setTruckAddOpen(true);
  };

  const buildSourcePayload = () => {
    const errors = [];
    const sourceId = sourceForm.source_id.trim();
    const sourceName = sourceForm.source_name.trim();
    const lat = parseNumber(sourceForm.lat);
    const lng = parseNumber(sourceForm.lng);
    const price = parseNumber(sourceForm.price_per_mt_ex_terminal);

    if (!sourceId) errors.push("source id");
    if (!sourceName) errors.push("source name");
    if (lat === null || lng === null) errors.push("coordinates");
    if (price === null) errors.push("price / mt ex terminal");

    return {
      payload: {
        source_id: sourceId,
        source_name: sourceName,
        coordinates: { lat, lng },
        price_per_mt_ex_terminal: price
      },
      errors
    };
  };

  const buildStationPayload = () => {
    const errors = [];
    const station = stationForm.station.trim();
    const lat = parseNumber(stationForm.lat);
    const lng = parseNumber(stationForm.lng);
    const capacity = parseNumber(stationForm.capacity_in_lt);
    const deadStock = parseNumber(stationForm.dead_stock_in_lt);
    const usable = parseNumber(stationForm.usable_lt);
    const changedField =
      deadStock !== null ? "dead_stock_in_lt" : usable !== null ? "usable_lt" : "capacity_in_lt";
    const computed = computeStockFields({
      capacity,
      deadStock,
      usable,
      changedField
    });
    const sufficientFuel = computeSufficientFuel(
      capacity,
      computed.deadStock ?? deadStock
    );

    if (!station) errors.push("station name");
    if (lat === null || lng === null) errors.push("coordinates");
    if (capacity === null) errors.push("capacity in lt");
    if (computed.deadStock === null && deadStock === null) errors.push("dead stock in lt");
    if (computed.usable === null && usable === null) errors.push("usable lt");

    return {
      payload: {
        station,
        coordinates: { lat, lng },
        capacity_in_lt: capacity,
        dead_stock_in_lt: computed.deadStock ?? deadStock,
        usable_lt: computed.usable ?? usable,
        sufficient_fuel: sufficientFuel
      },
      errors
    };
  };

  const buildTruckPayload = () => {
    const errors = [];
    const truckId = truckForm.truck_id.trim();
    const type = truckForm.type.trim();
    const station = truckForm.station.trim();
    let lat = parseNumber(truckForm.lat);
    let lon = parseNumber(truckForm.lon);

    if (!truckId) errors.push("truck id");
    if (!type) errors.push("truck type");
    if (!station) errors.push("station");
    if (lat === null || lon === null) {
      const match = stations.find(
        (item) =>
          String(item.station || "").trim().toLowerCase() ===
          station.trim().toLowerCase()
      );
      if (match?.coordinates) {
        lat = match.coordinates.lat;
        lon = match.coordinates.lng ?? match.coordinates.lon;
      }
    }
    if (lat === null || lon === null) errors.push("coordinates");

    return {
      payload: {
        truck_id: truckId,
        type,
        station,
        lat,
        lon
      },
      errors
    };
  };

  const handleLogin = async () => {
    setLoginError("");
    setLoginLoading(true);

    try {
      const response = await axios.post(`${API_BASE}/auth/login`, {
        username: form.username,
        password: form.password
      });

      if (response.data.role !== form.role) {
        setLoginError("Selected role does not match the provided credentials.");
        return;
      }

      const nextAuth = {
        token: response.data.token,
        role: response.data.role,
        name: response.data.name,
        station: response.data.station || ""
      };
      setAuth(nextAuth);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(nextAuth));
      if (response.data.role === "admin") {
        window.history.replaceState(null, "", "#menu");
        setActiveView("menu");
      }
    } catch (loginError) {
      setLoginError(loginError.response?.data?.message || "Unable to sign in.");
    } finally {
      setLoginLoading(false);
    }
  };

  const handleLogout = () => {
    setAuth({ token: "", role: "", name: "", station: "" });
    setForm(emptyForm);
    setSources([]);
    setStations([]);
    setTrucks([]);
    setError("");
    setActiveView("menu");
    setRoutePlanOpen(false);
    setRoutePlanLoading(false);
    setRoutePlanError("");
    setRoutePlanData(null);
    resetSourceForm();
    resetStationForm();
    setSourceAddOpen(false);
    setSourceEditOpen(false);
    setStationAddOpen(false);
    setStationEditOpen(false);
    setTruckAddOpen(false);
    setTruckEditOpen(false);
    closeConfirm();
    resetManagerForm();
    clearManagerMessages();
    clearSourceMessages();
    clearStationMessages();
    clearTruckMessages();
    resetTruckForm();
    setManagerStation(null);
    setManagerTrucks([]);
    setManagerErrorMsg("");
    setManagerNoticeMsg("");
    setManagerAlertMsg("");
    setManagerTruckForm({ truck_id: "", type: "" });
    setManagerStockForm({ dead_stock_in_lt: "", usable_lt: "" });
    localStorage.removeItem(STORAGE_KEY);
    localStorage.removeItem(FORM_STORAGE_KEY);
    window.history.replaceState(null, "", "#");
  };

  const fetchData = async (token) => {
    setLoading(true);
    setError("");

    try {
      const headers = { Authorization: `Bearer ${token}` };
      const [sourcesRes, stationsRes, trucksRes] = await Promise.all([
        axios.get(`${API_BASE}/sources`, { headers }),
        axios.get(`${API_BASE}/stations`, { headers }),
        axios.get(`${API_BASE}/trucks`, { headers })
      ]);
      setSources(sourcesRes.data);
      setStations(stationsRes.data);
      setTrucks(trucksRes.data);
    } catch (fetchError) {
      setError(fetchError.response?.data?.message || "Unable to load data.");
    } finally {
      setLoading(false);
    }
  };

  const fetchManagerOverview = async () => {
    setManagerLoading(true);
    setManagerErrorMsg("");

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      const response = await axios.get(`${API_BASE}/manager/overview`, { headers });
      const station = response.data.station;
      setManagerStation(station);
      setManagerTrucks(response.data.trucks || []);
      setManagerStockForm({
        dead_stock_in_lt: station?.dead_stock_in_lt ?? "",
        usable_lt: station?.usable_lt ?? ""
      });
    } catch (fetchError) {
      setManagerErrorMsg(
        fetchError.response?.data?.message || "Unable to load station overview."
      );
    } finally {
      setManagerLoading(false);
    }
  };

  const handleGiveRoutePlan = async () => {
    setRoutePlanOpen(true);
    setRoutePlanLoading(true);
    setRoutePlanError("");
    setRoutePlanData(null);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      const response = await axios.post(`${API_BASE}/route-plan`, {}, { headers });
      setRoutePlanData(response.data);
    } catch (planError) {
      setRoutePlanError(
        planError.response?.data?.message || "Unable to generate route plan."
      );
    } finally {
      setRoutePlanLoading(false);
    }
  };

  const closeRoutePlan = () => {
    setRoutePlanOpen(false);
    setRoutePlanError("");
  };

  const handleManagerStockSubmit = async (event) => {
    event.preventDefault();
    clearManagerDashboardMessages();
    setManagerStockSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      const payload = {
        dead_stock_in_lt: parseNumber(managerStockForm.dead_stock_in_lt),
        usable_lt: parseNumber(managerStockForm.usable_lt)
      };
      const response = await axios.patch(`${API_BASE}/manager/station`, payload, {
        headers
      });
      const station = response.data;
      setManagerStation(station);
      setManagerStockForm({
        dead_stock_in_lt: station.dead_stock_in_lt ?? "",
        usable_lt: station.usable_lt ?? ""
      });
      setManagerNoticeMsg("Stock levels updated.");
      if (station.dead_stock_in_lt >= 0.6 * station.capacity_in_lt) {
        setManagerAlertMsg(
          "Alert: Dead stock exceeds 60% of total capacity. Marking station as insufficient."
        );
      }
    } catch (saveError) {
      setManagerErrorMsg(
        saveError.response?.data?.message || "Unable to update stock."
      );
    } finally {
      setManagerStockSaving(false);
    }
  };

  const handleManagerTruckSubmit = async (event) => {
    event.preventDefault();
    clearManagerDashboardMessages();

    const truckId = managerTruckForm.truck_id.trim();
    const type = managerTruckForm.type.trim();
    if (!truckId) {
      setManagerErrorMsg("Please provide a truck id.");
      return;
    }

    setManagerTruckSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      const response = await axios.post(
        `${API_BASE}/manager/trucks`,
        { truck_id: truckId, type },
        { headers }
      );
      setManagerTrucks((prev) => {
        const next = prev.filter((truck) => truck._id !== response.data._id);
        next.push(response.data);
        return next.sort((a, b) =>
          String(a.truck_id || "").localeCompare(String(b.truck_id || ""))
        );
      });
      setManagerNoticeMsg("Truck saved for this station.");
      setManagerTruckForm({ truck_id: "", type: "" });
    } catch (saveError) {
      setManagerErrorMsg(
        saveError.response?.data?.message || "Unable to save truck."
      );
    } finally {
      setManagerTruckSaving(false);
    }
  };

  const handleManagerTruckRemove = async (truckId) => {
    clearManagerDashboardMessages();
    setManagerTruckSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      await axios.delete(`${API_BASE}/manager/trucks/${truckId}`, { headers });
      setManagerTrucks((prev) => prev.filter((truck) => truck._id !== truckId));
      setManagerNoticeMsg("Truck removed from this station.");
    } catch (removeError) {
      setManagerErrorMsg(
        removeError.response?.data?.message || "Unable to remove truck."
      );
    } finally {
      setManagerTruckSaving(false);
    }
  };

  useEffect(() => {
    if (auth.token && auth.role === "admin") {
      fetchData(auth.token);
    }
  }, [auth.token, auth.role]);

  useEffect(() => {
    if (auth.token && auth.role === "station_manager") {
      fetchManagerOverview();
    }
  }, [auth.token, auth.role]);

  const handleSourceSubmit = async (event) => {
    event.preventDefault();
    clearSourceMessages();

    const { payload, errors } = buildSourcePayload();
    if (errors.length) {
      setSourceError(`Please provide valid ${errors.join(", ")}.`);
      return;
    }

    setSourceSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      if (sourceEditingId) {
        const response = await axios.patch(
          `${API_BASE}/sources/${sourceEditingId}`,
          payload,
          { headers }
        );
        upsertSource(response.data);
        setSourceNotice("Source updated.");
        setSourceEditOpen(false);
      } else {
        const response = await axios.post(`${API_BASE}/sources`, payload, { headers });
        upsertSource(response.data);
        setSourceNotice("Source added.");
        setSourceAddOpen(false);
      }
      resetSourceForm();
    } catch (saveError) {
      setSourceError(saveError.response?.data?.message || "Unable to save source.");
    } finally {
      setSourceSaving(false);
    }
  };

  const handleStationSubmit = async (event) => {
    event.preventDefault();
    clearStationMessages();

    const { payload, errors } = buildStationPayload();
    if (errors.length) {
      setStationError(`Please provide valid ${errors.join(", ")}.`);
      return;
    }

    setStationSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      if (stationEditingId) {
        const response = await axios.patch(
          `${API_BASE}/stations/${stationEditingId}`,
          payload,
          { headers }
        );
        upsertStation(response.data);
        setStationNotice("Station updated.");
        setStationEditOpen(false);
      } else {
        const response = await axios.post(`${API_BASE}/stations`, payload, { headers });
        upsertStation(response.data);
        setStationNotice("Station added.");
        setStationAddOpen(false);
      }
      resetStationForm();
    } catch (saveError) {
      setStationError(saveError.response?.data?.message || "Unable to save station.");
    } finally {
      setStationSaving(false);
    }
  };

  const handleTruckSubmit = async (event) => {
    event.preventDefault();
    clearTruckMessages();

    const { payload, errors } = buildTruckPayload();
    if (errors.length) {
      setTruckError(`Please provide valid ${errors.join(", ")}.`);
      return;
    }

    setTruckSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      if (truckEditingId) {
        const response = await axios.patch(
          `${API_BASE}/trucks/${truckEditingId}`,
          payload,
          { headers }
        );
        upsertTruck(response.data);
        setTruckNotice("Truck updated.");
        setTruckEditOpen(false);
      } else {
        const response = await axios.post(`${API_BASE}/trucks`, payload, { headers });
        upsertTruck(response.data);
        setTruckNotice("Truck added.");
        setTruckAddOpen(false);
      }
      resetTruckForm();
    } catch (saveError) {
      setTruckError(saveError.response?.data?.message || "Unable to save truck.");
    } finally {
      setTruckSaving(false);
    }
  };

  const handleDeleteSource = async (id) => {
    clearSourceMessages();
    setSourceSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      await axios.delete(`${API_BASE}/sources/${id}`, { headers });
      setSources((prev) => prev.filter((source) => source._id !== id));
      if (sourceEditingId === id) {
        resetSourceForm();
        setSourceEditOpen(false);
      }
      setSourceNotice("Source deleted.");
    } catch (deleteError) {
      setSourceError(deleteError.response?.data?.message || "Unable to delete source.");
    } finally {
      setSourceSaving(false);
    }
  };

  const handleDeleteStation = async (id) => {
    clearStationMessages();
    setStationSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      await axios.delete(`${API_BASE}/stations/${id}`, { headers });
      setStations((prev) => prev.filter((station) => station._id !== id));
      if (stationEditingId === id) {
        resetStationForm();
        setStationEditOpen(false);
      }
      setStationNotice("Station deleted.");
    } catch (deleteError) {
      setStationError(deleteError.response?.data?.message || "Unable to delete station.");
    } finally {
      setStationSaving(false);
    }
  };

  const handleDeleteTruck = async (id) => {
    clearTruckMessages();
    setTruckSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      await axios.delete(`${API_BASE}/trucks/${id}`, { headers });
      setTrucks((prev) => prev.filter((truck) => truck._id !== id));
      if (truckEditingId === id) {
        resetTruckForm();
        setTruckEditOpen(false);
      }
      setTruckNotice("Truck deleted.");
    } catch (deleteError) {
      setTruckError(deleteError.response?.data?.message || "Unable to delete truck.");
    } finally {
      setTruckSaving(false);
    }
  };

  const startEditSource = (item) => {
    clearSourceMessages();
    setSourceAddOpen(false);
    setSourceEditingId(item._id);
    setSourceEditOpen(true);
    setSourceForm({
      source_id: item.source_id || "",
      source_name: item.source_name || "",
      lat: item.coordinates?.lat ?? "",
      lng: item.coordinates?.lng ?? "",
      price_per_mt_ex_terminal: item.price_per_mt_ex_terminal ?? ""
    });
  };

  const startEditStation = (item) => {
    clearStationMessages();
    setStationAddOpen(false);
    setStationEditingId(item._id);
    setStationEditOpen(true);
    const capacity = parseNumber(item.capacity_in_lt);
    const deadStock = parseNumber(item.dead_stock_in_lt);
    setStationForm({
      station: item.station || "",
      lat: item.coordinates?.lat ?? "",
      lng: item.coordinates?.lng ?? "",
      capacity_in_lt: item.capacity_in_lt ?? "",
      dead_stock_in_lt: item.dead_stock_in_lt ?? "",
      usable_lt: item.usable_lt ?? "",
      sufficient_fuel: computeSufficientFuel(capacity, deadStock)
    });
  };

  const startEditTruck = (item) => {
    clearTruckMessages();
    setTruckAddOpen(false);
    setTruckEditingId(item._id);
    setTruckEditOpen(true);
    setTruckForm({
      truck_id: item.truck_id || "",
      type: item.type || "",
      station: item.station || "",
      lat: item.lat ?? "",
      lon: item.lon ?? ""
    });
  };

  const requestDelete = (type, item) => {
    const name =
      type === "source"
        ? item.source_name || item.source_id || "this source"
        : type === "truck"
          ? item.truck_id || "this truck"
          : item.station || "this station";
    setConfirmState({ open: true, type, id: item._id, name });
  };

  const confirmDelete = async () => {
    if (!confirmState.open) return;
    if (confirmState.type === "source") {
      await handleDeleteSource(confirmState.id);
    }
    if (confirmState.type === "station") {
      await handleDeleteStation(confirmState.id);
    }
    if (confirmState.type === "truck") {
      await handleDeleteTruck(confirmState.id);
    }
    closeConfirm();
  };

  const updateManagerForm = (field, value) => {
    clearManagerMessages();
    setManagerForm((prev) => ({ ...prev, [field]: value }));
  };

  const handleManagerSubmit = async (event) => {
    event.preventDefault();
    clearManagerMessages();

    const name = managerForm.name.trim();
    const username = managerForm.username.trim();
    const password = managerForm.password.trim();
    const station = managerForm.station.trim();

    if (!name || !username || !password || !station) {
      setManagerError("Please provide name, email, password, and station.");
      return;
    }

    setManagerSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      await axios.post(
        `${API_BASE}/users/station-managers`,
        { name, username, password, station },
        { headers }
      );
      setManagerNotice("Station manager created.");
      resetManagerForm();
    } catch (saveError) {
      setManagerError(saveError.response?.data?.message || "Unable to add station manager.");
    } finally {
      setManagerSaving(false);
    }
  };

  const roleLabel = form.role === "admin" ? "Admin" : "Station Manager";
  const deliveryPlans = routePlanData?.delivery?.delivery_plans || [];
  const plannedTruckPositions =
    routePlanData?.truckPlanning?.truck_positions || [];
  const costSummary = routePlanData?.tentativeCost?.cost_summary || [];
  const costTotals = routePlanData?.tentativeCost?.totals || {};

  if (!auth.token) {
    return (
      <main className="login-page">
        <header className="brand">
          <img src={logoUrl} alt="KR Fuels" className="brand-logo" />
          <div>
            <p className="brand-kicker">KR Fuels</p>
            <h1>Operations Portal</h1>
            <p className="brand-sub">Sign in to access your role dashboard.</p>
          </div>
        </header>

        {loginError && <p className="error">{loginError}</p>}

        <section className="login-grid">
          <form
            className="login-card single"
            onSubmit={(event) => {
              event.preventDefault();
              handleLogin();
            }}
          >
            <h2>{roleLabel} Login</h2>
            <label>
              Role
              <select
                value={form.role}
                onChange={(event) => updateForm("role", event.target.value)}
              >
                <option value="admin">Admin</option>
                <option value="station_manager">Station Manager</option>
              </select>
            </label>
            <label>
              Username
              <input
                type="email"
                value={form.username}
                onChange={(event) => updateForm("username", event.target.value)}
                placeholder={
                  form.role === "admin" ? "admin@krfuels.com" : "manager@krfuels.com"
                }
                required
              />
            </label>
            <label>
              Password
              <div className="password-field">
                <input
                  type={showPassword ? "text" : "password"}
                  value={form.password}
                  onChange={(event) => updateForm("password", event.target.value)}
                  placeholder="Enter password"
                  required
                />
                <button
                  type="button"
                  className="icon-button"
                  aria-label="Show password"
                  onClick={() => setShowPassword((prev) => !prev)}
                >
                  {showPassword ? "Hide" : "Show"}
                </button>
              </div>
            </label>
            <button type="submit" disabled={loginLoading}>
              {loginLoading ? "Signing In..." : `Login as ${roleLabel}`}
            </button>
          </form>
        </section>
      </main>
    );
  }

  if (auth.role !== "admin") {
    return (
      <main className="manager-page">
        <header className="dashboard-header">
          <div className="brand compact">
            <img src={logoUrl} alt="KR Fuels" className="brand-logo" />
            <div>
              <p className="brand-kicker">KR Fuels</p>
              <h1>Station Manager</h1>
            </div>
          </div>
          <div className="user-actions">
            <span className="pill">{auth.name || "Station Manager"}</span>
            {(managerStation?.station || auth.station) && (
              <span className="pill">
                {managerStation?.station || auth.station}
              </span>
            )}
            <button
              type="button"
              className="secondary"
              onClick={fetchManagerOverview}
              disabled={managerLoading}
            >
              Refresh
            </button>
            <button className="ghost" type="button" onClick={handleLogout}>
              Logout
            </button>
          </div>
        </header>

        {managerErrorMsg && <p className="error">{managerErrorMsg}</p>}
        {managerNoticeMsg && <p className="notice">{managerNoticeMsg}</p>}
        {managerAlertMsg && <p className="notice error">{managerAlertMsg}</p>}

        {managerLoading ? (
          <section className="loading-state">
            <p>Loading station dashboard...</p>
          </section>
        ) : managerStation ? (
          <>
            <section className="stats">
              <div className="card">
                <h3>Total Capacity (Lt)</h3>
                <p>{numberFmt.format(managerStation.capacity_in_lt || 0)}</p>
              </div>
              <div className="card">
                <h3>Dead Stock (Lt)</h3>
                <p>{numberFmt.format(managerStation.dead_stock_in_lt || 0)}</p>
              </div>
              <div className="card">
                <h3>Usable (Lt)</h3>
                <p>{numberFmt.format(managerStation.usable_lt || 0)}</p>
              </div>
              <div className="card">
                <h3>Sufficient Fuel</h3>
                <p>{managerStation.sufficient_fuel || "YES"}</p>
              </div>
            </section>

            <section className="grid">
              <article className="table-wrap">
                <div className="table-header">
                  <div>
                    <h2>Update Station Stock</h2>
                    <p className="table-sub">
                      Update dead stock or usable liters. The other value updates
                      automatically.
                    </p>
                  </div>
                </div>
                <form className="form-panel" onSubmit={handleManagerStockSubmit}>
                  <div className="form-grid">
                    <label>
                      Dead Stock In Lt
                      <input
                        type="number"
                        step="0.01"
                        value={managerStockForm.dead_stock_in_lt}
                        onChange={(event) =>
                          updateManagerStockFormField(
                            "dead_stock_in_lt",
                            event.target.value
                          )
                        }
                        placeholder="3500"
                      />
                    </label>
                    <label>
                      Usable Lt
                      <input
                        type="number"
                        step="0.01"
                        value={managerStockForm.usable_lt}
                        onChange={(event) =>
                          updateManagerStockFormField("usable_lt", event.target.value)
                        }
                        placeholder="21500"
                      />
                    </label>
                    <label>
                      Capacity In Lt
                      <input
                        value={managerStation.capacity_in_lt ?? ""}
                        readOnly
                      />
                    </label>
                    <label>
                      Sufficient Fuel (Auto)
                      <input
                        value={managerStation.sufficient_fuel || "YES"}
                        readOnly
                      />
                    </label>
                  </div>
                  <div className="form-actions">
                    <button type="submit" disabled={managerStockSaving}>
                      {managerStockSaving ? "Updating..." : "Update Stock"}
                    </button>
                  </div>
                </form>
              </article>

              <article className="table-wrap">
                <div className="table-header">
                  <div>
                    <h2>Parked Trucks</h2>
                    <p className="table-sub">
                      Add or remove trucks parked at this station.
                    </p>
                  </div>
                </div>

                <form className="form-panel" onSubmit={handleManagerTruckSubmit}>
                  <div className="form-grid">
                    <label>
                      Truck ID
                      <input
                        value={managerTruckForm.truck_id}
                        onChange={(event) =>
                          updateManagerTruckFormField("truck_id", event.target.value)
                        }
                        placeholder="T01"
                      />
                    </label>
                    <label>
                      Truck Type
                      <select
                        value={managerTruckForm.type}
                        onChange={(event) =>
                          updateManagerTruckFormField("type", event.target.value)
                        }
                      >
                        <option value="">Select type</option>
                        <option value="7MT">7MT</option>
                        <option value="12MT">12MT</option>
                      </select>
                    </label>
                  </div>
                  <div className="form-actions">
                    <button type="submit" disabled={managerTruckSaving}>
                      {managerTruckSaving ? "Saving..." : "Add Truck"}
                    </button>
                  </div>
                </form>

                <table>
                  <thead>
                    <tr>
                      <th>Truck ID</th>
                      <th>Type</th>
                      <th>Location</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {managerTrucks.length ? (
                      managerTrucks.map((truck) => {
                        const mapUrl = toMapUrl({ lat: truck.lat, lon: truck.lon });
                        return (
                          <tr key={truck._id}>
                            <td>{truck.truck_id}</td>
                            <td>{truck.type}</td>
                            <td>
                              {mapUrl ? (
                                <a
                                  className="location-link"
                                  href={mapUrl}
                                  target="_blank"
                                  rel="noreferrer"
                                >
                                  {truck.lat}, {truck.lon}
                                </a>
                              ) : (
                                "-"
                              )}
                            </td>
                            <td>
                              <button
                                type="button"
                                className="danger"
                                onClick={() => handleManagerTruckRemove(truck._id)}
                                disabled={managerTruckSaving}
                              >
                                Remove
                              </button>
                            </td>
                          </tr>
                        );
                      })
                    ) : (
                      <tr>
                        <td colSpan={4}>No trucks parked at this station.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </article>
            </section>
          </>
        ) : (
          <section className="empty-state">
            <h2>No station assignment found.</h2>
            <p>Ask the admin to assign your station in the user profile.</p>
          </section>
        )}
      </main>
    );
  }

  return (
    <main className="page">
      <header className="dashboard-header">
        <div className="brand compact">
          <img src={logoUrl} alt="KR Fuels" className="brand-logo" />
          <div>
            <p className="brand-kicker">KR Fuels</p>
            <h1>Operations Dashboard</h1>
            <p className="brand-sub">Sources, station capacity, and fleet positions.</p>
          </div>
        </div>
        <div className="user-actions">
          <span className="pill">{auth.name || "Admin"}</span>
          {activeView !== "menu" && (
            <button
              type="button"
              className="secondary"
              onClick={() => navigateView("menu")}
            >
              Back to Admin
            </button>
          )}
          <button className="ghost" type="button" onClick={handleLogout}>
            Logout
          </button>
        </div>
      </header>

      {error && <p className="error">{error}</p>}

      {activeView === "menu" && (
        <section className="stats">
        <div className="card">
          <h3>Sources</h3>
          <p>{numberFmt.format(sources.length)}</p>
        </div>
        <div className="card">
          <h3>Stations</h3>
          <p>{numberFmt.format(stations.length)}</p>
        </div>
        <div className="card">
          <h3>Trucks</h3>
          <p>{numberFmt.format(trucks.length)}</p>
        </div>
        <div className="card">
          <h3>Total Capacity (Lt)</h3>
          <p>{numberFmt.format(totals.capacity)}</p>
        </div>
        <div className="card">
          <h3>Total Usable (Lt)</h3>
          <p>{numberFmt.format(totals.usable)}</p>
        </div>
        </section>
      )}
      {routePlanOpen && (
        <div className="modal-backdrop" role="presentation">
          <div className="modal plan-modal" role="dialog" aria-modal="true">
            <div className="modal-header">
              <h3 className="modal-title">Route Plan Suggestion</h3>
              <button type="button" className="ghost" onClick={closeRoutePlan}>
                Close
              </button>
            </div>

            {routePlanLoading ? (
              <p>Generating route plan</p>
            ) : routePlanData ? (
              <div className="plan-body">
                <section className="plan-section">
                  <h4>Delivery Sequence</h4>
                  {deliveryPlans.length === 0 ? (
                    <p>No deficit stations found for today.</p>
                  ) : (
                    deliveryPlans.map((plan, index) => {
                      const stops = plan.stops || [];
                      const directionsUrl = buildDirectionsUrl({
                        origin: {
                          lat: plan.initial_park_lat,
                          lng: plan.initial_park_lon
                        },
                        destination: { lat: plan.final_lat, lng: plan.final_lon },
                        waypoints: [
                          { lat: plan.source_lat, lng: plan.source_lon },
                          ...stops.map((stop) => ({
                            lat: stop.station_lat,
                            lng: stop.station_lon
                          }))
                        ]
                      });

                      return (
                        <div
                          className="plan-card"
                          key={`${plan.truck_id}-${plan.source_id}-${index}`}
                        >
                          <div className="plan-card-header">
                            <div>
                              <h5>
                                Truck {plan.truck_id} - Source {plan.source_id}
                              </h5>
                              <p>Stops: {stops.map((s) => s.station).join(" -> ")}</p>
                            </div>
                            <div>
                              <span className="pill">
                                Total Lt {numberFmt.format(plan.total_lt)}
                              </span>
                            </div>
                          </div>

                          {directionsUrl && (
                            <a
                              className="location-link"
                              href={directionsUrl}
                              target="_blank"
                              rel="noreferrer"
                            >
                              View route on Google Maps
                            </a>
                          )}

                          <div className="plan-grid">
                            <div>
                              <strong>Purchase</strong>
                              <p>{numberFmt.format(plan.tot_purchase)}</p>
                            </div>
                            <div>
                              <strong>Transport</strong>
                              <p>{numberFmt.format(plan.tot_transport)}</p>
                            </div>
                            <div>
                              <strong>Toll</strong>
                              <p>{numberFmt.format(plan.tot_toll)}</p>
                            </div>
                            <div>
                              <strong>Grand Total</strong>
                              <p>{numberFmt.format(plan.grand_total)}</p>
                            </div>
                          </div>
                        </div>
                      );
                    })
                  )}
                </section>

                <section className="plan-section">
                  <h4>Truck Positions After Delivery</h4>
                  {plannedTruckPositions.length ? (
                    <>
                      <table>
                        <thead>
                          <tr>
                            <th>Truck</th>
                            <th>Type</th>
                            <th>Final Station</th>
                            <th>Location</th>
                          </tr>
                        </thead>
                        <tbody>
                          {plannedTruckPositions.map((truck) => {
                            const mapUrl = toMapUrl({
                              lat: truck.lat,
                              lon: truck.lon
                            });
                            return (
                              <tr key={truck.truck_id}>
                                <td>{truck.truck_id}</td>
                                <td>{truck.type}</td>
                                <td>{truck.station}</td>
                                <td>
                                  {mapUrl ? (
                                    <a
                                      className="location-link"
                                      href={mapUrl}
                                      target="_blank"
                                      rel="noreferrer"
                                    >
                                      {truck.lat}, {truck.lon}
                                    </a>
                                  ) : (
                                    "-"
                                  )}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </>
                  ) : (
                    <p>No truck positions were generated.</p>
                  )}
                </section>

                <section className="plan-section">
                  <h4>Tentative Cost Summary</h4>
                  <div className="plan-grid">
                    <div>
                      <strong>Total Purchase</strong>
                      <p>{numberFmt.format(costTotals.purchase || 0)}</p>
                    </div>
                    <div>
                      <strong>Total Transport</strong>
                      <p>{numberFmt.format(costTotals.transport || 0)}</p>
                    </div>
                    <div>
                      <strong>Total Toll</strong>
                      <p>{numberFmt.format(costTotals.toll || 0)}</p>
                    </div>
                    <div>
                      <strong>Grand Total</strong>
                      <p>{numberFmt.format(costTotals.grand_total || 0)}</p>
                    </div>
                  </div>

                  <table>
                    <thead>
                      <tr>
                        <th>Truck</th>
                        <th>Source</th>
                        <th>Stations</th>
                        <th>Grand Total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {costSummary.map((row) => (
                        <tr key={`${row.truck_id}-${row.source_id}`}>
                          <td>{row.truck_id}</td>
                          <td>{row.source_id}</td>
                          <td>{row.stations?.join(" -> ")}</td>
                          <td>{numberFmt.format(row.grand_total)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </section>
              </div>
            ) : (
              <p>No route plan data available.</p>
            )}
          </div>
        </div>
      )}

      {activeView === "menu" && (
        <section className="menu-list">
          <button
            type="button"
            className="menu-item"
            onClick={() => navigateView("stations")}
          >
            <h3>View Stations</h3>
            <p>Manage station capacity, stock, and fuel sufficiency.</p>
          </button>
          <button
            type="button"
            className="menu-item"
            onClick={() => navigateView("sources")}
          >
            <h3>View Sources</h3>
            <p>Update source pricing and coordinates.</p>
          </button>
          <button
            type="button"
            className="menu-item"
            onClick={() => navigateView("trucks")}
          >
            <h3>View Trucks</h3>
            <p>Review parked truck locations and positions.</p>
          </button>
          <button
            type="button"
            className="menu-item"
            onClick={() => navigateView("deficit")}
          >
            <h3>View Stations In Deficit</h3>
            <p>Plan routes for stations marked with low fuel.</p>
          </button>
          <button
            type="button"
            className="menu-item"
            onClick={() => navigateView("managers")}
          >
            <h3>Add Station Manager</h3>
            <p>Create and onboard new station manager accounts.</p>
          </button>
        </section>
      )}

      {loading && activeView !== "menu" ? (
        <section className="loading-state">
          <p>Loading dashboard data...</p>
        </section>
      ) : (
        <section className="grid">
          {activeView === "sources" && (
          <article className="table-wrap">
            <div className="table-header">
              <div>
                <h2>Sources</h2>
                <p className="table-sub">Add, update, or remove source details.</p>
              </div>
              <div className="table-actions">
                <button
                  type="button"
                  className="secondary"
                  onClick={() => {
                    if (sourceAddOpen) {
                      setSourceAddOpen(false);
                      clearSourceMessages();
                    } else {
                      openAddSource();
                    }
                  }}
                >
                  {sourceAddOpen ? "Close Add Source" : "Add Source"}
                </button>
              </div>
            </div>

            <div className={`drop-panel ${sourceAddOpen ? "open" : ""}`}>
              <form className="form-panel" onSubmit={handleSourceSubmit}>
                <div className="form-title">Add Source</div>
                <div className="form-grid">
                  <label>
                    Source Id
                    <input
                      value={sourceForm.source_id}
                      onChange={(event) =>
                        updateSourceForm("source_id", event.target.value)
                      }
                      placeholder="SRC-1001"
                      required
                    />
                  </label>
                  <label>
                    Source Name
                    <input
                      value={sourceForm.source_name}
                      onChange={(event) =>
                        updateSourceForm("source_name", event.target.value)
                      }
                      placeholder="Primary Depot"
                      required
                    />
                  </label>
                  <label>
                    Price / MT Ex Terminal
                    <input
                      type="number"
                      step="0.01"
                      value={sourceForm.price_per_mt_ex_terminal}
                      onChange={(event) =>
                        updateSourceForm(
                          "price_per_mt_ex_terminal",
                          event.target.value
                        )
                      }
                      placeholder="64500"
                      required
                    />
                  </label>
                  <label>
                    Latitude
                    <input
                      type="number"
                      step="any"
                      value={sourceForm.lat}
                      onChange={(event) => updateSourceForm("lat", event.target.value)}
                      placeholder="13.0827"
                      required
                    />
                  </label>
                  <label>
                    Longitude
                    <input
                      type="number"
                      step="any"
                      value={sourceForm.lng}
                      onChange={(event) => updateSourceForm("lng", event.target.value)}
                      placeholder="80.2707"
                      required
                    />
                  </label>
                </div>
                <div className="form-actions">
                  <button type="submit" disabled={sourceSaving}>
                    Add Source
                  </button>
                  <button
                    type="button"
                    className="secondary"
                    onClick={() => {
                      setSourceAddOpen(false);
                      resetSourceForm();
                      clearSourceMessages();
                    }}
                    disabled={sourceSaving}
                  >
                    Cancel
                  </button>
                </div>
                {sourceError && <p className="notice error">{sourceError}</p>}
                {sourceNotice && <p className="notice">{sourceNotice}</p>}
              </form>
            </div>


            <table>
              <thead>
                <tr>
                  <th>Source Id</th>
                  <th>Source Name</th>
                  <th>Location</th>
                  <th>Price / MT Ex Terminal</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {sources.map((item) => {
                  const mapUrl = toMapUrl(item.coordinates);
                  return (
                    <tr key={item._id}>
                      <td>{item.source_id}</td>
                      <td>{item.source_name}</td>
                      <td>
                        {mapUrl ? (
                          <a
                            className="location-link"
                            href={mapUrl}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {item.coordinates?.lat}, {item.coordinates?.lng}
                          </a>
                        ) : (
                          "-"
                        )}
                      </td>
                      <td>{numberFmt.format(item.price_per_mt_ex_terminal)}</td>
                      <td>
                        <div className="action-buttons">
                          <button
                            type="button"
                            className="secondary"
                            onClick={() => startEditSource(item)}
                            disabled={sourceSaving}
                          >
                            Edit
                          </button>
                          <button
                            type="button"
                            className="danger"
                            onClick={() => requestDelete("source", item)}
                            disabled={sourceSaving}
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </article>
          )}

          {activeView === "stations" && (
          <article className="table-wrap">
            <div className="table-header">
              <div>
                <h2>Stations</h2>
                <p className="table-sub">Manage station capacity and stock levels.</p>
              </div>
              <div className="table-actions">
                <button
                  type="button"
                  className="secondary"
                  onClick={() => {
                    if (stationAddOpen) {
                      setStationAddOpen(false);
                      clearStationMessages();
                    } else {
                      openAddStation();
                    }
                  }}
                >
                  {stationAddOpen ? "Close Add Station" : "Add Station"}
                </button>
              </div>
            </div>

            <div className={`drop-panel ${stationAddOpen ? "open" : ""}`}>
              <form className="form-panel" onSubmit={handleStationSubmit}>
                <div className="form-title">Add Station</div>
                <div className="form-grid">
                  <label>
                    Station Name
                    <input
                      value={stationForm.station}
                      onChange={(event) =>
                        updateStationForm("station", event.target.value)
                      }
                      placeholder="Anna Nagar"
                      required
                    />
                  </label>
                <label>
                  Sufficient Fuel (Auto)
                  <input value={stationForm.sufficient_fuel} readOnly />
                </label>
                  <label>
                    Capacity In Lt
                    <input
                      type="number"
                      step="0.01"
                      value={stationForm.capacity_in_lt}
                      onChange={(event) =>
                        updateStationForm("capacity_in_lt", event.target.value)
                      }
                      placeholder="25000"
                      required
                    />
                  </label>
                  <label>
                    Dead Stock In Lt
                    <input
                      type="number"
                      step="0.01"
                      value={stationForm.dead_stock_in_lt}
                      onChange={(event) =>
                        updateStationForm("dead_stock_in_lt", event.target.value)
                      }
                      placeholder="3500"
                      required
                    />
                  </label>
                  <label>
                    Usable Lt
                    <input
                      type="number"
                      step="0.01"
                      value={stationForm.usable_lt}
                      onChange={(event) =>
                        updateStationForm("usable_lt", event.target.value)
                      }
                      placeholder="21500"
                      required
                    />
                  </label>
                  <label>
                    Latitude
                    <input
                      type="number"
                      step="any"
                      value={stationForm.lat}
                      onChange={(event) => updateStationForm("lat", event.target.value)}
                      placeholder="13.0569"
                      required
                    />
                  </label>
                  <label>
                    Longitude
                    <input
                      type="number"
                      step="any"
                      value={stationForm.lng}
                      onChange={(event) => updateStationForm("lng", event.target.value)}
                      placeholder="80.2425"
                      required
                    />
                  </label>
                </div>
                <div className="form-actions">
                  <button type="submit" disabled={stationSaving}>
                    Add Station
                  </button>
                  <button
                    type="button"
                    className="secondary"
                    onClick={() => {
                      setStationAddOpen(false);
                      resetStationForm();
                      clearStationMessages();
                    }}
                    disabled={stationSaving}
                  >
                    Cancel
                  </button>
                </div>
                {stationError && <p className="notice error">{stationError}</p>}
                {stationNotice && <p className="notice">{stationNotice}</p>}
              </form>
            </div>


            <table>
              <thead>
                <tr>
                  <th>Station</th>
                  <th>Location</th>
                  <th>Capacity In Lt</th>
                  <th>Dead Stock In Lt</th>
                  <th>Usable Lt</th>
                  <th>Sufficient Fuel</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {stations.map((item) => {
                  const mapUrl = toMapUrl(item.coordinates);
                  return (
                    <tr key={item._id}>
                      <td>{item.station}</td>
                      <td>
                        {mapUrl ? (
                          <a
                            className="location-link"
                            href={mapUrl}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {item.coordinates?.lat}, {item.coordinates?.lng}
                          </a>
                        ) : (
                          "-"
                        )}
                      </td>
                      <td>{numberFmt.format(item.capacity_in_lt)}</td>
                      <td>{numberFmt.format(item.dead_stock_in_lt)}</td>
                      <td>{numberFmt.format(item.usable_lt)}</td>
                      <td>{item.sufficient_fuel || "-"}</td>
                      <td>
                        <div className="action-buttons">
                          <button
                            type="button"
                            className="secondary"
                            onClick={() => startEditStation(item)}
                            disabled={stationSaving}
                          >
                            Edit
                          </button>
                          <button
                            type="button"
                            className="danger"
                            onClick={() => requestDelete("station", item)}
                            disabled={stationSaving}
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </article>
          )}

          {activeView === "trucks" && (
          <article className="table-wrap">
            <div className="table-header">
              <div>
                <h2>Trucks</h2>
              </div>
              <div className="table-actions">
                <button
                  type="button"
                  className="secondary"
                  onClick={() => {
                    if (truckAddOpen) {
                      setTruckAddOpen(false);
                      clearTruckMessages();
                    } else {
                      openAddTruck();
                    }
                  }}
                >
                  {truckAddOpen ? "Close Add Truck" : "Add Truck"}
                </button>
              </div>
            </div>

            <div className={`drop-panel ${truckAddOpen ? "open" : ""}`}>
              <form className="form-panel" onSubmit={handleTruckSubmit}>
                <div className="form-title">Add Truck</div>
                <div className="form-grid">
                  <label>
                    Truck ID
                    <input
                      value={truckForm.truck_id}
                      onChange={(event) =>
                        updateTruckForm("truck_id", event.target.value)
                      }
                      placeholder="T01"
                      required
                    />
                  </label>
                  <label>
                    Truck Type
                    <select
                      value={truckForm.type}
                      onChange={(event) => updateTruckForm("type", event.target.value)}
                      required
                    >
                      <option value="">Select type</option>
                      <option value="7MT">7MT</option>
                      <option value="12MT">12MT</option>
                    </select>
                  </label>
                  <label>
                    Station
                    <input
                      list="truck-station-options"
                      value={truckForm.station}
                      onChange={(event) =>
                        updateTruckForm("station", event.target.value)
                      }
                      placeholder="Station name"
                      required
                    />
                  </label>
                </div>
                <datalist id="truck-station-options">
                  {stations.map((station) => (
                    <option key={station._id} value={station.station} />
                  ))}
                </datalist>
                <div className="form-actions">
                  <button type="submit" disabled={truckSaving}>
                    Add Truck
                  </button>
                  <button
                    type="button"
                    className="secondary"
                    onClick={() => {
                      setTruckAddOpen(false);
                      resetTruckForm();
                      clearTruckMessages();
                    }}
                    disabled={truckSaving}
                  >
                    Cancel
                  </button>
                </div>
                {truckError && <p className="notice error">{truckError}</p>}
                {truckNotice && <p className="notice">{truckNotice}</p>}
              </form>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Truck ID</th>
                  <th>Type</th>
                  <th>Station</th>
                  <th>Location</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {trucks.map((item) => {
                  const mapUrl = toMapUrl({ lat: item.lat, lon: item.lon });
                  return (
                    <tr key={item._id}>
                      <td>{item.truck_id}</td>
                      <td>{item.type}</td>
                      <td>{item.station}</td>
                      <td>
                        {mapUrl ? (
                          <a
                            className="location-link"
                            href={mapUrl}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {item.lat}, {item.lon}
                          </a>
                        ) : (
                          "-"
                        )}
                      </td>
                      <td>
                        <div className="action-buttons">
                          <button
                            type="button"
                            className="secondary"
                            onClick={() => startEditTruck(item)}
                            disabled={truckSaving}
                          >
                            Edit
                          </button>
                          <button
                            type="button"
                            className="danger"
                            onClick={() => requestDelete("truck", item)}
                            disabled={truckSaving}
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </article>
          )}

          {activeView === "deficit" && (
          <article className="table-wrap">
            <div className="table-header">
              <div>
                <h2>Stations In Deficit</h2>
                <p className="table-sub">
                  Stations marked with sufficient fuel as NO.
                </p>
              </div>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Station</th>
                  <th>Location</th>
                  <th>Usable Lt</th>
                  <th>Sufficient Fuel</th>
                </tr>
              </thead>
              <tbody>
                {deficitStations.map((item) => {
                  const mapUrl = toMapUrl(item.coordinates);
                  return (
                    <tr key={item._id}>
                      <td>{item.station}</td>
                      <td>
                        {mapUrl ? (
                          <a
                            className="location-link"
                            href={mapUrl}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {item.coordinates?.lat}, {item.coordinates?.lng}
                          </a>
                        ) : (
                          "-"
                        )}
                      </td>
                      <td>{numberFmt.format(item.usable_lt)}</td>
                      <td>{item.sufficient_fuel || "-"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>

            <div className="form-actions">
              <button
                type="button"
                onClick={handleGiveRoutePlan}
                disabled={routePlanLoading}
              >
                {routePlanLoading ? "Generating route plan" : "Give Route Plan"}
              </button>
            </div>
            {routePlanError && <p className="notice error">{routePlanError}</p>}
          </article>
          )}

          {activeView === "managers" && (
          <article className="table-wrap">
            <div className="table-header">
              <div>
                <h2>Add Station Manager</h2>
                <p className="table-sub">
                  Create a new station manager login account.
                </p>
              </div>
            </div>
            <form className="form-panel" onSubmit={handleManagerSubmit}>
              <div className="form-grid">
                <label>
                  Name
                  <input
                    value={managerForm.name}
                    onChange={(event) => updateManagerForm("name", event.target.value)}
                    placeholder="Station Manager"
                    required
                  />
                </label>
                <label>
                  Station
                  <select
                    value={managerForm.station}
                    onChange={(event) =>
                      updateManagerForm("station", event.target.value)
                    }
                    required
                  >
                    <option value="">Select station</option>
                    {stations.map((station) => (
                      <option key={station._id} value={station.station}>
                        {station.station}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Email
                  <input
                    type="email"
                    value={managerForm.username}
                    onChange={(event) =>
                      updateManagerForm("username", event.target.value)
                    }
                    placeholder="manager@krfuels.com"
                    required
                  />
                </label>
                <label>
                  Password
                  <div className="password-field">
                    <input
                      type={showManagerPassword ? "text" : "password"}
                      value={managerForm.password}
                      onChange={(event) =>
                        updateManagerForm("password", event.target.value)
                      }
                      placeholder="Enter password"
                      required
                    />
                    <button
                      type="button"
                      className="icon-button"
                      onClick={() => setShowManagerPassword((prev) => !prev)}
                    >
                      {showManagerPassword ? "Hide" : "Show"}
                    </button>
                  </div>
                </label>
              </div>
              <div className="form-actions">
                <button type="submit" disabled={managerSaving}>
                  {managerSaving ? "Creating..." : "Create Manager"}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={resetManagerForm}
                  disabled={managerSaving}
                >
                  Reset
                </button>
              </div>
              {managerError && <p className="notice error">{managerError}</p>}
              {managerNotice && <p className="notice">{managerNotice}</p>}
            </form>
          </article>
          )}
        </section>
      )}
      {confirmState.open && (
        <div className="modal-backdrop" role="presentation">
          <div className="modal" role="dialog" aria-modal="true">
            <h3 className="modal-title">
              {confirmState.type === "source"
                ? "Delete Source"
                : confirmState.type === "truck"
                  ? "Delete Truck"
                  : "Delete Station"}
            </h3>
            <p className="modal-sub">
              Are you sure you want to delete {confirmState.name}?
            </p>
            <div className="modal-actions">
              <button
                type="button"
                className="danger"
                onClick={confirmDelete}
                disabled={sourceSaving || stationSaving || truckSaving}
              >
                Delete
              </button>
              <button
                type="button"
                className="secondary"
                onClick={closeConfirm}
                disabled={sourceSaving || stationSaving || truckSaving}
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
      {sourceEditOpen && (
        <div className="modal-backdrop" role="presentation">
          <div className="modal form-modal" role="dialog" aria-modal="true">
            <div className="modal-header">
              <h3 className="modal-title">
                Update Source
                {sourceForm.source_name ? `: ${sourceForm.source_name}` : ""}
              </h3>
              <button
                type="button"
                className="ghost"
                onClick={() => {
                  setSourceEditOpen(false);
                  resetSourceForm();
                  clearSourceMessages();
                }}
              >
                Close
              </button>
            </div>
            <form className="modal-form" onSubmit={handleSourceSubmit}>
              <div className="form-grid">
                <label>
                  Source Id
                  <input
                    value={sourceForm.source_id}
                    onChange={(event) => updateSourceForm("source_id", event.target.value)}
                    placeholder="SRC-1001"
                    required
                  />
                </label>
                <label>
                  Source Name
                  <input
                    value={sourceForm.source_name}
                    onChange={(event) =>
                      updateSourceForm("source_name", event.target.value)
                    }
                    placeholder="Primary Depot"
                    required
                  />
                </label>
                <label>
                  Price / MT Ex Terminal
                  <input
                    type="number"
                    step="0.01"
                    value={sourceForm.price_per_mt_ex_terminal}
                    onChange={(event) =>
                      updateSourceForm(
                        "price_per_mt_ex_terminal",
                        event.target.value
                      )
                    }
                    placeholder="64500"
                    required
                  />
                </label>
                <label>
                  Latitude
                  <input
                    type="number"
                    step="any"
                    value={sourceForm.lat}
                    onChange={(event) => updateSourceForm("lat", event.target.value)}
                    placeholder="13.0827"
                    required
                  />
                </label>
                <label>
                  Longitude
                  <input
                    type="number"
                    step="any"
                    value={sourceForm.lng}
                    onChange={(event) => updateSourceForm("lng", event.target.value)}
                    placeholder="80.2707"
                    required
                  />
                </label>
              </div>
              <div className="form-actions">
                <button type="submit" disabled={sourceSaving}>
                  Update Source
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => {
                    setSourceEditOpen(false);
                    resetSourceForm();
                    clearSourceMessages();
                  }}
                  disabled={sourceSaving}
                >
                  Cancel
                </button>
              </div>
              {sourceError && <p className="notice error">{sourceError}</p>}
              {sourceNotice && <p className="notice">{sourceNotice}</p>}
            </form>
          </div>
        </div>
      )}
      {stationEditOpen && (
        <div className="modal-backdrop" role="presentation">
          <div className="modal form-modal" role="dialog" aria-modal="true">
            <div className="modal-header">
              <h3 className="modal-title">
                Update Station
                {stationForm.station ? `: ${stationForm.station}` : ""}
              </h3>
              <button
                type="button"
                className="ghost"
                onClick={() => {
                  setStationEditOpen(false);
                  resetStationForm();
                  clearStationMessages();
                }}
              >
                Close
              </button>
            </div>
            <form className="modal-form" onSubmit={handleStationSubmit}>
              <div className="form-grid">
                <label>
                  Station Name
                  <input
                    value={stationForm.station}
                    onChange={(event) => updateStationForm("station", event.target.value)}
                    placeholder="Anna Nagar"
                    required
                  />
                </label>
                <label>
                  Sufficient Fuel (Auto)
                  <input value={stationForm.sufficient_fuel} readOnly />
                </label>
                <label>
                  Capacity In Lt
                  <input
                    type="number"
                    step="0.01"
                    value={stationForm.capacity_in_lt}
                    onChange={(event) =>
                      updateStationForm("capacity_in_lt", event.target.value)
                    }
                    placeholder="25000"
                    required
                  />
                </label>
                <label>
                  Dead Stock In Lt
                  <input
                    type="number"
                    step="0.01"
                    value={stationForm.dead_stock_in_lt}
                    onChange={(event) =>
                      updateStationForm("dead_stock_in_lt", event.target.value)
                    }
                    placeholder="3500"
                    required
                  />
                </label>
                <label>
                  Usable Lt
                  <input
                    type="number"
                    step="0.01"
                    value={stationForm.usable_lt}
                    onChange={(event) =>
                      updateStationForm("usable_lt", event.target.value)
                    }
                    placeholder="21500"
                    required
                  />
                </label>
                <label>
                  Latitude
                  <input
                    type="number"
                    step="any"
                    value={stationForm.lat}
                    onChange={(event) => updateStationForm("lat", event.target.value)}
                    placeholder="13.0569"
                    required
                  />
                </label>
                <label>
                  Longitude
                  <input
                    type="number"
                    step="any"
                    value={stationForm.lng}
                    onChange={(event) => updateStationForm("lng", event.target.value)}
                    placeholder="80.2425"
                    required
                  />
                </label>
              </div>
              <div className="form-actions">
                <button type="submit" disabled={stationSaving}>
                  Update Station
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => {
                    setStationEditOpen(false);
                    resetStationForm();
                    clearStationMessages();
                  }}
                  disabled={stationSaving}
                >
                  Cancel
                </button>
              </div>
              {stationError && <p className="notice error">{stationError}</p>}
              {stationNotice && <p className="notice">{stationNotice}</p>}
            </form>
          </div>
        </div>
      )}
      {truckEditOpen && (
        <div className="modal-backdrop" role="presentation">
          <div className="modal form-modal" role="dialog" aria-modal="true">
            <div className="modal-header">
              <h3 className="modal-title">
                Update Truck
                {truckForm.truck_id ? `: ${truckForm.truck_id}` : ""}
              </h3>
              <button
                type="button"
                className="ghost"
                onClick={() => {
                  setTruckEditOpen(false);
                  resetTruckForm();
                  clearTruckMessages();
                }}
              >
                Close
              </button>
            </div>
            <form className="modal-form" onSubmit={handleTruckSubmit}>
              <div className="form-grid">
                <label>
                  Truck ID
                  <input
                    value={truckForm.truck_id}
                    onChange={(event) =>
                      updateTruckForm("truck_id", event.target.value)
                    }
                    placeholder="T01"
                    required
                  />
                </label>
                  <label>
                    Truck Type
                    <select
                      value={truckForm.type}
                      onChange={(event) => updateTruckForm("type", event.target.value)}
                      required
                    >
                      <option value="">Select type</option>
                      <option value="7MT">7MT</option>
                      <option value="12MT">12MT</option>
                    </select>
                  </label>
                <label>
                  Station
                  <input
                    list="truck-edit-station-options"
                    value={truckForm.station}
                    onChange={(event) =>
                      updateTruckForm("station", event.target.value)
                    }
                    placeholder="Station name"
                    required
                  />
                </label>
              </div>
              <datalist id="truck-edit-station-options">
                {stations.map((station) => (
                  <option key={station._id} value={station.station} />
                ))}
              </datalist>
              <div className="form-actions">
                <button type="submit" disabled={truckSaving}>
                  Update Truck
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => {
                    setTruckEditOpen(false);
                    resetTruckForm();
                    clearTruckMessages();
                  }}
                  disabled={truckSaving}
                >
                  Cancel
                </button>
              </div>
              {truckError && <p className="notice error">{truckError}</p>}
              {truckNotice && <p className="notice">{truckNotice}</p>}
            </form>
          </div>
        </div>
      )}
    </main>
  );
}

export default App;
