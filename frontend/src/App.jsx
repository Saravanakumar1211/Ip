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
  price_in_lt: ""
};
const emptyStationForm = {
  station: "",
  lat: "",
  lng: "",
  capacity_in_lt: "",
  dead_stock_in_lt: "",
  usable_lt: ""
};
const emptyManagerForm = {
  name: "",
  username: "",
  password: ""
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
  const lng = parseNumber(coordinates.lng);
  if (lat === null || lng === null) return "";
  return `https://www.google.com/maps?q=${lat},${lng}`;
};

function App() {
  const [auth, setAuth] = useState({ token: "", role: "", name: "" });
  const [form, setForm] = useState(emptyForm);
  const [showPassword, setShowPassword] = useState(false);
  const [loginError, setLoginError] = useState("");
  const [loginLoading, setLoginLoading] = useState(false);

  const [sources, setSources] = useState([]);
  const [stations, setStations] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

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
  const [managerOpen, setManagerOpen] = useState(false);
  const [managerForm, setManagerForm] = useState(emptyManagerForm);
  const [managerSaving, setManagerSaving] = useState(false);
  const [managerNotice, setManagerNotice] = useState("");
  const [managerError, setManagerError] = useState("");
  const [showManagerPassword, setShowManagerPassword] = useState(false);

  const totals = useMemo(() => {
    const capacity = stations.reduce((sum, item) => sum + (item.capacity_in_lt || 0), 0);
    const usable = stations.reduce((sum, item) => sum + (item.usable_lt || 0), 0);
    return { capacity, usable };
  }, [stations]);

  useEffect(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (!stored) return;

    try {
      const parsed = JSON.parse(stored);
      if (parsed?.token) {
        setAuth(parsed);
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

  const updateForm = (field, value) => {
    setLoginError("");
    setForm((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const clearSourceMessages = () => {
    setSourceNotice("");
    setSourceError("");
  };

  const clearStationMessages = () => {
    setStationNotice("");
    setStationError("");
  };

  const clearManagerMessages = () => {
    setManagerNotice("");
    setManagerError("");
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
    setStationForm((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const resetSourceForm = () => {
    setSourceForm(emptySourceForm);
    setSourceEditingId("");
  };

  const resetStationForm = () => {
    setStationForm(emptyStationForm);
    setStationEditingId("");
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

  const buildSourcePayload = () => {
    const errors = [];
    const sourceId = sourceForm.source_id.trim();
    const sourceName = sourceForm.source_name.trim();
    const lat = parseNumber(sourceForm.lat);
    const lng = parseNumber(sourceForm.lng);
    const price = parseNumber(sourceForm.price_in_lt);

    if (!sourceId) errors.push("source id");
    if (!sourceName) errors.push("source name");
    if (lat === null || lng === null) errors.push("coordinates");
    if (price === null) errors.push("price in lt");

    return {
      payload: {
        source_id: sourceId,
        source_name: sourceName,
        coordinates: { lat, lng },
        price_in_lt: price
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

    if (!station) errors.push("station name");
    if (lat === null || lng === null) errors.push("coordinates");
    if (capacity === null) errors.push("capacity in lt");
    if (deadStock === null) errors.push("dead stock in lt");
    if (usable === null) errors.push("usable lt");

    return {
      payload: {
        station,
        coordinates: { lat, lng },
        capacity_in_lt: capacity,
        dead_stock_in_lt: deadStock,
        usable_lt: usable
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
        name: response.data.name
      };
      setAuth(nextAuth);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(nextAuth));
    } catch (loginError) {
      setLoginError(loginError.response?.data?.message || "Unable to sign in.");
    } finally {
      setLoginLoading(false);
    }
  };

  const handleLogout = () => {
    setAuth({ token: "", role: "", name: "" });
    setForm(emptyForm);
    setSources([]);
    setStations([]);
    setError("");
    resetSourceForm();
    resetStationForm();
    setSourceAddOpen(false);
    setSourceEditOpen(false);
    setStationAddOpen(false);
    setStationEditOpen(false);
    closeConfirm();
    setManagerOpen(false);
    resetManagerForm();
    clearManagerMessages();
    clearSourceMessages();
    clearStationMessages();
    localStorage.removeItem(STORAGE_KEY);
    localStorage.removeItem(FORM_STORAGE_KEY);
  };

  const fetchData = async (token) => {
    setLoading(true);
    setError("");

    try {
      const headers = { Authorization: `Bearer ${token}` };
      const [sourcesRes, stationsRes] = await Promise.all([
        axios.get(`${API_BASE}/sources`, { headers }),
        axios.get(`${API_BASE}/stations`, { headers })
      ]);
      setSources(sourcesRes.data);
      setStations(stationsRes.data);
    } catch (fetchError) {
      setError(fetchError.response?.data?.message || "Unable to load data.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (auth.token && auth.role === "admin") {
      fetchData(auth.token);
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
      price_in_lt: item.price_in_lt ?? ""
    });
  };

  const startEditStation = (item) => {
    clearStationMessages();
    setStationAddOpen(false);
    setStationEditingId(item._id);
    setStationEditOpen(true);
    setStationForm({
      station: item.station || "",
      lat: item.coordinates?.lat ?? "",
      lng: item.coordinates?.lng ?? "",
      capacity_in_lt: item.capacity_in_lt ?? "",
      dead_stock_in_lt: item.dead_stock_in_lt ?? "",
      usable_lt: item.usable_lt ?? ""
    });
  };

  const requestDelete = (type, item) => {
    const name =
      type === "source"
        ? item.source_name || item.source_id || "this source"
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
    closeConfirm();
  };

  const updateManagerForm = (field, value) => {
    clearManagerMessages();
    setManagerForm((prev) => ({ ...prev, [field]: value }));
  };

  const openManagerModal = () => {
    clearManagerMessages();
    resetManagerForm();
    setManagerOpen(true);
  };

  const handleManagerSubmit = async (event) => {
    event.preventDefault();
    clearManagerMessages();

    const name = managerForm.name.trim();
    const username = managerForm.username.trim();
    const password = managerForm.password.trim();

    if (!name || !username || !password) {
      setManagerError("Please provide name, email, and password.");
      return;
    }

    setManagerSaving(true);

    try {
      const headers = { Authorization: `Bearer ${auth.token}` };
      await axios.post(
        `${API_BASE}/users/station-managers`,
        { name, username, password },
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
            <button className="ghost" type="button" onClick={handleLogout}>
              Logout
            </button>
          </div>
        </header>

        <section className="empty-state">
          <h2>No dashboard data available yet.</h2>
          <p>
            Station manager insights will appear here once the module is enabled by
            the admin team.
          </p>
        </section>
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
            <p className="brand-sub">Sources and station capacity.</p>
          </div>
        </div>
        <div className="user-actions">
          <span className="pill">{auth.name || "Admin"}</span>
          <button type="button" className="secondary" onClick={openManagerModal}>
            Add Station Manager
          </button>
          <button className="ghost" type="button" onClick={handleLogout}>
            Logout
          </button>
        </div>
      </header>

      {error && <p className="error">{error}</p>}

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
          <h3>Total Capacity (Lt)</h3>
          <p>{numberFmt.format(totals.capacity)}</p>
        </div>
        <div className="card">
          <h3>Total Usable (Lt)</h3>
          <p>{numberFmt.format(totals.usable)}</p>
        </div>
      </section>

      {loading ? (
        <section className="loading-state">
          <p>Loading dashboard data...</p>
        </section>
      ) : (
        <section className="grid">
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
                    Price In Lt
                    <input
                      type="number"
                      step="0.01"
                      value={sourceForm.price_in_lt}
                      onChange={(event) =>
                        updateSourceForm("price_in_lt", event.target.value)
                      }
                      placeholder="102.50"
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
                  <th>Price In Lt</th>
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
                      <td>{numberFmt.format(item.price_in_lt)}</td>
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
        </section>
      )}
      {confirmState.open && (
        <div className="modal-backdrop" role="presentation">
          <div className="modal" role="dialog" aria-modal="true">
            <h3 className="modal-title">
              {confirmState.type === "source" ? "Delete Source" : "Delete Station"}
            </h3>
            <p className="modal-sub">
              Are you sure you want to delete {confirmState.name}?
            </p>
            <div className="modal-actions">
              <button
                type="button"
                className="danger"
                onClick={confirmDelete}
                disabled={sourceSaving || stationSaving}
              >
                Delete
              </button>
              <button
                type="button"
                className="secondary"
                onClick={closeConfirm}
                disabled={sourceSaving || stationSaving}
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
                  Price In Lt
                  <input
                    type="number"
                    step="0.01"
                    value={sourceForm.price_in_lt}
                    onChange={(event) =>
                      updateSourceForm("price_in_lt", event.target.value)
                    }
                    placeholder="102.50"
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
      {managerOpen && (
        <div className="modal-backdrop" role="presentation">
          <div className="modal form-modal" role="dialog" aria-modal="true">
            <div className="modal-header">
              <h3 className="modal-title">Add Station Manager</h3>
              <button
                type="button"
                className="ghost"
                onClick={() => {
                  setManagerOpen(false);
                  resetManagerForm();
                  clearManagerMessages();
                }}
              >
                Close
              </button>
            </div>
            <form className="modal-form" onSubmit={handleManagerSubmit}>
              <div className="form-grid">
                <label>
                  Name
                  <input
                    value={managerForm.name}
                    onChange={(event) =>
                      updateManagerForm("name", event.target.value)
                    }
                    placeholder="Station Manager"
                    required
                  />
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
                  Create Manager
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => {
                    setManagerOpen(false);
                    resetManagerForm();
                    clearManagerMessages();
                  }}
                  disabled={managerSaving}
                >
                  Cancel
                </button>
              </div>
              {managerError && <p className="notice error">{managerError}</p>}
              {managerNotice && <p className="notice">{managerNotice}</p>}
            </form>
          </div>
        </div>
      )}
    </main>
  );
}

export default App;
