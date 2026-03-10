import { useEffect, useMemo, useState } from "react";
import axios from "axios";

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:5000/api";

const numberFmt = new Intl.NumberFormat("en-IN");

function App() {
  const [sources, setSources] = useState([]);
  const [stations, setStations] = useState([]);
  const [loading, setLoading] = useState(false);
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState("");

  const totals = useMemo(() => {
    const capacity = stations.reduce((sum, item) => sum + (item.capacity_in_lt || 0), 0);
    const usable = stations.reduce((sum, item) => sum + (item.usable_lt || 0), 0);
    return { capacity, usable };
  }, [stations]);

  const fetchData = async () => {
    setLoading(true);
    setError("");
    try {
      const [sourcesRes, stationsRes] = await Promise.all([
        axios.get(`${API_BASE}/sources`),
        axios.get(`${API_BASE}/stations`)
      ]);
      setSources(sourcesRes.data);
      setStations(stationsRes.data);
    } catch (fetchError) {
      setError(fetchError.response?.data?.message || "Unable to load data.");
    } finally {
      setLoading(false);
    }
  };

  const importData = async () => {
    setImporting(true);
    setError("");
    try {
      await axios.post(`${API_BASE}/import`);
      await fetchData();
    } catch (importError) {
      setError(importError.response?.data?.message || "Unable to import data.");
    } finally {
      setImporting(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  return (
    <main className="page">
      <header className="hero">
        <h1>Fuel Operations Optimization</h1>
        <p>Manage source pricing and station capacity with MongoDB-backed data.</p>
        <div className="actions">
          <button onClick={importData} disabled={importing}>
            {importing ? "Importing..." : "Import Excel Data"}
          </button>
          <button onClick={fetchData} disabled={loading}>
            {loading ? "Refreshing..." : "Refresh"}
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

      <section className="grid">
        <article className="table-wrap">
          <h2>Source Collection</h2>
          <table>
            <thead>
              <tr>
                <th>source_id</th>
                <th>source_name</th>
                <th>coordinates</th>
                <th>price_in_lt</th>
              </tr>
            </thead>
            <tbody>
              {sources.map((item) => (
                <tr key={item._id}>
                  <td>{item.source_id}</td>
                  <td>{item.source_name}</td>
                  <td>
                    {item.coordinates?.lat}, {item.coordinates?.lng}
                  </td>
                  <td>{item.price_in_lt}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="table-wrap">
          <h2>Station Collection</h2>
          <table>
            <thead>
              <tr>
                <th>station</th>
                <th>coordinates</th>
                <th>capacity_in_lt</th>
                <th>dead_stock_in_lt</th>
                <th>usable_lt</th>
              </tr>
            </thead>
            <tbody>
              {stations.map((item) => (
                <tr key={item._id}>
                  <td>{item.station}</td>
                  <td>
                    {item.coordinates?.lat}, {item.coordinates?.lng}
                  </td>
                  <td>{item.capacity_in_lt}</td>
                  <td>{item.dead_stock_in_lt}</td>
                  <td>{item.usable_lt}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>
      </section>
    </main>
  );
}

export default App;
