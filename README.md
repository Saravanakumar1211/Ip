# Operations Optimization - Fuel Company

This project provides a basic full-stack setup for fuel operations optimization:
- `backend`: Node.js + Express + MongoDB (Mongoose)
- `frontend`: React (Vite)
- `database`: MongoDB collections `source` and `station`
- `data source`: Excel files in `data/`

## Collections and Schema

### `source` collection
Derived from `data/sources.xlsx`:
- `source_id` (String, unique)
- `source_name` (String)
- `coordinates.lat` (Number)
- `coordinates.lng` (Number)
- `price_in_lt` (Number)

### `station` collection
Derived from `data/clean_stationss.xlsx`:
- `station` (String, unique)
- `coordinates.lat` (Number)
- `coordinates.lng` (Number)
- `capacity_in_lt` (Number)
- `dead_stock_in_lt` (Number)
- `usable_lt` (Number)

## Prerequisites

- Node.js 18+
- npm
- MongoDB Atlas/local MongoDB URI in `.env`

## Environment

Root `.env` is already used:

```env
MONGO_URI="your-mongodb-connection-string"
DB_NAME="operations_optimization"
PORT=5000
```

`DB_NAME` and `PORT` are optional.

## Setup

Install backend dependencies:

```bash
cd backend
npm install
```

Install frontend dependencies:

```bash
cd frontend
npm install
```

## Import Excel Data into MongoDB

From `backend` folder:

```bash
npm run import-data
```

This reads:
- `data/sources.xlsx`
- `data/clean_stationss.xlsx`

and upserts records into `source` and `station`.

## Run Backend

```bash
cd backend
npm run dev
```

Backend runs on `http://localhost:5000` by default.

API endpoints:
- `GET /health`
- `GET /api/sources`
- `GET /api/stations`
- `POST /api/import`

## Run Frontend

```bash
cd frontend
npm run dev
```

Frontend runs on Vite default port (`http://localhost:5173`).

Optional API URL override:

```env
VITE_API_URL=http://localhost:5000/api
```

## Notes

- The importer handles trailing spaces in Excel column names (`Source_ID ` and `Stations `).
- Coordinates are parsed from a comma-separated string into `{ lat, lng }`.
