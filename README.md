# Operations Optimization - Fuel Company

This project provides a basic full-stack setup for fuel operations optimization:
- `backend`: Node.js + Express + MongoDB (Mongoose)
- `frontend`: React (Vite)
- `database`: MongoDB collections `user`, `source`, `station`, and `truck`
- `data source`: Excel files in `pythonLogic/` (preferred) or `data/` (fallback)

## Collections and Schema

### `user` collection
- `username` (String, unique via normalized)
- `username_normalized` (String, unique)
- `name` (String)
- `type` (String: `admin` or `station_manager`)
- `password_hash` (String, bcrypt)

### `source` collection
Derived from `pythonLogic/sources.xlsx` (or `data/sources.xlsx` fallback):
- `source_id` (String, unique)
- `source_name` (String)
- `coordinates.lat` (Number)
- `coordinates.lng` (Number)
- `price_per_mt_ex_terminal` (Number)

### `station` collection
Derived from `pythonLogic/clean_stationss.xlsx` (or `data/clean_stationss.xlsx` fallback):
- `station` (String, unique)
- `coordinates.lat` (Number)
- `coordinates.lng` (Number)
- `capacity_in_lt` (Number)
- `dead_stock_in_lt` (Number)
- `usable_lt` (Number)
- `sufficient_fuel` (String: `YES` or `NO`)

### `truck` collection
Derived from `pythonLogic/truck_positions.json`:
- `truck_id` (String, unique)
- `station` (String)
- `lat` (Number)
- `lon` (Number)
- `type` (String)

## Prerequisites

- Node.js 18+
- npm
- MongoDB Atlas/local MongoDB URI in `.env`

Routing env knobs:
- `ROUTE_MAX_STOPS_PER_TRUCK`: max stations per truck run (default `2`).
- `ROUTE_MAX_GROUPING_KM`: grouping radius used when clustering stops (default `80`).
- `ROUTE_SPLIT_LOOKBACK_MONTHS`: sales split lookback window in months (`1` = previous month).
- `ROUTE_SOURCE_ASSIGNMENT_WORKERS`: parallel workers for source-cost assignment (default `4`).

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
- `pythonLogic/sources.xlsx` (fallback: `data/sources.xlsx`)
- `pythonLogic/clean_stationss.xlsx` (fallback: `data/clean_stationss.xlsx`)
- `pythonLogic/truck_positions.json`

## Route Planning (Python)

Install Python requirements:

```bash
pip install -r pythonLogic/requirements.txt
```

Optional environment variable for the python executable:

```env
PYTHON_PATH=python
```

The admin dashboard triggers the planner via `POST /api/route-plan`, which
writes suggested outputs into MongoDB collections:
- `delivery`
- `truckPlanning`
- `tentativeCost`

Planner script: `pythonLogic/route_plan_db.py`.

and upserts records into `source` and `station`.

## Run Backend

```bash
cd backend
npm run dev
```

Backend runs on `http://localhost:5000` by default.

API endpoints:
- `GET /health`
- `POST /api/auth/login`
- `POST /api/users/station-managers` (admin JWT required)
- `GET /api/sources` (admin JWT required)
- `GET /api/stations` (admin JWT required)
- `GET /api/trucks` (admin JWT required)
- `POST /api/import` (admin JWT required)
- `POST /api/route-plan` (admin JWT required)
- `GET /api/route-plan/latest` (admin JWT required)

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

## Authentication

The frontend uses JWT tokens for login. Users are stored in MongoDB with
hashed passwords. Data endpoints are admin-only. On backend start, an admin
user is created if one does not exist (using `ADMIN_EMAIL` and
`ADMIN_PASSWORD`).

Login example:

```json
POST /api/auth/login
{
  "username": "admin@krfuels.com",
  "password": "Admin@123"
}
```

Create station manager (admin only):

```json
POST /api/users/station-managers
{
  "name": "Station Manager",
  "username": "manager@krfuels.com",
  "password": "Manager@123",
  "station": "Anna Nagar"
}
```

Station manager demo accounts are auto-seeded for each station (with plaintext
passwords stored in the `user` collection for easy viewing in MongoDB).

## Notes

- The importer handles trailing spaces in Excel column names (`Source_ID ` and `Stations `).
- Coordinates are parsed from a comma-separated string into `{ lat, lng }`.
