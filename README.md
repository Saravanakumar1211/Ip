# Operations Optimization - Fuel Company

This project provides a basic full-stack setup for fuel operations optimization:
- `backend`: Node.js + Express + MongoDB (Mongoose)
- `frontend`: React (Vite)
- `database`: MongoDB collections `user`, `source`, and `station`
- `data source`: Excel files in `data/`

## Collections and Schema

### `user` collection
- `username` (String, unique via normalized)
- `username_normalized` (String, unique)
- `name` (String)
- `type` (String: `admin` or `station_manager`)
- `password_hash` (String, bcrypt)

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
JWT_SECRET="change-this-secret"
ADMIN_EMAIL="admin@krfuels.com"
ADMIN_PASSWORD="Admin@123"
ADMIN_NAME="Admin"
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
- `POST /api/auth/login`
- `POST /api/users/station-managers` (admin JWT required)
- `GET /api/sources` (admin JWT required)
- `GET /api/stations` (admin JWT required)
- `POST /api/import` (admin JWT required)

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
  "password": "Manager@123"
}
```

## Notes

- The importer handles trailing spaces in Excel column names (`Source_ID ` and `Stations `).
- Coordinates are parsed from a comma-separated string into `{ lat, lng }`.
