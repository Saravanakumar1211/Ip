import mongoose from "mongoose";

export const connectDB = async () => {
  const mongoUri = process.env.MONGO_URI;
  const dbName = process.env.DB_NAME || "operations_optimization";

  if (!mongoUri) {
    throw new Error("MONGO_URI is missing in environment variables.");
  }

  await mongoose.connect(mongoUri, { dbName });
  console.log(`MongoDB connected. Database: ${dbName}`);
};
