import mongoose from "mongoose";

const userSchema = new mongoose.Schema(
  {
    username: { type: String, required: true, trim: true },
    username_normalized: { type: String, required: true, unique: true, lowercase: true },
    name: { type: String, required: true, trim: true },
    type: {
      type: String,
      required: true,
      enum: ["admin", "station_manager"]
    },
    password_hash: { type: String, required: true },
    station: { type: String, trim: true }
  },
  {
    timestamps: true
  }
);

userSchema.pre("validate", function (next) {
  if (this.username) {
    this.username_normalized = String(this.username).trim().toLowerCase();
  }
  next();
});

export const User = mongoose.model("User", userSchema, "user");
