const mongoose = require("mongoose");

const UserInterestSchema = new mongoose.Schema({
  _id: { type: String },
  userid: { type: String, required: true },
  short_term: { type: String },
  medium_term: { type: String },
  long_term: { type: String },
  top_category: { type: String },
  top_topic: { type: String },
  window_start: { type: Date },
  window_end: { type: Date }
}, {
  collection: "user_data",
  timestamps: false
});

module.exports = mongoose.model("UserInterest", UserInterestSchema);
