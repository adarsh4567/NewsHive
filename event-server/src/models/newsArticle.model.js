const mongoose = require("mongoose");

const NewsArticleSchema = new mongoose.Schema({
  uuid: { type: String, required: true, unique: true },
  title: { type: String, required: true },
  description: { type: String },
  keywords: { type: String },
  snippet: { type: String },
  url: { type: String, required: true },
  image_url: { type: String },
  language: { type: String },
  published_at: { type: Date, required: true },
  source: { type: String },
  categories: [{ type: String }],
  topics: [{ type: String }],
  entities: [{
    symbol: String,
    name: String,
    exchange: String,
    type: String,
    industry: String,
    match_score: Number,
    sentiment_score: Number,
  }],
  ingested_at: { type: Date, default: Date.now }
}, {
  collection: "news_articles",
  timestamps: false
});

// Index for fast text/metadata matching during ranking
NewsArticleSchema.index({ categories: 1 });
NewsArticleSchema.index({ topics: 1 });
NewsArticleSchema.index({ "entities.symbol": 1 });
NewsArticleSchema.index({ published_at: -1 }); // Fast chronological queries

module.exports = mongoose.model("NewsArticle", NewsArticleSchema);
