const { marketauxApi } = require('../services/marketaux.service');
const NewsArticle = require('../models/newsArticle.model');

/**
 * Ingestion Worker
 * Fetches recent news globally to populate the Content Pool.
 */
async function ingestNews() {
  console.log('[Ingestion Worker] Starting periodic news ingestion...');
  try {
    const response = await marketauxApi.get('/v1/news/everything', {
      params: {
        countries: 'in,us',
        limit: 50,
        language: 'en'
      }
    });

    const articles = response.data.results || [];
    let newArticlesCount = 0;

    for (const article of articles) {
      // Map entities strictly
      const entities = (article.entities || []).map(e => ({
        symbol: e.symbol,
        name: e.name,
        exchange: e.exchange,
        type: e.type,
        industry: e.industry,
        match_score: e.match_score,
        sentiment_score: e.sentiment_score
      }));

      // Upsert based on UUID
      const result = await NewsArticle.updateOne(
        { uuid: article.uuid },
        {
          $setOnInsert: {
            uuid: article.uuid,
            title: article.title,
            description: article.description,
            keywords: article.keywords,
            snippet: article.snippet,
            url: article.url,
            image_url: article.image_url,
            language: article.language,
            published_at: new Date(article.published_at),
            source: article.source,
            categories: article.categories || [],
            topics: article.topics || [],
            entities: entities
          }
        },
        { upsert: true }
      );

      if (result.upsertedCount > 0) {
        newArticlesCount++;
      }
    }

    console.log(`[Ingestion Worker] Ingested ${newArticlesCount} new articles.`);
  } catch (error) {
    console.error('[Ingestion Worker] Failed to ingest news:', error.message);
  }
}

function startIngestionWorker(intervalMs = 60 * 60 * 1000) { // Default: Every hour
  // Run immediately on boot
  ingestNews();
  // Schedule periodic runs
  setInterval(ingestNews, intervalMs);
}

module.exports = startIngestionWorker;
