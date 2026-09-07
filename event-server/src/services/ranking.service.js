const UserInterest = require('../models/userInterest.model');
const NewsArticle = require('../models/newsArticle.model');
const { client: redisClient } = require('../config/redis');

/**
 * Ranks all available news against a user's specific profile and 
 * materializes the resulting feed into a Redis Sorted Set (ZSET).
 */
async function generateFeed(userid) {
  try {
    console.log(`[Ranking Engine] Generating feed for user: ${userid}`);
    
    // 1. Fetch User Profile
    const userProfile = await UserInterest.findOne({ userid });
    if (!userProfile) {
      console.log(`[Ranking Engine] No profile found for ${userid}`);
      return;
    }

    const shortInterest = JSON.parse(userProfile.short_term || '{}');
    const mediumInterest = JSON.parse(userProfile.medium_term || '{}');
    const longInterest = JSON.parse(userProfile.long_term || '{}');

    // Extract sets for O(1) lookups
    const userCategories = new Set([
      ...(shortInterest.categories || []).map(c => c.category_id),
      ...(mediumInterest.categories || []).map(c => c.category_id),
      ...(longInterest.categories || []).map(c => c.category_id)
    ]);
    
    const userTopics = new Set([
      ...(shortInterest.topics || []).map(t => t.topic_id),
      ...(mediumInterest.topics || []).map(t => t.topic_id),
      ...(longInterest.topics || []).map(t => t.topic_id)
    ]);

    const userEntities = new Set([
      ...(shortInterest.entities || []),
      ...(mediumInterest.entities || []),
      ...(longInterest.entities || [])
    ]);

    // 2. Fetch Content Pool (Limit to 1000 recent articles for performance)
    const recentArticles = await NewsArticle.find()
      .sort({ published_at: -1 })
      .limit(1000)
      .lean();

    // 3. Score Articles
    const scoredArticles = recentArticles.map(article => {
      let score = 0;

      // Category match (Heavy weight)
      if (article.categories) {
        article.categories.forEach(cat => {
          if (userCategories.has(cat)) score += 5;
        });
      }

      // Topic match (Medium weight)
      if (article.topics) {
        article.topics.forEach(topic => {
          if (userTopics.has(topic)) score += 3;
        });
      }

      // Entity match (Lighter weight, but highly specific)
      if (article.entities) {
        article.entities.forEach(entity => {
          if (userEntities.has(entity.symbol)) score += 2;
        });
      }

      // Freshness Penalty: Older news decays in score
      const hoursOld = (Date.now() - new Date(article.published_at).getTime()) / (1000 * 60 * 60);
      score = score * Math.exp(-0.01 * hoursOld); // Exponential decay

      // Random jitter to prevent identical feeds on identical profiles
      score += Math.random() * 0.1;

      return { uuid: article.uuid, score };
    });

    // Sort by score descending and take Top 500
    scoredArticles.sort((a, b) => b.score - a.score);
    const topArticles = scoredArticles.slice(0, 500);

    if (topArticles.length === 0) return;

    // 4. Materialize into Redis ZSET
    const feedKey = `feed:${userid}`;
    
    // Clear old feed
    await redisClient.del(feedKey);

    // Push new scored items
    const zaddArgs = [];
    topArticles.forEach(item => {
      zaddArgs.push({ score: item.score, value: item.uuid });
    });

    await redisClient.zAdd(feedKey, zaddArgs);
    
    // Set expiry (e.g., feed expires in 24 hours if not touched)
    await redisClient.expire(feedKey, 24 * 60 * 60);

    console.log(`[Ranking Engine] Successfully materialized ${topArticles.length} items for ${userid}`);
  } catch (err) {
    console.error(`[Ranking Engine] Error generating feed for ${userid}:`, err);
  }
}

module.exports = {
  generateFeed
};
