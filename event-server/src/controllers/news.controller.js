const { marketauxApi, MARKETAUX_API_KEY } = require('../services/marketaux.service');
const UserInterest = require('../models/userInterest.model');
const { producer } = require('../config/kafka');
const { client: redisClient } = require('../config/redis');

let producerReady = true; // Assumes producer connects on boot

exports.getIndiaNews = async (req, res) => {
  try {
    const response = await marketauxApi.get('/v1/news/everything', {
      params: {
        countries: 'in',
        limit: 10,
        page: req.query.page || 1
      }
    });
    res.json(response.data);
  } catch (error) {
    res.status(500).json({ error: 'API Error', message: error.message });
  }
};

const NewsArticle = require('../models/newsArticle.model');
const rankingService = require('../services/ranking.service');

exports.getInfiniteFeed = async (req, res) => {
  try {
    const userid = req.query.userid;
    if (!userid) {
      return res.status(400).json({ error: "Missing userid parameter" });
    }

    const pageNo = parseInt(req.query.pageNo || "0", 10);
    const limit = 10;
    const start = pageNo * limit;
    const end = start + limit - 1;

    const feedKey = `feed:${userid}`;
    
    // 1. Fetch ranked news IDs from Redis
    let feedIds = await redisClient.zRange(feedKey, start, end, { REV: true });

    // 2. Fallback: If feed is empty, trigger generation and return generic recent news for now
    if (!feedIds || feedIds.length === 0) {
      console.log(`[API] Cache miss for ${userid}. Triggering background generation.`);
      // Fire and forget
      rankingService.generateFeed(userid).catch(err => console.error(err));
      
      // Fallback to recent generic news
      const fallbackNews = await NewsArticle.find()
        .sort({ published_at: -1 })
        .skip(start)
        .limit(limit)
        .lean();
      return res.json({ results: fallbackNews, isFallback: true });
    }

    // 3. Fetch full articles from MongoDB
    const articles = await NewsArticle.find({ uuid: { $in: feedIds } }).lean();

    // 4. Sort articles to match the exact ranking order from Redis
    const articleMap = {};
    articles.forEach(a => articleMap[a.uuid] = a);
    
    const sortedResults = feedIds.map(id => articleMap[id]).filter(Boolean);

    return res.json({ results: sortedResults });
  } catch (err) {
    console.error('infinite route error:', err.message);
    return res.status(500).json({ error: 'internal', message: err.message });
  }
};

exports.publishNewsEvent = async (req, res) => {
  const { userid, group, newsid, top_category, top_topic, categories, entities, topics, dwell } = req.body;
  
  if (!userid || !newsid || !top_category || !top_topic) {
    return res.status(400).json({
      error: 'Missing fields',
      message: 'userid, newsid, top_category, top_topic are required'
    });
  }

  try {
    const interests = [
      ...(Array.isArray(categories) ? categories.map(String) : []),
      ...(Array.isArray(topics) ? topics.map(String) : []),
      ...(Array.isArray(entities) ? entities.map(String) : [])
    ];

    const message = { userid, newsid, top_category, top_topic, categories, topics, entities, dwell, interests };
    
    await producer.send({
      topic: 'user-events',
      messages: [{ value: JSON.stringify(message) }]
    });
    
    res.json({ success: true, data: message });
  } catch (err) {
    res.status(500).json({ error: 'Kafka publish failed', message: err.message });
  }
};

exports.getSimilarNews = async (req, res) => {
  const { newsid } = req.query;
  if (!newsid) {
    return res.status(400).json({ error: 'Missing parameters' });
  }
  try {
    const response = await axios.get(`https://api.marketaux.com/v1/news/similar/${newsid}`, {
      params: { api_token: MARKETAUX_API_KEY }
    });
    res.json(response.data);
  } catch (error) {
    res.status(500).json({ error: 'API Error', message: error.message });
  }
};

exports.cacheMessage = async (req, res) => {
  const { groupId } = req.body;
  const messages = await redisClient.lRange(groupId, 0, -1);
  
  if (messages !== null) {
    const parsedMessages = messages.map(item => JSON.parse(item));
    return res.json({ messages: parsedMessages });
  }
  res.json({ messages: [] });
};

// Phase 1 (On-Demand AI): Proxy the streaming response from financial-agent
exports.analyzeNews = async (req, res) => {
  const { newsid } = req.params;
  const { userid } = req.body; // For credit deduction

  try {
    // 1. Validate Credits / Subscription
    // (Pretend we do a User.findOne({ userid }) and decrement credits here)

    // 2. Fetch the actual news text from our DB or MarketAux
    const article = await NewsArticle.findOne({ uuid: newsid });
    if (!article) {
      return res.status(404).json({ error: 'Article not found' });
    }

    // 3. Proxy to FastAPI Streaming Endpoint
    const FINANCIAL_AGENT_URL = process.env.FINANCIAL_AGENT_URL || 'http://financial-agent:8000';
    
    // We use a raw HTTP request to pipe the SSE stream directly to the client
    const axiosResponse = await axios({
      method: 'post',
      url: `${FINANCIAL_AGENT_URL}/analyze`,
      data: { news: article.description || article.title },
      responseType: 'stream'
    });

    // Set SSE headers
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    // Pipe the python generator stream directly to the Node.js response
    axiosResponse.data.pipe(res);

  } catch (error) {
    console.error('[API] Error requesting AI Analysis:', error.message);
    res.status(500).json({ error: 'Failed to stream AI analysis' });
  }
};
