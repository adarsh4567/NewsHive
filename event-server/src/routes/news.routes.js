const express = require('express');
const router = express.Router();
const newsController = require('../controllers/news.controller');

router.get('/', (req, res) => {
  res.json({
    message: 'Indian News API Server',
    version: '1.0.0',
    endpoints: {
      news: '/api/news/india',
      health: '/health',
      publish: '/api/publish',
      infinite: '/infinite',
      cache: '/cacheMessage'
    }
  });
});

router.get('/health', (req, res) => res.status(200).json({ status: 'ok' }));

router.get('/api/news/india', newsController.getIndiaNews);
router.get('/infinite', newsController.getInfiniteFeed);
router.post('/api/publish', newsController.publishNewsEvent);
router.get('/api/news/similar', newsController.getSimilarNews);
router.post('/cacheMessage', newsController.cacheMessage);
router.post('/:newsid/analyze', newsController.analyzeNews);

module.exports = router;
