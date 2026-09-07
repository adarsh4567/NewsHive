const axios = require('axios');

const MARKETAUX_API_KEY = 'api_live_Nflbtv4UQKq5LD8N4LFhJvRuzqzBPqlNCTk6D4TL';
const MARKETAUX_BASE_URL = 'https://api.apitube.io';

const marketauxApi = axios.create({
  baseURL: MARKETAUX_BASE_URL,
  timeout: 10000,
  headers: {
    'Authorization': `Bearer ${MARKETAUX_API_KEY}`,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  }
});

module.exports = {
  marketauxApi,
  MARKETAUX_API_KEY
};
