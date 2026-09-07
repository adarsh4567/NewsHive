const { createClient } = require('redis');

const client = createClient({
  socket: {
    host: process.env.REDIS_HOST || 'redis',
    port: parseInt(process.env.REDIS_PORT || '6379'),
  }
});

client.on('error', (err) => console.error('Redis Client Error', err));

async function connectRedis() {
  try {
    await client.connect();
    console.log('✅ Redis connected');
  } catch (err) {
    console.error('❌ Failed to connect Redis:', err.message);
  }
}

module.exports = {
  client,
  connectRedis
};
