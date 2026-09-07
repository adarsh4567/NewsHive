const { kafka } = require('../config/kafka');
const { client: redisClient } = require('../config/redis');
const rankingService = require('../services/ranking.service');

// CDC Consumer - listens for MongoDB updates via Debezium
const cdc_consumer = kafka.consumer({ groupId: 'cdc-mongo-cache-invalidator' });

async function startCdcConsumer() {
  await cdc_consumer.connect();
  await cdc_consumer.subscribe({ topic: 'mongo.flinkdb.user_data', fromBeginning: false });

  await cdc_consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const raw_data = message.value.toString();
        const parsed = JSON.parse(raw_data);
        
        // Debezium wraps the data in 'payload.after' for inserts/updates
        const payload = parsed.payload;
        if (!payload || !payload.after) return;
        
        const userid = payload.after.userid;
        
        console.log(`[CDC] Detected update for user ${userid}`);
        
        // Phase 3: Trigger feed ranking in the background
        // We don't await this so it doesn't block the Kafka consumer
        rankingService.generateFeed(userid).catch(err => {
          console.error(`[CDC] Failed to generate feed for ${userid}:`, err);
        });
        
      } catch (err) {
        console.error("CDC parse error:", err);
      }
    },
  });
}

module.exports = startCdcConsumer;
