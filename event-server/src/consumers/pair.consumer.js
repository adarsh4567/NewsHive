const { kafka } = require('../config/kafka');
const axios = require('axios');

// This consumer handles pairs coming from 'output-signal' (Flink LSH Job)
const consumer = kafka.consumer({ groupId: 'pair-consumer-group' });

const CHAT_SERVER_URL = process.env.CHAT_SERVER_URL || 'http://chat-server:5000';

async function startPairConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'output-signal', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const raw_data = message.value.toString();
        const parsed = JSON.parse(raw_data);
        const first_mem = parsed["grouped"][0];
        const second_mem = parsed["grouped"][1];

        const group_name = `${first_mem}_${second_mem}`;
        
        // Handoff to Chat Server to create the Conversation
        await axios.post(`${CHAT_SERVER_URL}/api/conversations`, {
          conversationId: group_name,
          type: 'one_to_one',
          participants: [first_mem, second_mem]
        });

        console.log(`[Event-Server] Handoff pair match to Chat Server: ${first_mem} <-> ${second_mem}`);
      } catch (err) {
        console.error('[Event-Server] Failed to process pair match:', err.message);
      }
    },
  });
}

module.exports = startPairConsumer;
