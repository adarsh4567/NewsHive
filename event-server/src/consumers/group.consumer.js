const { kafka } = require('../config/kafka');
const axios = require('axios');

const consumer = kafka.consumer({ groupId: 'group-pair-consumer' });
const CHAT_SERVER_URL = process.env.CHAT_SERVER_URL || 'http://chat-server:5000';

async function startGroupConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'output-group-signals', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const raw_data = message.value.toString();
        const parsed = JSON.parse(raw_data);
        const member = parsed["userid"];
        const group_name = parsed["group"];

        // We only know one member in this event, but the chat-server will upsert and add them.
        // Wait, the API needs to add participants to a set. Let's send it to chat-server.
        await axios.post(`${CHAT_SERVER_URL}/api/conversations`, {
          conversationId: group_name,
          type: 'group',
          participants: [member] // Chat server should technically handle $addToSet in MongoDB, we'll need to update that if needed
        });

        console.log(`[Event-Server] Handoff group match to Chat Server: User ${member} -> ${group_name}`);
      } catch (err) {
        console.error('[Event-Server] Failed to process group match:', err.message);
      }
    },
  });
}

module.exports = startGroupConsumer;
