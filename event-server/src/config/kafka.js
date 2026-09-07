const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'event-server',
  brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',')
});

const producer = kafka.producer();
let isProducerConnected = false;

async function connectProducer() {
  if (isProducerConnected) return;
  try {
    await producer.connect();
    isProducerConnected = true;
    console.log('✅ Kafka producer connected');
  } catch (err) {
    console.error('❌ Failed to connect Kafka producer:', err.message);
  }
}

module.exports = {
  kafka,
  producer,
  connectProducer
};
