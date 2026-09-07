const http = require('http');
const { Server } = require("socket.io");
const app = require('./app');

// Configs
const { connectMongo } = require('./config/database');
const { connectRedis } = require('./config/redis');
const { connectProducer } = require('./config/kafka');
const { MARKETAUX_API_KEY } = require('./services/marketaux.service');

// Consumers & Workers
const startPairConsumer = require('./consumers/pair.consumer');
const startGroupConsumer = require('./consumers/group.consumer');
const startCdcConsumer = require('./consumers/cdc.consumer');
const startIngestionWorker = require('./workers/ingestion.worker');

// Socket handlers
const handleSocketConnection = require('./socket/socket.handler');

const PORT = process.env.PORT || 8000;

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Initialize Socket.io
handleSocketConnection(io);

server.listen(PORT, async () => {
  console.log(`🚀 Server running on port ${PORT}`);

  // Initialize Databases and Messaging
  await connectMongo();
  await connectRedis();
  await connectProducer();

  // Initialize Kafka Consumers
  await startPairConsumer(io);
  await startGroupConsumer(io);
  await startCdcConsumer();

  // Start Background Workers
  startIngestionWorker();

  // Warning if API key not set
  if (!MARKETAUX_API_KEY) {
    console.log('⚠️  WARNING: API key not set. Please add your MarketAux API key.');
  }
});
