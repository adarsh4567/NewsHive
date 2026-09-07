const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { createClient } = require("redis");
const { createAdapter } = require("@socket.io/redis-adapter");
const cors = require('cors');

const { connectMongo } = require('./config/database');
const Message = require('./models/message.model');
const Conversation = require('./models/conversation.model');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const PORT = process.env.PORT || 5000;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';

// Redis clients for Pub/Sub (Socket.IO Adapter)
const pubClient = createClient({ url: `redis://${REDIS_HOST}:${REDIS_PORT}` });
const subClient = pubClient.duplicate();

// Redis client for Presence and Caching
const redisClient = createClient({ url: `redis://${REDIS_HOST}:${REDIS_PORT}` });

// ─── REST API FOR PAGINATED CHAT HISTORY ────────────────────────────
app.get('/api/chat/:conversationId/history', async (req, res) => {
  try {
    const { conversationId } = req.params;
    const { beforeCursor, limit = 50 } = req.query;

    let query = { conversationId };
    if (beforeCursor) {
      query.timestamp = { $lt: new Date(beforeCursor) };
    }

    const messages = await Message.find(query)
      .sort({ timestamp: -1 })
      .limit(Number(limit))
      .lean();

    res.json({ messages: messages.reverse() });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch history' });
  }
});

// ─── REST API FOR CONVERSATION INBOX & MATCH HANDOFF ────────────────
app.post('/api/conversations', async (req, res) => {
  try {
    const { conversationId, type, participants } = req.body;
    
    // Upsert conversation and add participants
    const conversation = await Conversation.findOneAndUpdate(
      { conversationId },
      { 
        $setOnInsert: { conversationId, type },
        $addToSet: { participants: { $each: participants } }
      },
      { upsert: true, new: true }
    );
    
    // Notify online users about the new match via Pub/Sub
    participants.forEach(userid => {
      io.to(userid).emit('new_conversation_match', conversation);
    });

    res.status(201).json(conversation);
  } catch (err) {
    res.status(500).json({ error: 'Failed to create conversation' });
  }
});

app.get('/api/conversations/user/:userid', async (req, res) => {
  try {
    const { userid } = req.params;
    const conversations = await Conversation.find({ participants: userid })
      .sort({ updatedAt: -1 })
      .lean();
    res.json(conversations);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch conversations' });
  }
});

async function setup() {
  await connectMongo();
  await Promise.all([
    pubClient.connect(),
    subClient.connect(),
    redisClient.connect()
  ]);

  // Setup Redis Adapter for horizontal scaling
  io.adapter(createAdapter(pubClient, subClient));

  io.on('connection', (socket) => {
    console.log(`[Socket] User connected: ${socket.id}`);
    
    // Heartbeat logic for presence
    socket.on('heartbeat', async (data) => {
      const userid = data.userid;
      if (userid) {
        // Set presence with a 30 second TTL
        await redisClient.set(`presence:${userid}`, 'online', { EX: 30 });
      }
    });

    socket.on('join_conversation', (conversationId) => {
      socket.join(conversationId);
      console.log(`[Socket] ${socket.id} joined conversation: ${conversationId}`);
    });

    socket.on('send_message', async (data) => {
      const { conversationId, userid, message } = data;
      console.log(`[Socket] Message from ${userid} in ${conversationId}: ${message}`);
      
      const timestamp = new Date();

      // Emit to everyone in the room across ALL servers via Redis Adapter
      const messagePayload = {
        conversationId,
        userid,
        message,
        timestamp: timestamp.toISOString()
      };

      io.to(conversationId).emit('new_message', messagePayload);
      
      // Phase 2: Save this message to MongoDB and Redis cache (async fire & forget)
      Message.create({
        conversationId,
        senderId: userid,
        message,
        timestamp
      }).catch(err => console.error('DB Save error:', err));

      // Update Hot Cache (Keep only last 50)
      const cacheKey = `chat_cache:${conversationId}`;
      redisClient.rPush(cacheKey, JSON.stringify(messagePayload)).then(() => {
        redisClient.lTrim(cacheKey, -50, -1);
      }).catch(err => console.error('Cache error:', err));
    });

    socket.on('disconnect', () => {
      console.log(`[Socket] User disconnected: ${socket.id}`);
      // The heartbeat TTL naturally expires, so no DB writes needed on sudden disconnect
    });
  });

  server.listen(PORT, () => {
    console.log(`🚀 Chat Server (Stateless) running on port ${PORT}`);
  });
}

setup().catch(err => console.error(err));
