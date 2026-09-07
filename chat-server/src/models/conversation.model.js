const mongoose = require('mongoose');

const ConversationSchema = new mongoose.Schema({
  conversationId: { type: String, required: true, unique: true },
  type: { type: String, enum: ['one_to_one', 'group'], required: true },
  participants: [{ type: String }], // Array of user IDs
  lastMessage: {
    message: String,
    senderId: String,
    timestamp: Date
  },
  unreadCounts: {
    type: Map,
    of: Number,
    default: {}
  },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

module.exports = mongoose.model('Conversation', ConversationSchema);
