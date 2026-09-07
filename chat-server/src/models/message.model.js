const mongoose = require('mongoose');

const MessageSchema = new mongoose.Schema({
  conversationId: { type: String, required: true, index: true },
  senderId: { type: String, required: true },
  message: { type: String, required: true },
  messageType: { type: String, enum: ['text', 'image', 'system'], default: 'text' },
  timestamp: { type: Date, default: Date.now, index: true }
});

// Index to query paginated messages within a conversation quickly
MessageSchema.index({ conversationId: 1, timestamp: -1 });

module.exports = mongoose.model('Message', MessageSchema);
