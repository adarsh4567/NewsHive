const mongoose = require('mongoose');

const MONGODB_URI = process.env.MONGODB_URI || "mongodb://admin:password@mongodb:27017/flinkdb?authSource=admin";

let isConnected = false;

async function connectMongo() {
  if (isConnected) return;
  try {
    mongoose.set('strictQuery', false);
    await mongoose.connect(MONGODB_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    isConnected = true;
    console.log('✅ MongoDB connected');
  } catch (err) {
    console.error('❌ Failed to connect MongoDB:', err.message);
    process.exit(1);
  }
}

module.exports = {
  connectMongo
};
