// In-memory data structures (Socket.IO state)
const rooms = new Map();
const usersWithSockets = new Map();
const userWithGroups = new Map();

module.exports = {
  rooms,
  usersWithSockets,
  userWithGroups
};
