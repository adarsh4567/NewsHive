const { rooms, usersWithSockets, userWithGroups } = require('./state');
const { client: redisClient } = require('../config/redis');

function handleSocketConnection(io) {
  io.on('connection', (socket) => {
    console.log('A user connected:', socket.id);

    // Register user
    socket.on("register", async (data) => {
      const { userid } = data;
      const groups = userWithGroups.get(userid);
      
      if (groups) {
        groups.forEach((item) => {
          socket.join(item);
          console.log("group_register done of user", userid);
        });
      }
      
      usersWithSockets.set(userid, socket.id);
      console.log("register done of user", userid);
    });

    // Join group/room
    socket.on("join", async (data) => {
      const { userid, groupid, hasGroup } = data;

      if (!rooms.has(groupid)) {
        rooms.set(groupid, []);
      }

      if (!userWithGroups.has(userid)) {
        userWithGroups.set(userid, []);
      }

      const existingUsers = rooms.get(groupid);
      console.log(`[join request] user: ${userid}, group: ${groupid}, hasGroup: ${hasGroup}`);

      if (existingUsers.includes(userid)) {
        console.log(`[join request] user ${userid} already in existingUsers. Returning early.`);
        return;
      }

      if (hasGroup) {
        socket.join(groupid);
        existingUsers.push(userid);
        rooms.set(groupid, existingUsers);
        usersWithSockets.set(userid, socket.id);

        const existingUserGroups = userWithGroups.get(userid);
        existingUserGroups.push(groupid);
        userWithGroups.set(userid, existingUserGroups);
        return;
      }

      if (existingUsers.length === 0) {
        socket.join(groupid);
        await redisClient.HSET(`groups:${groupid}`, { active: 'true' });
        
        rooms.set(groupid, [userid]);
        usersWithSockets.set(userid, socket.id);

        const existingUserGroups = userWithGroups.get(userid);
        existingUserGroups.push(groupid);
        userWithGroups.set(userid, existingUserGroups);

      } else {
        existingUsers.forEach(async (item) => {
          await redisClient.RPUSH(item, userid);
          await redisClient.RPUSH(userid, item);
        });

        existingUsers.push(userid);
        rooms.set(groupid, existingUsers);
        socket.join(groupid);
        usersWithSockets.set(userid, socket.id);

        const existingUserGroups = userWithGroups.get(userid);
        existingUserGroups.push(groupid);
        userWithGroups.set(userid, existingUserGroups);
      }
    });

    // Handle messages
    socket.on("message", async (msg) => {
      const { message, groupid, user } = msg;
      const redis_msg = { groupid, message, user };
      console.log(`[message received] from user: ${user} in group: ${groupid}`);
      await redisClient.rPush(groupid, JSON.stringify(redis_msg));
      socket.to(groupid).emit("message", { groupid, message, user });
      console.log(`[message broadcasted] to group: ${groupid} (excluding sender)`);
    });

    // Handle disconnection
    socket.on('disconnect', () => {
      let removedUser = null;
      for (const [uid, sid] of usersWithSockets.entries()) {
        if (sid === socket.id) {
          removedUser = uid;
          break;
        }
      }
      if (removedUser) {
        usersWithSockets.delete(removedUser);
        console.log('Removed user on disconnect:', removedUser);
      }
      for (const [key, valueArray] of rooms.entries()) {
        const updatedArray = valueArray.filter(item => item !== removedUser);
        rooms.set(key, updatedArray);
      }
    });

    // Handle errors
    socket.on("error", () => {
      let removedUser = null;
      for (const [uid, sid] of usersWithSockets.entries()) {
        if (sid === socket.id) {
          removedUser = uid;
          break;
        }
      }
      if (removedUser) usersWithSockets.delete(removedUser);
      for (const [key, valueArray] of rooms.entries()) {
        const updatedArray = valueArray.filter(item => item !== removedUser);
        rooms.set(key, updatedArray);
      }
    });
  });
}

module.exports = handleSocketConnection;
