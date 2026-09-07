import { createContext, useContext, useEffect, useRef, useState } from 'react';
import { io } from 'socket.io-client';

const SocketContext = createContext(null);

export const SocketProvider = ({ children }) => {
  const socketRef = useRef(null);
  const [notifications, setNotifications] = useState([]);
  const [connected, setConnected] = useState(false);

  const initSocket = (userid) => {
    if (socketRef.current) return;

    const socket = io({
      transports: ['websocket', 'polling'],
    });

    socket.on('connect', () => {
      setConnected(true);
      socket.emit('register', { userid });
    });

    socket.on('disconnect', () => setConnected(false));

    socket.on('notification', (data) => {
      setNotifications((prev) => [
        { ...data, id: Date.now(), read: false },
        ...prev,
      ]);
    });

    socketRef.current = socket;
  };

  const joinGroup = (userid, groupid, hasGroup = false) => {
    socketRef.current?.emit('join', { userid, groupid, hasGroup });
  };

  const sendMessage = (message, groupid, user) => {
    socketRef.current?.emit('message', { message, groupid, user });
  };

  const onMessage = (handler) => {
    socketRef.current?.on('message', handler);
    return () => socketRef.current?.off('message', handler);
  };

  const markRead = (id) =>
    setNotifications((prev) =>
      prev.map((n) => (n.id === id ? { ...n, read: true } : n))
    );

  const disconnect = () => {
    socketRef.current?.disconnect();
    socketRef.current = null;
    setConnected(false);
  };

  return (
    <SocketContext.Provider
      value={{ connected, notifications, initSocket, joinGroup, sendMessage, onMessage, markRead, disconnect, socket: socketRef }}
    >
      {children}
    </SocketContext.Provider>
  );
};

export const useSocket = () => useContext(SocketContext);
