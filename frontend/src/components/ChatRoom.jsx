import { useState, useEffect, useRef } from 'react';
import { fetchCachedMessages } from '../api';
import { useSocket } from '../context/SocketContext';
import '../styles/chat.css';

export default function ChatRoom({ groupId, members, userid, onClose }) {
  const { sendMessage, onMessage, joinGroup } = useSocket();
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [joined, setJoined] = useState(false);
  const bottomRef = useRef(null);

  // Load cached messages and join the socket room
  useEffect(() => {
    const init = async () => {
      try {
        const r = await fetchCachedMessages(groupId);
        setMessages(r.data?.messages || []);
      } catch {
        setMessages([]);
      }
      joinGroup(userid, groupId, true);
      setJoined(true);
    };
    init();
  }, [groupId]);

  // Listen for incoming messages
  useEffect(() => {
    const cleanup = onMessage((msg) => {
      if (msg.groupid === groupId) {
        setMessages((prev) => [...prev, msg]);
      }
    });
    return cleanup;
  }, [groupId, onMessage]);

  // Auto-scroll to bottom
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSend = () => {
    const trimmed = input.trim();
    if (!trimmed || !joined) return;
    // Add the message to local state immediately so the sender sees it right away.
    // The server broadcasts to others via socket.to() (excludes sender), so no duplicate.
    const outgoing = { groupid: groupId, message: trimmed, user: userid };
    setMessages((prev) => [...prev, outgoing]);
    sendMessage(trimmed, groupId, userid);
    setInput('');
  };

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend(); }
  };

  return (
    <div className="chat-overlay">
      <div className="chat-room">
        <div className="chat-header">
          <div className="chat-info">
            <span className="chat-icon">💬</span>
            <div>
              <h4>Group Chat</h4>
              <p className="chat-id">{groupId}</p>
              {members?.length > 0 && (
                <p className="chat-members">Members: {members.join(', ')}</p>
              )}
            </div>
          </div>
          <button className="close-btn-sm" onClick={onClose}>✕</button>
        </div>

        <div className="chat-messages">
          {messages.length === 0 && (
            <div className="chat-empty">
              <p>No messages yet. Say hi! 👋</p>
            </div>
          )}
          {messages.map((msg, i) => (
            <div
              key={i}
              className={`message-row ${msg.user === userid ? 'mine' : 'theirs'}`}
            >
              {msg.user !== userid && (
                <span className="msg-avatar">{msg.user?.[0]?.toUpperCase()}</span>
              )}
              <div className="message-bubble">
                {msg.user !== userid && <span className="msg-user">{msg.user}</span>}
                <p className="msg-text">{msg.message}</p>
              </div>
            </div>
          ))}
          <div ref={bottomRef} />
        </div>

        <div className="chat-input-row">
          <textarea
            className="chat-input"
            placeholder="Type a message..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKey}
            rows={1}
          />
          <button className="send-btn" onClick={handleSend} disabled={!input.trim()}>
            ➤
          </button>
        </div>
      </div>
    </div>
  );
}
