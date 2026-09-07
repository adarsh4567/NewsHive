import { useSocket } from '../context/SocketContext';
import '../styles/notifications.css';

export default function NotificationPanel({ onClose, onOpenChat, userid }) {
  const { notifications, markRead, joinGroup } = useSocket();

  const handleAccept = (notification) => {
    markRead(notification.id);
    joinGroup(userid, notification.groupId, true);
    onOpenChat(notification);
  };

  return (
    <div className="notif-panel">
      <div className="notif-header">
        <h3>🔔 Notifications</h3>
        <button onClick={onClose} className="close-btn-sm">✕</button>
      </div>

      {notifications.length === 0 ? (
        <div className="notif-empty">
          <p>No notifications yet</p>
          <span>You'll be notified when you're matched with another reader</span>
        </div>
      ) : (
        <div className="notif-list">
          {notifications.map((n) => (
            <div key={n.id} className={`notif-item ${n.read ? 'read' : 'unread'}`}>
              <div className="notif-icon">
                {n.isGroupMatch ? '👥' : '🤝'}
              </div>
              <div className="notif-body">
                {n.isGroupMatch ? (
                  <>
                    <p className="notif-title">Group match found!</p>
                    <p className="notif-sub">
                      Group <code>{n.groupId}</code> has {n.users?.length || 0} member(s)
                    </p>
                  </>
                ) : (
                  <>
                    <p className="notif-title">New reading partner!</p>
                    <p className="notif-sub">
                      You've been matched with <strong>{n.colleague}</strong>
                    </p>
                  </>
                )}
                {!n.read && (
                  <button className="accept-btn" onClick={() => handleAccept(n)}>
                    Join Group Chat →
                  </button>
                )}
                {n.read && <span className="joined-label">✓ Joined</span>}
              </div>
              {!n.read && <span className="unread-dot" />}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
