import { useState, useEffect, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { fetchIndiaNews, fetchInfiniteNews } from '../api';
import NewsCard from '../components/NewsCard';
import NotificationPanel from '../components/NotificationPanel';
import ChatRoom from '../components/ChatRoom';
import { useSocket } from '../context/SocketContext';
import '../styles/feed.css';

export default function FeedPage() {
  const navigate = useNavigate();
  // Capture userid into state ONCE on mount.
  // localStorage is shared across same-origin tabs, so reading it on every
  // render would cause tab 1 (userA) to pick up userB's value the moment
  // tab 2 logs in. Pinning it to state prevents that cross-tab identity leak.
  const [userid] = useState(() => localStorage.getItem('userid'));
  const { connected, notifications } = useSocket();

  const [tab, setTab] = useState('india');
  const [indiaNews, setIndiaNews] = useState([]);
  const [indiaPageNo, setIndiaPageNo] = useState(1);
  const [indiaLoading, setIndiaLoading] = useState(false);
  const [indiaHasMore, setIndiaHasMore] = useState(true);

  const [infiniteNews, setInfiniteNews] = useState([]);
  const [pageNo, setPageNo] = useState(0);
  const [infiniteLoading, setInfiniteLoading] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [showNotifications, setShowNotifications] = useState(false);
  const [activeChat, setActiveChat] = useState(null); // { groupId, members }
  const loaderRef = useRef(null);

  const unreadCount = notifications.filter((n) => !n.read).length;

  // Load India news incrementally
  const loadMoreIndia = useCallback(async () => {
    if (indiaLoading || !indiaHasMore) return;
    setIndiaLoading(true);
    try {
      const r = await fetchIndiaNews(indiaPageNo);
      const results = r.data?.results || [];
      if (results.length === 0) { setIndiaHasMore(false); return; }
      
      const seen = new Set(indiaNews.map(item => item.id || item.uuid));
      const unique = results.filter(item => {
        const key = item.id || item.uuid;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });

      setIndiaNews((prev) => [...prev, ...unique]);
      setIndiaPageNo((p) => p + 1);
    } catch {
      setIndiaHasMore(false);
    } finally {
      setIndiaLoading(false);
    }
  }, [indiaPageNo, indiaLoading, indiaHasMore, indiaNews]);

  // Load personalised feed
  const loadMore = useCallback(async () => {
    if (infiniteLoading || !hasMore) return;
    setInfiniteLoading(true);
    try {
      const r = await fetchInfiniteNews(pageNo);
      const results = r.data?.results || [];
      if (results.length === 0) { setHasMore(false); return; }
      setInfiniteNews((prev) => [...prev, ...results]);
      setPageNo((p) => p + 1);
    } catch {
      setHasMore(false);
    } finally {
      setInfiniteLoading(false);
    }
  }, [pageNo, infiniteLoading, hasMore]);

  // Intersection observer for infinite scroll
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => { 
        if (entries[0].isIntersecting) {
          if (tab === 'india') loadMoreIndia();
          if (tab === 'foryou') loadMore();
        }
      },
      { threshold: 0.1 }
    );
    if (loaderRef.current) observer.observe(loaderRef.current);
    return () => observer.disconnect();
  }, [tab, loadMoreIndia, loadMore]);

  // Load first pages when tab switches
  useEffect(() => {
    if (tab === 'india' && indiaNews.length === 0) loadMoreIndia();
    if (tab === 'foryou' && infiniteNews.length === 0) loadMore();
  }, [tab]);

  const handleLogout = () => {
    localStorage.clear();
    navigate('/');
  };

  const handleOpenChat = (notification) => {
    setActiveChat({
      groupId: notification.groupId,
      members: notification.users || [notification.colleague],
    });
    setShowNotifications(false);
  };

  return (
    <div className="feed-root">
      {/* NAVBAR */}
      <nav className="navbar">
        <div className="nav-brand">
          <span>📰</span>
          <span>NewsFlow</span>
        </div>
        <div className="nav-tabs">
          <button className={tab === 'india' ? 'active' : ''} onClick={() => setTab('india')}>🇮🇳 India</button>
          <button className={tab === 'foryou' ? 'active' : ''} onClick={() => setTab('foryou')}>✨ For You</button>
        </div>
        <div className="nav-actions">
          <div className={`connection-dot ${connected ? 'online' : 'offline'}`} title={connected ? 'Connected' : 'Disconnected'} />
          <span className="nav-user">👤 {userid}</span>
          <button className="notif-btn" onClick={() => setShowNotifications(!showNotifications)}>
            🔔
            {unreadCount > 0 && <span className="notif-badge">{unreadCount}</span>}
          </button>
          <button className="logout-btn" onClick={handleLogout}>Logout</button>
        </div>
      </nav>

      {/* NOTIFICATIONS PANEL */}
      {showNotifications && (
        <NotificationPanel
          onClose={() => setShowNotifications(false)}
          onOpenChat={handleOpenChat}
          userid={userid}
        />
      )}

      {/* CHAT ROOM */}
      {activeChat && (
        <ChatRoom
          groupId={activeChat.groupId}
          members={activeChat.members}
          userid={userid}
          onClose={() => setActiveChat(null)}
        />
      )}

      {/* CONTENT */}
      <main className="feed-content">
        {tab === 'india' && (
          <>
            <div className="section-header">
              <h2>🇮🇳 Top News from India</h2>
              <span className="article-count">{indiaNews.length} articles</span>
            </div>
            <div className="news-grid">
              {indiaNews.map((article, i) => (
                <NewsCard key={`${article.id || article.uuid}-${i}`} article={article} userid={userid} />
              ))}
            </div>
            <div ref={loaderRef} className="load-trigger">
              {indiaLoading && <div className="spinner-lg" />}
              {!indiaHasMore && <p className="end-msg">You've caught up! 🎉</p>}
            </div>
          </>
        )}

        {tab === 'foryou' && (
          <>
            <div className="section-header">
              <h2>✨ Personalised For You</h2>
              <span className="article-count">Based on your reading profile</span>
            </div>
            <div className="news-grid">
              {infiniteNews.map((article, i) => (
                <NewsCard key={`${article.id || article.uuid}-${i}`} article={article} userid={userid} />
              ))}
            </div>
            <div ref={loaderRef} className="load-trigger">
              {infiniteLoading && <div className="spinner-lg" />}
              {!hasMore && <p className="end-msg">You've caught up! 🎉</p>}
            </div>
          </>
        )}
      </main>
    </div>
  );
}
