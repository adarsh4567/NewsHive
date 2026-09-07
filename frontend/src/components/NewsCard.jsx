import { useState, useEffect, useRef } from 'react';
import { publishEvent, fetchSimilarNews, streamAgentAnalysis } from '../api';
import '../styles/newscard.css';

function getTopCategory(article) {
  return article?.categories?.[0]?.id || article?.category?.id || 'medtop:20000385';
}
function getTopTopic(article) {
  return article?.topics?.[0]?.id || article?.topic?.id || 'topic_general';
}

export default function NewsCard({ article, userid }) {
  const [expanded, setExpanded] = useState(false);
  const [similar, setSimilar] = useState([]);
  const [similarLoading, setSimilarLoading] = useState(false);

  // Financial agent state
  const [agentText, setAgentText] = useState('');
  const [agentStatus, setAgentStatus] = useState('idle'); // idle | loading | streaming | done | error
  const [agentError, setAgentError] = useState('');
  const agentControllerRef = useRef(null);

  const dwellStart = useRef(null);

  const title = article?.title || 'Untitled';
  const description = article?.description || article?.snippet || '';
  const sourceImageUrl = article?.image_url || article?.media?.[0]?.url;
  const imageUrl = sourceImageUrl || 'https://images.unsplash.com/photo-1585829365295-ab7cd400c167?w=600&q=80';
  
  const source = article?.source?.name || article?.source_name || 'Unknown Source';
  const publishedAt = article?.published_at || article?.publishedAt || '';
  const articleId = article?.id || article?.uuid || String(Math.random());
  const articleUrl = article?.url || '#';

  const safeExtract = (arr) => 
    Array.isArray(arr) ? arr.map(item => typeof item === 'object' ? String(item.id || item.name || '') : String(item)).filter(Boolean) : [];

  const categories = safeExtract(article?.categories);
  const topics = safeExtract(article?.topics);
  const entities = safeExtract(article?.entities);

  // ── Publish event on OPEN (every click) ──────────────────────────────────
  const publishOnOpen = async () => {
    if (!userid) return;
    try {
      await publishEvent({
        userid,
        newsid: articleId,
        top_category: getTopCategory(article),
        top_topic: getTopTopic(article),
        categories,
        topics,
        entities,
        dwell: 0,
      });
    } catch (e) {
      console.warn('publish error', e.message);
    }
  };

  const handleOpen = async () => {
    setExpanded(true);
    dwellStart.current = Date.now();

    // 🔴 Publish to Kafka immediately on click
    publishOnOpen();

    // Fetch similar news if not already loaded
    if (!similar.length) {
      setSimilarLoading(true);
      try {
        const r = await fetchSimilarNews(articleId);
        setSimilar(r.data?.results || []);
      } catch {
        setSimilar([]);
      } finally {
        setSimilarLoading(false);
      }
    }
  };

  const handleClose = () => {
    // Cancel any in-flight agent stream
    if (agentControllerRef.current) {
      agentControllerRef.current.abort();
      agentControllerRef.current = null;
    }

    setExpanded(false);
    dwellStart.current = null;

    // Reset agent state for next open
    setAgentText('');
    setAgentStatus('idle');
    setAgentError('');
  };

  // Cancel stream on unmount
  useEffect(() => {
    return () => {
      if (agentControllerRef.current) agentControllerRef.current.abort();
    };
  }, []);

  // ── Financial Agent handler ──────────────────────────────────────────────
  const handleAskAgent = () => {
    if (agentStatus === 'loading' || agentStatus === 'streaming') return;

    setAgentText('');
    setAgentError('');
    setAgentStatus('loading');

    const newsInput = `${title}\n\n${description}`;

    const controller = streamAgentAnalysis(
      newsInput,
      (text) => {
        setAgentStatus('streaming');
        setAgentText(text);
      },
      () => {
        setAgentStatus('done');
        agentControllerRef.current = null;
      },
      (err) => {
        setAgentStatus('error');
        setAgentError(err);
        agentControllerRef.current = null;
      }
    );

    agentControllerRef.current = controller;
  };

  const formattedDate = publishedAt
    ? new Date(publishedAt).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', year: 'numeric' })
    : '';

  return (
    <>
      <div className="news-card" onClick={handleOpen}>
        {imageUrl && (
          <div className="card-image">
            <img src={imageUrl} alt={title} loading="lazy" />
          </div>
        )}
        <div className="card-body">
          <div className="card-meta">
            <span className="source-badge">{source}</span>
            {formattedDate && <span className="card-date">{formattedDate}</span>}
          </div>
          <h3 className="card-title">{title}</h3>
          {description && <p className="card-desc">{description.slice(0, 120)}…</p>}
          {categories.length > 0 && (
            <div className="card-tags">
              {categories.slice(0, 2).map((c) => (
                <span key={c} className="tag">{typeof c === 'string' ? c.split(':').pop() : c}</span>
              ))}
            </div>
          )}
          <button className="read-more-btn">Read More →</button>
        </div>
      </div>

      {/* ARTICLE MODAL */}
      {expanded && (
        <div className="article-overlay" onClick={handleClose}>
          <div className="article-modal" onClick={(e) => e.stopPropagation()}>
            <button className="close-btn" onClick={handleClose}>✕</button>
            {imageUrl && <img className="modal-image" src={imageUrl} alt={title} />}
            <div className="modal-content">
              <div className="modal-meta">
                <span className="source-badge">{source}</span>
                <span className="card-date">{formattedDate}</span>
              </div>
              <h2>{title}</h2>
              <p className="modal-desc">{description}</p>
              <a href={articleUrl} target="_blank" rel="noreferrer" className="original-link">
                Read full article ↗
              </a>

              {/* ── FINANCIAL AGENT SECTION ── */}
              <div className="agent-section">
                <div className="agent-header">
                  <span className="agent-title">🤖 Financial AI Analysis</span>
                  <button
                    className={`agent-btn ${agentStatus === 'loading' || agentStatus === 'streaming' ? 'agent-btn--busy' : ''}`}
                    onClick={handleAskAgent}
                    disabled={agentStatus === 'loading' || agentStatus === 'streaming'}
                  >
                    {agentStatus === 'loading' && <span className="agent-spinner" />}
                    {agentStatus === 'loading' ? 'Analysing…' :
                     agentStatus === 'streaming' ? 'Streaming…' :
                     agentStatus === 'done' ? '↻ Re-analyse' :
                     '✦ Analyse with AI'}
                  </button>
                </div>

                {(agentStatus === 'loading' || agentStatus === 'streaming' || agentStatus === 'done') && (
                  <div className="agent-panel">
                    {agentStatus === 'loading' && (
                      <div className="agent-loading">
                        <div className="agent-dots">
                          <span /><span /><span />
                        </div>
                        <p>Agent is thinking…</p>
                      </div>
                    )}
                    {(agentStatus === 'streaming' || agentStatus === 'done') && agentText && (
                      <div className="agent-output">
                        <p className="agent-text">
                          {agentText}
                          {agentStatus === 'streaming' && <span className="agent-cursor" />}
                        </p>
                      </div>
                    )}
                  </div>
                )}

                {agentStatus === 'error' && (
                  <div className="agent-error">
                    <span>⚠️ {agentError || 'Failed to connect to financial agent. Ensure the service is running.'}</span>
                  </div>
                )}
              </div>

              {/* SIMILAR NEWS */}
              <div className="similar-section">
                <h3>Similar Articles</h3>
                {similarLoading ? (
                  <div className="spinner-sm" />
                ) : similar.length > 0 ? (
                  <div className="similar-list">
                    {similar.slice(0, 4).map((s) => (
                      <a
                        key={s.id || s.uuid}
                        href={s.url || '#'}
                        target="_blank"
                        rel="noreferrer"
                        className="similar-item"
                      >
                        {s.image_url && <img src={s.image_url} alt={s.title} />}
                        <span>{s.title}</span>
                      </a>
                    ))}
                  </div>
                ) : (
                  <p className="no-similar">No similar articles found.</p>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

