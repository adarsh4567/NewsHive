import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { login } from '../api';
import { useSocket } from '../context/SocketContext';
import '../styles/login.css';

export default function LoginPage() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const navigate = useNavigate();
  const { initSocket } = useSocket();

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!username || !password) { setError('Both fields are required'); return; }
    setLoading(true);
    setError('');
    try {
      const res = await login(username, password);
      const { token, username: user } = res.data;
      localStorage.setItem('token', token);
      localStorage.setItem('userid', user);
      initSocket(user);
      navigate('/feed');
    } catch (err) {
      setError(err.response?.data?.message || 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-root">
      <div className="login-bg" />
      <div className="login-card">
        <div className="login-logo">
          <span className="logo-icon">📰</span>
          <h1>NewsFlow</h1>
          <p>Your personalised news intelligence platform</p>
        </div>

        <form onSubmit={handleSubmit} className="login-form">
          {error && <div className="login-error">{error}</div>}
          <div className="form-group">
            <label>Username</label>
            <input
              id="username"
              type="text"
              placeholder="Enter any username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoComplete="username"
            />
          </div>
          <div className="form-group">
            <label>Password</label>
            <input
              id="password"
              type="password"
              placeholder="Enter any password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="current-password"
            />
          </div>
          <button type="submit" className="login-btn" disabled={loading}>
            {loading ? <span className="spinner" /> : 'Sign In'}
          </button>
          <p className="login-hint">Any username & password combination works.</p>
        </form>
      </div>
    </div>
  );
}
