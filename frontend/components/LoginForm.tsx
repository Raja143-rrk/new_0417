'use client';

import { FormEvent, useState } from 'react';
import { useRouter } from 'next/navigation';
import { login } from '@/lib/rbac';
import '../app/login/form.css';

export default function LoginForm() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const router = useRouter();

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError('');
    setLoading(true);

    try {
      const trimmedUsername = username.trim();
      const trimmedPassword = password.trim();

      if (!trimmedUsername || !trimmedPassword) {
        setError('Please enter both user and password.');
        setLoading(false);
        return;
      }

      const session = login(trimmedUsername, trimmedPassword);
      const resolvedSession = await session;
      if (resolvedSession) {
        router.push(resolvedSession.home || '/home');
      } else {
        setError('Invalid username or password.');
      }
    } catch {
      setError('Login failed. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
      {error && (
        <div className="error-alert">
          <span>{error}</span>
          <button type="button" className="error-close" onClick={() => setError('')}>
            x
          </button>
        </div>
      )}

      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <input
            className="form-input"
            id="username"
            type="text"
            autoComplete="username"
            placeholder="User"
            value={username}
            onChange={(event) => setUsername(event.target.value)}
            disabled={loading}
          />
        </div>

        <div className="form-group">
          <input
            className="form-input"
            id="password"
            type="password"
            autoComplete="current-password"
            placeholder="Password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            disabled={loading}
          />
        </div>

        <button className="submit-btn" type="submit" disabled={loading}>
          {loading ? 'Signing in...' : 'Sign in'}
        </button>
      </form>

      <div className="form-footer">
        <a href="#" className="forgot-link">
          Forgot Password?
        </a>
      </div>

      <div className="back-link">
        <a href="#" className="link-text">
          Back to sign in
        </a>
      </div>
    </>
  );
}
