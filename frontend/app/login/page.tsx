'use client';

import { useState, useEffect } from 'react';
import LoginForm from '@/components/LoginForm';
import './login.css';

export default function LoginPage() {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) return null;

  return (
    <div className="login-container">
      <div className="login-card">
        <div className="card-header">
          <div className="header-content">
            <div className="logo-icon">
              <i className="fa-solid fa-database"></i>
            </div>
            <h1 className="card-heading">
              <span className="text-black">Database</span> <span className="text-blue">Migrator</span>
            </h1>
          </div>
          <p className="card-subheading">Sign in to Database Migrator</p>
        </div>
        <LoginForm />
      </div>
    </div>
  );
}
