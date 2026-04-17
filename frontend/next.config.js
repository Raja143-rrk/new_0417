const path = require('path');
const fs = require('fs');

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  swcMinify: true,
  async rewrites() {
    return {
      beforeFiles: [
        {
          source: '/api/:path*',
          destination: 'http://localhost:8000/api/:path*',
        },
      ],
    };
  },
  webpack: (config, { isServer }) => {
    // Copy HTML files to public on build
    if (isServer) {
      const sourceDir = path.join(__dirname, '../');
      const publicDir = path.join(__dirname, 'public');
      
      const htmlFiles = ['home.html', 'login.html', 'add-user.html', 'advanced-connectors.html', 
                         'connection-manager.html', 'database-migrator-dark.html', 
                         'jobs-schedule.html', 'migration-history.html'];
      
      htmlFiles.forEach(file => {
        const source = path.join(sourceDir, file);
        const dest = path.join(publicDir, file);
        if (fs.existsSync(source)) {
          fs.copyFileSync(source, dest);
        }
      });

      // Copy rbac.js
      const rbacSource = path.join(sourceDir, 'rbac.js');
      const rbacDest = path.join(publicDir, 'rbac.js');
      if (fs.existsSync(rbacSource)) {
        fs.copyFileSync(rbacSource, rbacDest);
      }
    }
    
    return config;
  },
};

module.exports = nextConfig;
