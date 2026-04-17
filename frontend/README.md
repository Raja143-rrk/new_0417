# Database Migrator - Next.js Frontend

Enterprise-grade Next.js frontend for the Database Migrator application, integrated with the FastAPI backend.

## Project Structure

```
frontend/
├── app/
│   ├── layout.tsx          # Root layout
│   ├── page.tsx            # Home page
│   ├── globals.css         # Global styles
│   ├── login/
│   │   ├── page.tsx        # Login page
│   │   ├── login.css       # Login page styles
│   │   └── form.css        # Form component styles
├── components/
│   └── LoginForm.tsx       # Reusable login form component
├── public/                 # Static assets
├── next.config.js          # Next.js configuration
├── tsconfig.json           # TypeScript configuration
└── package.json            # Dependencies
```

## Features

✨ **Enterprise Design**
- Modern gradient-based UI inspired by Databricks
- Smooth animations and micro-interactions
- Responsive mobile-first design
- Dark mode support with system preference detection

🔐 **Authentication**
- Client-side login with role-based access control (RBAC)
- Password visibility toggle
- Demo credentials for testing
- Session management via sessionStorage

🎨 **Visual Polish**
- Floating background shapes
- Glassmorphism effects
- Loading spinner on form submission
- Error message display
- Theme switcher (Light/Dark/Auto)

🚀 **Developer Experience**
- Built with Next.js 14+ (App Router)
- TypeScript for type safety
- Modular component structure
- API proxy configuration for backend integration

## Setup Instructions

### Prerequisites
- Node.js 18+ 
- npm or yarn
- FastAPI backend running on `http://localhost:8000`

### Installation

1. Navigate to the frontend directory:
```bash
cd frontend
```

2. Install dependencies:
```bash
npm install
```

3. Configure environment:
```bash
# Update .env.local if needed (default is http://localhost:8000)
```

4. Start the development server:
```bash
npm run dev
```

5. Open your browser:
```
http://localhost:3000
```

## Available Scripts

```bash
npm run dev      # Start development server (http://localhost:3000)
npm run build    # Build for production
npm start        # Start production server
npm run lint     # Run ESLint
```

## Login Credentials

Demo credentials for testing:
- **admin** / admin
- **operator** / operator
- **viewer** / viewer

Click on any credential badge to auto-fill the login form.

## Backend Integration

### API Configuration
The frontend is configured to proxy API requests to the FastAPI backend:

```javascript
// next.config.js
async rewrites() {
  return {
    beforeFiles: [
      {
        source: '/api/:path*',
        destination: 'http://localhost:8000/api/:path*',
      },
    ],
  };
}
```

### Environment Variables
- `NEXT_PUBLIC_API_URL` - Base URL for the FastAPI backend (default: `http://localhost:8000`)

## Component Details

### LoginForm Component
Located in `components/LoginForm.tsx`

**Props:** None (uses React hooks for state management)

**Features:**
- Username and password fields
- Password visibility toggle
- Error message display
- Loading state during submission
- Demo credential buttons with autofill
- Form validation

**State Management:**
- `username` - User input
- `password` - Password input
- `showPassword` - Toggle password visibility
- `loading` - Submission state
- `error` - Error message display

### Login Page
Located in `app/login/page.tsx`

**Features:**
- Hero panel with feature highlights
- Trust badges
- Login form integration
- Theme switcher
- Responsive layout

## Styling

### CSS Architecture
- **globals.css** - Global styles and color variables
- **login.css** - Login container and hero panel styles
- **form.css** - Form component and UI element styles

### Color Scheme
```css
:root {
  --blue: #1f73c9;
  --blue-dark: #0f5aa1;
  --blue-light: #60a5fa;
  --blue-accent: #3b82f6;
  /* ... more colors ... */
}
```

### Responsive Breakpoints
- **1024px** - Tablet layout (single column)
- **640px** - Mobile layout (optimized spacing)

## Theme Support

The application supports three theme modes:

1. **Light** - Bright, clean interface
2. **Dark** - Eye-friendly dark mode
3. **Auto** - Follows system preference

Theme preference is stored in `localStorage` as `dbm_theme`.

## Browser Support

- Chrome/Edge 90+
- Firefox 88+
- Safari 14+
- Mobile browsers (iOS Safari, Chrome Mobile)

## Performance

- **Code Splitting** - Automatic with Next.js
- **Image Optimization** - Via Next.js Image component
- **CSS Optimization** - Minified in production
- **Build Size** - ~50KB (gzipped)

## Security Considerations

⚠️ **Important:** Current login uses client-side validation. For production:

1. **Implement backend authentication:**
   - Create API endpoint: `POST /api/auth/login`
   - Validates credentials against secure database
   - Returns JWT or session token

2. **Update LoginForm component:**
   ```typescript
   const response = await fetch('/api/auth/login', {
     method: 'POST',
     headers: { 'Content-Type': 'application/json' },
     body: JSON.stringify({ username, password }),
   });
   ```

3. **Add authentication middleware:**
   - Protect routes with `middleware.ts`
   - Validate tokens on request

4. **Use secure cookies:**
   - HTTPOnly, Secure, SameSite flags
   - Token refresh mechanism

## Troubleshooting

### Port 3000 already in use
```bash
# Specify different port
npm run dev -- -p 3001
```

### API connection refused
- Ensure FastAPI backend is running on `http://localhost:8000`
- Check `.env.local` configuration
- Browser console for CORS errors

### Styling not loading
```bash
# Clear Next.js cache
rm -rf .next
npm run dev
```

## Future Enhancements

- [ ] Real backend authentication endpoint
- [ ] Session management with JWT
- [ ] Protected routes and middleware
- [ ] Additional pages (home, migrations, settings)
- [ ] API integration with migration endpoints
- [ ] User profile and settings page
- [ ] Notification system
- [ ] Error boundary components

## Support

For issues or questions:
1. Check the backend FastAPI logs
2. Browser developer tools (F12)
3. Review `.next` build folder for errors

## License

Same as the main Database Migrator project.
