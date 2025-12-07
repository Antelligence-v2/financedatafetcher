# Ragz Frontend - Vercel Deployment

## Quick Fix for 404 Error

If you're getting a 404 error on Vercel, check these:

### 1. Root Directory Setting
In Vercel Dashboard → Project Settings → General:
- **Root Directory**: Should be `frontend` (if deploying from repo root)
- OR deploy directly from the `frontend` folder

### 2. Build Settings
- **Framework Preset**: Other
- **Build Command**: (leave empty)
- **Output Directory**: `.` (current directory)

### 3. Files Required
Make sure these files are in the root of your deployment:
- ✅ `index.html`
- ✅ `styles.css`
- ✅ `app.js`
- ✅ `vercel.json`
- ✅ `package.json`

### 4. Redeploy
After fixing settings, redeploy:
```bash
vercel --prod
```

## File Structure
```
frontend/
├── index.html      # Main HTML file
├── styles.css      # Styles
├── app.js          # JavaScript
├── vercel.json     # Vercel config
└── package.json    # Package info
```












