#!/usr/bin/env bash
cd "$(dirname "$0")"
export TWEB_DIR="$PWD"

# backend URL baked into the frontend at build time (change if your Codespace URL changes)
export VITE_FAKE_API="${VITE_FAKE_API:-https://ominous-engine-x59xx6vpp565cpgpq-3000.app.github.dev}"

# one-time deps
[ -d node_modules/express ] && [ -d node_modules/ws ] && [ -d node_modules/compression ] || pnpm install
corepack enable >/dev/null 2>&1

# build only if dist is missing or REBUILD=1 (skips tests/typecheck, which `pnpm build` runs)
if [ ! -d dist ] || [ "$REBUILD" = "1" ]; then
  echo "building production bundle..."
  NODE_OPTIONS=--max-old-space-size=4096 npx vite build || exit 1
fi

# tiny static server: compression + long cache for hashed assets
# FIX: vite has copyPublicDir:false, so public/ (assets/img/pattern.svg etc.) is served as a second static root,
#      and file-like paths that don't exist return 404 instead of the index.html SPA fallback.
cat > .serve-dist.cjs <<'EOF'
const express = require('express');
const compression = require('compression');
const path = require('path');
const dist = path.join(__dirname, 'dist');
const pub = path.join(__dirname, 'public');
const app = express();
app.use(compression());
app.use(express.static(dist, {
  index: false,
  setHeaders(res, file) {
    if (/(index\.html|sw[^/]*\.js|manifest.*\.json)$/.test(file)) res.setHeader('Cache-Control', 'no-cache');
    else if (/-[\w-]{8}\.\w+$/.test(file)) res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
  }
}));
app.use(express.static(pub, { index: false, setHeaders(res) { res.setHeader('Cache-Control', 'public, max-age=86400'); } }));
app.use((req, res) => {
  if (/\.\w{2,5}$/.test(req.path)) return res.sendStatus(404);
  res.set('Cache-Control', 'no-cache').sendFile(path.join(dist, 'index.html'));
});
app.listen(8080, '0.0.0.0', () => console.log('frontend on http://localhost:8080'));
EOF
node -c .serve-dist.cjs || exit 1

# stop both on Ctrl+C
trap 'kill 0' EXIT

# backend
VALIDATE=0 PORT=3000 node server.js &

# frontend (production build)
node .serve-dist.cjs
