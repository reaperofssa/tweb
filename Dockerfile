FROM node:22-bookworm

# git: some deps (baileys / libsignal) are fetched from GitHub; tini: clean signal handling so sessions save on SIGTERM
RUN apt-get update && apt-get install -y --no-install-recommends git tini ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN corepack enable

# deps first (cached unless the lockfile changes)
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY patches ./patches
RUN pnpm install --frozen-lockfile

# the rest of the repo
COPY . .

# backend URL as the browser reaches it (baked into the frontend at build time)
ENV VITE_FAKE_API=https://tweb-production-dad1.up.railway.app
ENV TWEB_DIR=/app
# same build step w.sh does (skips tests/typecheck)
RUN NODE_OPTIONS=--max-old-space-size=4096 npx vite build

# WhatsApp login + message store + media cache live in /data: attach a Railway Volume mounted at /data (VOLUME instruction is not allowed on Railway)
ENV WA_SESSIONS_DIR=/data/wa-sessions
ENV WA_MEDIA_DIR=/data/wa-media

EXPOSE 3000 8080
ENTRYPOINT ["tini", "--"]
CMD ["bash", "w.sh"]
