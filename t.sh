#!/usr/bin/env bash
cd "$(dirname "$0")"
export TWEB_DIR="$PWD"

# one-time deps for the backend
[ -d node_modules/express ] && [ -d node_modules/ws ] || npm i express ws --no-save

# stop both on Ctrl+C
trap 'kill 0' EXIT

# backend (VALIDATE=1 checks replies against the TL schema; remove it if too noisy)
VALIDATE=1 PORT=3000 node server.js &

# frontend
VITE_FAKE_API=http://localhost:3000 pnpm start
