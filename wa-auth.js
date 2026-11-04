'use strict';
/**
 * wa-auth.js — real WhatsApp (Baileys) login for the tweb simulated backend.
 *
 * Pairing flow (same mechanics as sano's handlePairingCode, minus the terminal prompt):
 *   startPairing(phone) -> socket -> wait until the noise handshake is done -> requestPairingCode(phone)
 *   user types the 8-char code in WhatsApp > Linked devices > Link with phone number
 *   WhatsApp closes the socket with `restartRequired` (515) -> we reconnect with the now-registered creds
 *   connection 'open' -> status 'open'
 *
 * Baileys is ESM-only, so it is loaded with import() from this CommonJS file.
 *
 * Usage from server_75.js:
 *   const WA = require('./wa-auth');
 *   WA.init({onOpen: (entry) => {...}, onLogout: () => {...}});
 *   await WA.restore();                       // on boot: reconnect a session saved on disk
 *   const r = await WA.startPairing(phone);   // {entry, linked}
 *   WA.status(hash)                           // entry | null   (entry.status: see STATUS below)
 *   await WA.cancel(hash); await WA.logout();
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const SESSIONS_DIR = process.env.WA_SESSIONS_DIR || path.join(__dirname, 'wa-sessions');
const PAIR_TTL_MS = +process.env.WA_PAIR_TTL_MS || 120000; // how long we show a code before giving up on it
const READY_TIMEOUT_MS = 15000;
const LINKED_TIMEOUT_MS = 20000;
const RECONNECT_MIN_MS = +process.env.WA_RECONNECT_MIN_MS || 2000; // reconnect backoff: 2 s, 4 s, ... capped, forever (a linked session must never stay down)
const RECONNECT_MAX_MS = +process.env.WA_RECONNECT_MAX_MS || 60000;
const WATCHDOG_MS = +process.env.WA_WATCHDOG_MS || 30000; // how often a linked socket is checked for being really alive
const STALE_MS = +process.env.WA_STALE_MS || 150000; // an 'open' socket that produced no event at all for this long gets probed / recycled

/** STATUS: connecting -> pending (code shown) -> open | expired | failed */
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), '[WA]', ...a);
const err = (code, type, message) => Object.assign(new Error(message || type), {code, type, message: message || type});
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let injected = null; // tests can pass a fake baileys module through init({baileys})
let B = null;
let hooks = {onOpen() {}, onLogout() {}, onSocket() {}, getMessage: async() => undefined, cachedGroupMetadata: async() => undefined};
const entries = new Map(); // hash -> entry
let active = null; // the entry whose socket is open

async function baileys() {
  if(injected) return injected;
  return (B ||= await import('@whiskeysockets/baileys'));
}

function init(opts = {}) {
  hooks = {...hooks, ...opts};
  if(opts.baileys) injected = opts.baileys;
}

const digits = (s) => String(s || '').replace(/\D/g, '');
const jidToPhone = (jid) => digits(String(jid || '').split('@')[0].split(':')[0]);
const credsFile = (dir) => path.join(dir, 'creds.json');

function isRegisteredOnDisk(dir) {
  try { return !!JSON.parse(fs.readFileSync(credsFile(dir), 'utf8')).registered; } catch(_) { return false; }
}
const isRegistered = (entry) => !!entry.sock?.authState?.creds?.registered;
const isLinked = (entry) => entry.status === 'open' || isRegistered(entry) || isRegisteredOnDisk(entry.dir); // a session that was linked once must be kept alive

function wipe(dir) {
  try { fs.rmSync(dir, {recursive: true, force: true}); } catch(_) { /* ignore */ }
}

/** Opens (or reopens) the socket of an entry on its session dir. */
// Baileys asks for a counter cache to track how often a message was re-requested after a decrypt failure; without it every
// retry request restarts from 0 and undecryptable messages (the usual "missing message") are retried forever or never.
const retryCache = (() => {
  const m = new Map();
  return {
    get: (k) => m.get(k),
    set: (k, v) => { m.set(k, v); if(m.size > 5000) m.delete(m.keys().next().value); },
    del: (k) => { m.delete(k); },
    flushAll: () => m.clear()
  };
})();

/** Ends and forgets a socket completely, so two sockets of one account never run side by side (WhatsApp would answer connectionReplaced). */
function killSocket(sock) {
  if(!sock) return;
  try { sock.ev.removeAllListeners('connection.update'); } catch(_) { /* ignore */ }
  try { sock.ev.removeAllListeners('creds.update'); } catch(_) { /* ignore */ }
  try { sock.end(undefined); } catch(_) { /* ignore */ }
}

async function connect(entry) {
  if(entry.closed) return null;
  if(entry.connecting) return entry.connecting; // never two connects in parallel for one entry
  entry.connecting = connectNow(entry).finally(() => { entry.connecting = null; });
  return entry.connecting;
}

async function connectNow(entry) {
  clearTimeout(entry.retryTimer);
  const old = entry.sock;
  entry.sock = null;
  killSocket(old);
  const b = await baileys();
  const makeWASocket = b.default || b.makeWASocket;
  const pino = require('pino');
  const {state, saveCreds} = await b.useMultiFileAuthState(entry.dir);
  const {version} = await b.fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    logger: pino({level: 'silent'}),
    printQRInTerminal: false,
    browser: b.Browsers.ubuntu('Chrome'), // same as sano; pairing codes need a desktop-browser identity
    markOnlineOnConnect: false, // a chat client should not silence the phone's notifications
    syncFullHistory: process.env.WA_FULL_HISTORY === '1',
    keepAliveIntervalMs: +process.env.WA_KEEPALIVE_MS || 20000, // detect a dead link in ~20 s instead of the 30 s default
    connectTimeoutMs: 60000,
    defaultQueryTimeoutMs: 60000,
    retryRequestDelayMs: 250,
    maxMsgRetryCount: 5,
    msgRetryCounterCache: retryCache,
    getMessage: (key) => Promise.resolve(hooks.getMessage(key)).catch(() => undefined),
    cachedGroupMetadata: (jid) => Promise.resolve(hooks.cachedGroupMetadata(jid)).catch(() => undefined) // no groupMetadata call per group message
  });
  entry.sock = sock;
  entry.ready = false;
  entry.lastEvent = Date.now();
  sock.ev.on('messages.upsert', () => { entry.lastEvent = Date.now(); });
  sock.ev.on('presence.update', () => { entry.lastEvent = Date.now(); });
  try { hooks.onSocket(entry, sock, b); } catch(e) { log('onSocket hook:', e.stack || e.message); }

  sock.ev.on('creds.update', saveCreds);
  sock.ev.on('connection.update', (u) => onUpdate(entry, sock, u, b));
  return sock;
}

function onUpdate(entry, sock, {connection, lastDisconnect, qr}, b) {
  if(sock !== entry.sock || entry.closed) return; // an update from a socket we already replaced

  // the handshake is done once the (unused) QR shows up; requestPairingCode needs that
  if(qr) entry.ready = true;

  entry.lastEvent = Date.now();
  if(connection === 'open') {
    clearTimeout(entry.timer);
    clearTimeout(entry.retryTimer);
    entry.attempts = 0; // healthy again: next outage starts the backoff from the beginning
    entry.status = 'open';
    entry.user = {jid: sock.user?.id, name: sock.user?.name, phone: jidToPhone(sock.user?.id) || entry.phone};
    active = entry;
    log(`open: ${entry.user.phone}`);
    try { hooks.onOpen(entry); } catch(e) { log('onOpen hook:', e.message); }
    return;
  }

  if(connection === 'close') {
    const reason = lastDisconnect?.error?.output?.statusCode;
    const D = b.DisconnectReason;
    log(`close: ${entry.phone} reason=${reason}`);

    if(reason === D.restartRequired) { // expected right after the code was accepted
      connect(entry).catch((e) => fail(entry, e));
    } else if(reason === D.loggedOut) {
      entry.status = 'failed';
      entry.closed = true;
      if(active === entry) active = null;
      wipe(entry.dir);
      try { hooks.onLogout(entry); } catch(_) { /* ignore */ }
    } else if(isLinked(entry)) {
      // a linked session dropped: bring it back, forever, with a growing delay.
      // connectionReplaced (440) means another client took the session over; fighting it in a tight loop gets the number flagged, so wait longer.
      entry.status = 'connecting';
      scheduleReconnect(entry, reason === D.connectionReplaced ? 30000 : 0);
    } else if(entry.status !== 'expired') {
      entry.status = 'failed'; // dropped before the code was accepted: the code is dead with it
    }
  }
}

/** retries a linked session's connection until it works: 2 s, 4 s, 8 s ... 60 s (+ jitter). Never gives up while the session is registered. */
function scheduleReconnect(entry, minDelay = 0) {
  if(entry.closed) return;
  clearTimeout(entry.retryTimer);
  const n = entry.attempts = (entry.attempts || 0) + 1;
  const delay = Math.max(minDelay, Math.min(RECONNECT_MAX_MS, RECONNECT_MIN_MS * 2 ** (n - 1))) + Math.random() * 500;
  log(`reconnect #${n} for ${entry.phone} in ${Math.round(delay / 1000)} s`);
  entry.retryTimer = setTimeout(() => {
    entry.retryTimer = null;
    if(entry.closed) return;
    connect(entry).catch((e) => { log(`reconnect #${n} failed: ${e.message}`); entry.status = 'connecting'; scheduleReconnect(entry); });
  }, delay);
  entry.retryTimer.unref?.();
}

/** a real round trip to WhatsApp (an IQ query with an answer); a socket that only *looks* open fails it */
function probe(entry) {
  const sock = entry.sock;
  let t;
  const timeout = new Promise((_, rej) => { t = setTimeout(() => rej(new Error('probe timeout')), 15000); });
  const query = typeof sock?.fetchPrivacySettings === 'function' ? sock.fetchPrivacySettings(true) : sock.sendPresenceUpdate('unavailable');
  return Promise.race([query, timeout]).finally(() => clearTimeout(t));
}

/** every WATCHDOG_MS: a linked session whose websocket is gone without a 'close' event (or silent for too long) is reconnected */
setInterval(() => {
  for(const entry of entries.values()) {
    if(entry.closed || entry.connecting || !isLinked(entry)) continue;
    if(entry.status === 'connecting') { // down, and nothing is going to bring it back
      if(!entry.retryTimer) scheduleReconnect(entry);
      continue;
    }
    if(entry.status !== 'open' || !entry.sock) continue;
    let wsDead = false;
    try { wsDead = entry.sock.ws?.isClosed === true || entry.sock.ws?.isClosing === true; } catch(_) { /* unknown ws shape: rely on the silence check */ }
    const recycle = (why) => {
      if(entry.closed || entry.status !== 'open') return;
      log(`watchdog: ${entry.phone} ${why}, reconnecting`);
      entry.status = 'connecting';
      scheduleReconnect(entry);
    };
    if(wsDead) recycle('socket is closed without notice');
    else if(Date.now() - (entry.lastEvent || 0) > STALE_MS) {
      entry.lastEvent = Date.now(); // WhatsApp is quiet on a healthy link too: probe before deciding it is dead
      probe(entry).catch((e) => recycle(`did not answer a probe (${e.message})`));
    }
  }
}, WATCHDOG_MS).unref();

function fail(entry, e) {
  log(`failed: ${entry.phone}: ${e.message}`);
  if(!entry.closed && isLinked(entry)) { entry.status = 'connecting'; scheduleReconnect(entry); } // a linked session is never written off
  else entry.status = 'failed';
}

/** Ends an entry's socket. `wipeCreds` removes the session dir, but only for a number that never got linked. */
async function dispose(entry, wipeCreds) {
  entry.closed = true;
  clearTimeout(entry.timer);
  try { entry.sock?.end(undefined); } catch(_) { /* ignore */ }
  if(wipeCreds && !isRegistered(entry) && !isRegisteredOnDisk(entry.dir)) wipe(entry.dir);
  if(active === entry) active = null;
  setTimeout(() => entries.delete(entry.hash), 60000).unref?.();
}

async function waitFor(pred, ms) {
  const end = Date.now() + ms;
  while(Date.now() < end) {
    if(pred()) return true;
    await sleep(200);
  }
  return pred();
}

/**
 * Starts a pairing for `rawPhone`.
 * Returns {entry, linked}: linked=true when that number already had a working session (no code needed).
 */
async function startPairing(rawPhone) {
  const phone = digits(rawPhone);
  if(phone.length < 7 || phone.length > 15) throw err(400, 'PHONE_NUMBER_INVALID');

  // a new attempt for the same number replaces the old one
  for(const e of entries.values()) {
    if(e.phone === phone && !e.closed && e.status !== 'open') await dispose(e, false);
  }

  const entry = {hash: crypto.randomBytes(12).toString('hex'), phone, dir: path.join(SESSIONS_DIR, phone), status: 'connecting'};
  entries.set(entry.hash, entry);
  fs.mkdirSync(entry.dir, {recursive: true});

  try {
    // already linked on disk (or still open): reuse it instead of asking for a second code
    if(active && active.phone === phone && active.status === 'open') {
      entries.delete(entry.hash);
      return {entry: active, linked: true};
    }
    if(isRegisteredOnDisk(entry.dir)) {
      await connect(entry);
      await waitFor(() => entry.status === 'open' || entry.status === 'failed', LINKED_TIMEOUT_MS);
      if(entry.status === 'open') return {entry, linked: true};
      await dispose(entry, false);
      wipe(entry.dir); // stale or logged-out session: link again from scratch
      fs.mkdirSync(entry.dir, {recursive: true});
      entry.closed = false;
      entry.status = 'connecting';
    }

    await connect(entry);
    // requestPairingCode only works after the handshake; sano just sleeps 3 s, we wait for the signal (with a fallback)
    await waitFor(() => entry.ready, READY_TIMEOUT_MS);

    let code;
    try {
      code = await entry.sock.requestPairingCode(phone);
    } catch(e) {
      throw err(500, 'WA_PAIRING_FAILED', e.message);
    }
    entry.code = code.match(/.{1,4}/g).join('-');
    entry.status = 'pending';
    entry.ttl = Math.round(PAIR_TTL_MS / 1000);
    entry.timer = setTimeout(() => {
      // once the code was accepted the creds are registered and the 515 restart is under way: not an expiry
      if(entry.status === 'pending' && !isRegistered(entry)) {
        entry.status = 'expired';
        dispose(entry, true);
      }
    }, PAIR_TTL_MS);
    log(`code for ${phone}: ${entry.code}`);
    return {entry, linked: false};
  } catch(e) {
    await dispose(entry, true);
    throw e;
  }
}

const status = (hash) => entries.get(hash) || null;

async function cancel(hash) {
  const entry = entries.get(hash);
  if(entry && entry.status !== 'open') await dispose(entry, true);
}

async function logout() {
  const entry = active;
  if(!entry) return;
  try { await entry.sock.logout(); } catch(_) { /* already gone */ }
  await dispose(entry, false);
  wipe(entry.dir);
}

/** On boot: reconnect the first session that was saved on disk. */
async function restore() {
  if(!fs.existsSync(SESSIONS_DIR)) return null;
  for(const name of fs.readdirSync(SESSIONS_DIR)) {
    const dir = path.join(SESSIONS_DIR, name);
    if(!isRegisteredOnDisk(dir)) continue;
    const entry = {hash: 'restored-' + name, phone: digits(name), dir, status: 'connecting'};
    entries.set(entry.hash, entry);
    log(`restoring session ${name}`);
    try { await connect(entry); } catch(e) { log(`restore connect failed: ${e.message}, will keep retrying`); scheduleReconnect(entry); }
    return entry;
  }
  return null;
}

const getActive = () => active;

module.exports = {init, startPairing, status, cancel, logout, restore, getActive};
