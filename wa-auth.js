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

function wipe(dir) {
  try { fs.rmSync(dir, {recursive: true, force: true}); } catch(_) { /* ignore */ }
}

/** Opens (or reopens) the socket of an entry on its session dir. */
async function connect(entry) {
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
    getMessage: (key) => Promise.resolve(hooks.getMessage(key)).catch(() => undefined),
    cachedGroupMetadata: (jid) => Promise.resolve(hooks.cachedGroupMetadata(jid)).catch(() => undefined) // no groupMetadata call per group message
  });
  entry.sock = sock;
  entry.ready = false;
  try { hooks.onSocket(entry, sock, b); } catch(e) { log('onSocket hook:', e.stack || e.message); }

  sock.ev.on('creds.update', saveCreds);
  sock.ev.on('connection.update', (u) => onUpdate(entry, sock, u, b));
  return sock;
}

function onUpdate(entry, sock, {connection, lastDisconnect, qr}, b) {
  if(sock !== entry.sock || entry.closed) return; // an update from a socket we already replaced

  // the handshake is done once the (unused) QR shows up; requestPairingCode needs that
  if(qr) entry.ready = true;

  if(connection === 'open') {
    clearTimeout(entry.timer);
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
    } else if(entry.status === 'open' || isRegistered(entry)) {
      // a linked session dropped: bring it back (sano does the same with a 5 s retry)
      entry.status = 'connecting';
      setTimeout(() => { if(!entry.closed) connect(entry).catch((e) => fail(entry, e)); }, 3000);
    } else if(entry.status !== 'expired') {
      entry.status = 'failed'; // dropped before the code was accepted: the code is dead with it
    }
  }
}

function fail(entry, e) {
  log(`failed: ${entry.phone}: ${e.message}`);
  entry.status = 'failed';
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
    await connect(entry);
    return entry;
  }
  return null;
}

const getActive = () => active;

module.exports = {init, startPairing, status, cancel, logout, restore, getActive};
