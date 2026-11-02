#!/usr/bin/env node
'use strict';
/**
 * Simulated Telegram backend for tweb (Telegram Web K).
 *
 * tweb (patched, see apiManager.ts.patch) sends every request as JSON to  POST /api  and
 * receives live updates over  WS /updates.  Replies use the SAME object shapes tweb produces
 * after decoding MTProto: {_: 'constructorName', pFlags: {...}, ...}, Bool -> true/false,
 * Vector -> array, bytes -> {"__b64": "..."}.
 *
 * Run:   npm i express ws && TWEB_DIR=/path/to/tweb node server.js
 * Front: VITE_FAKE_API=http://localhost:3000 pnpm start   (in the tweb repo)
 * Login: any phone number, code 12345.
 *
 * Everything marked  [WA]  is where Baileys plugs in later.
 */
const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const {WebSocketServer} = require('ws');

const PORT = +process.env.PORT || 3000;
const TWEB_DIR = process.env.TWEB_DIR || path.join(__dirname, 'tweb');
const LOGIN_CODE = '12345';
const SELF = 1000;
const now = () => Math.floor(Date.now() / 1000);
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

/* ───────────────────────────── TL schema (for fallbacks) ───────────────────────────── */
function loadSchema() {
  try {
    const s = fs.readFileSync(path.join(TWEB_DIR, 'src/lib/mtproto/schema.ts'), 'utf8');
    const j = s.slice(s.indexOf('export default ') + 15);
    let d = 0, inStr = false, esc = false, end = -1;
    for(let k = 0; k < j.length; k++) {
      const c = j[k];
      if(inStr) { if(esc) esc = false; else if(c === '\\') esc = true; else if(c === '"') inStr = false; continue; }
      if(c === '"') inStr = true; else if(c === '{') d++; else if(c === '}' && --d === 0) { end = k; break; }
    }
    const api = JSON.parse(j.slice(0, end + 1)).API;
    api.method = Object.fromEntries(api.methods.map((m) => [m.method, m]));
    api.byPredicate = Object.fromEntries(api.constructors.map((c) => [c.predicate, c]));
    api.byType = {};
    for(const c of api.constructors) (api.byType[c.type] ||= []).push(c);
    log(`schema: ${api.methods.length} methods, ${api.constructors.length} constructors`);
    return api;
  } catch(e) {
    log('! schema not loaded (set TWEB_DIR). Fallbacks will be crude:', e.message);
    return null;
  }
}
const SCHEMA = loadSchema();

/** Builds a minimal valid object of a TL type (used only for methods without a handler). */
function synth(type, depth = 0) {
  if(!type) return null;
  if(/^[Vv]ector/.test(type)) return [];
  switch(type) {
    case 'Bool': case 'true': return true;
    case 'int': case 'long': case 'double': case 'int128': case 'int256': return 0;
    case 'string': return '';
    case 'bytes': return new Uint8Array(0);
    case 'Object': case '!X': return {};
  }
  if(!SCHEMA || depth > 3) return null;
  const list = (SCHEMA.byType[type] || []).filter((c) => !/NotModified|TooLong/.test(c.predicate));
  if(!list.length) return null;
  const req = (c) => c.params.filter((p) => !/^flags2?$/.test(p.name) && !/^flags2?\.\d+\?/.test(p.type)).length;
  const c = list.sort((a, b) => req(a) - req(b))[0];
  const o = {_: c.predicate};
  if(c.params.some((p) => /^flags/.test(p.type) || p.type === '#')) o.pFlags = {};
  for(const p of c.params) {
    if(p.type === '#' || /^flags2?\.\d+\?/.test(p.type)) continue;
    o[p.name] = synth(p.type, depth + 1);
  }
  return o;
}

/* ───────────────────────────── simulated data store  [WA] ───────────────────────────── */
// [WA] replace with Baileys: users <- contacts/jids (id = Number(phone)), groups <- group metadata,
// msgs <- messages.upsert, and addOutgoing() <- sock.sendMessage().
const users = new Map([
  [1000, {first_name: 'Me', last_name: '', phone: '10000000000', username: 'me'}],
  [1001, {first_name: 'Alice', last_name: 'Walker', phone: '10000000001'}],
  [1002, {first_name: 'Bob', last_name: 'Stone', phone: '10000000002'}],
  [1003, {first_name: 'Carol', last_name: '', phone: '10000000003'}]
]);
const groups = new Map([[2001, {title: 'Test Group', members: [1000, 1001, 1002]}]]);
const msgs = [];
const readState = {}; // peerKey -> {inbox, outbox}
let lastId = 0, pts = 0;

const peerKeyOf = (p) => p && (p._ === 'inputPeerSelf' ? 'u' + SELF : p._ === 'inputPeerUser' || p._ === 'peerUser' ? 'u' + p.user_id : p._ === 'inputPeerChat' || p._ === 'peerChat' ? 'c' + p.chat_id : null);
const peerOf = (key) => key[0] === 'u' ? {_: 'peerUser', user_id: +key.slice(1)} : {_: 'peerChat', chat_id: +key.slice(1)};
const keys = () => [...new Set(msgs.map((m) => m.key))];
const msgsOf = (key) => msgs.filter((m) => m.key === key);

function addMessage(key, from, text, date = now(), extra = {}) {
  const m = {id: ++lastId, key, from, text, date, out: from === SELF, ...extra};
  msgs.push(m);
  pts++;
  return m;
}

/* TL builders */
const flags = (o) => Object.fromEntries(Object.entries(o).filter(([, v]) => v));
const tlUser = (id) => {
  const u = users.get(id) || {first_name: 'User ' + id, last_name: '', phone: String(id)};
  return {_: 'user', pFlags: flags({self: id === SELF, contact: id !== SELF, mutual_contact: id !== SELF}), id, access_hash: '1',
    first_name: u.first_name, last_name: u.last_name || '', ...(u.username && {username: u.username}), phone: u.phone,
    status: {_: 'userStatusRecently', pFlags: {}}};
};
const tlChat = (id) => ({_: 'chat', pFlags: {}, id, title: groups.get(id).title, photo: {_: 'chatPhotoEmpty'},
  participants_count: groups.get(id).members.length, date: now(), version: 1});
const tlMessage = (m) => ({_: 'message', pFlags: flags({out: m.out}), id: m.id, from_id: {_: 'peerUser', user_id: m.from},
  peer_id: peerOf(m.key), date: m.date, message: m.text, ...(m.edit_date && {edit_date: m.edit_date}),
  ...(m.reply_to && {reply_to: {_: 'messageReplyHeader', pFlags: {}, reply_to_msg_id: m.reply_to}})});
const tlDialog = (key) => {
  const list = msgsOf(key), top = list[list.length - 1], rs = readState[key] || (readState[key] = {inbox: 0, outbox: 0});
  return {_: 'dialog', pFlags: {}, peer: peerOf(key), top_message: top ? top.id : 0, read_inbox_max_id: rs.inbox,
    read_outbox_max_id: rs.outbox, unread_count: list.filter((m) => !m.out && m.id > rs.inbox).length,
    unread_mentions_count: 0, unread_reactions_count: 0, unread_poll_votes_count: 0,
    notify_settings: {_: 'peerNotifySettings', pFlags: {}}};
};
const state = () => ({_: 'updates.state', pts, qts: 0, date: now(), seq: 0, unread_count: 0});

/** users+chats referenced by a set of messages / dialog keys */
function entities(ms, ks = []) {
  const uid = new Set([SELF]), cid = new Set();
  for(const m of ms) { uid.add(m.from); const k = m.key; (k[0] === 'u' ? uid : cid).add(+k.slice(1)); }
  for(const k of ks) (k[0] === 'u' ? uid : cid).add(+k.slice(1));
  for(const id of cid) for(const u of groups.get(id)?.members || []) uid.add(u);
  return {users: [...uid].map(tlUser), chats: [...cid].filter((i) => groups.has(i)).map(tlChat)};
}
function dialogsResult(ks, type = 'messages.dialogs') {
  const dialogs = ks.map(tlDialog), tops = ks.map((k) => msgsOf(k).pop()).filter(Boolean);
  const r = {_: type, dialogs, messages: tops.map(tlMessage), ...entities(tops, ks)};
  if(type === 'messages.peerDialogs') r.state = state();
  return r;
}
const updatesOf = (updates, ms = []) => ({_: 'updates', updates, ...entities(ms), date: now(), seq: 0});

/* ───────────────────────────── live updates (WS) ───────────────────────────── */
const sockets = new Set();
function push(obj) {
  const data = JSON.stringify(obj, jsonReplacer);
  for(const s of sockets) if(s.readyState === 1) s.send(data);
}
function pushIncoming(key, from, text) {
  const m = addMessage(key, from, text);
  push(updatesOf([{_: 'updateNewMessage', message: tlMessage(m), pts, pts_count: 1}], [m]));
  return m;
}

/* ───────────────────────────── method handlers ───────────────────────────── */
const fail = (code, type, message) => Object.assign(new Error(type), {code, type, message});
const H = {};

// -- bootstrap / config
H['help.getNearestDc'] = () => ({_: 'nearestDc', country: 'US', this_dc: 2, nearest_dc: 2});
H['help.getConfig'] = () => ({...synth('Config'), _: 'config', date: now(), expires: now() + 3600, test_mode: false, this_dc: 2,
  dc_options: [{_: 'dcOption', pFlags: {}, id: 2, ip_address: '127.0.0.1', port: 443}], dc_txt_domain_name: '', chat_size_max: 200,
  megagroup_size_max: 200000, forwarded_count_max: 100, online_update_period_ms: 120000, offline_blur_timeout_ms: 5000,
  offline_idle_timeout_ms: 30000, online_cloud_timeout_ms: 300000, notify_cloud_delay_ms: 30000, notify_default_delay_ms: 1500,
  push_chat_period_ms: 60000, push_chat_limit: 2, edit_time_limit: 172800, revoke_time_limit: 2147483647,
  revoke_pm_time_limit: 2147483647, rating_e_decay: 2419200, stickers_recent_limit: 200, channels_read_media_period: 604800,
  call_receive_timeout_ms: 20000, call_ring_timeout_ms: 90000, call_connect_timeout_ms: 30000, call_packet_timeout_ms: 10000,
  me_url_prefix: 'https://t.me/', caption_length_max: 1024, message_length_max: 4096, webfile_dc_id: 2});
H['help.getAppConfig'] = () => ({_: 'help.appConfig', hash: 0, config: {_: 'jsonObject', value: []}});
H['langpack.getDifference'] = H['langpack.getLangPack'] = (p) => ({_: 'langPackDifference', lang_code: p.lang_code || 'en', from_version: 0, version: 0, strings: []});

// -- auth  [WA] later: sendCode -> request pairing code / QR, signIn -> resolves when Baileys 'connection: open'
H['auth.sendCode'] = (p) => ({_: 'auth.sentCode', pFlags: {}, type: {_: 'auth.sentCodeTypeApp', length: LOGIN_CODE.length},
  phone_code_hash: 'sim-' + p.phone_number, timeout: 60});
H['auth.resendCode'] = H['auth.sendCode'];
H['auth.cancelCode'] = () => true;
H['auth.signIn'] = (p) => {
  if(p.phone_code !== LOGIN_CODE) throw fail(400, 'PHONE_CODE_INVALID');
  return {_: 'auth.authorization', pFlags: {}, user: tlUser(SELF)};
};
H['auth.exportLoginToken'] = () => ({_: 'auth.loginToken', expires: now() + 30, token: new Uint8Array(32).fill(7)});
H['auth.logOut'] = () => ({_: 'auth.loggedOut', pFlags: {}});

// -- users / contacts
const inputUserId = (u) => (u._ === 'inputUserSelf' ? SELF : u.user_id);
H['users.getUsers'] = (p) => p.id.map((u) => tlUser(inputUserId(u)));
H['users.getFullUser'] = (p) => {
  const id = inputUserId(p.id);
  return {_: 'users.userFull', full_user: {_: 'userFull', pFlags: {}, id, about: '', settings: {_: 'peerSettings', pFlags: {}},
    notify_settings: {_: 'peerNotifySettings', pFlags: {}}, common_chats_count: 0}, chats: [], users: [tlUser(id)]};
};
H['contacts.getContacts'] = () => {
  const ids = [...users.keys()].filter((i) => i !== SELF);
  return {_: 'contacts.contacts', contacts: ids.map((i) => ({_: 'contact', user_id: i, mutual: true})), saved_count: 0, users: ids.map(tlUser)};
};
H['contacts.resolveUsername'] = (p) => {
  const hit = [...users].find(([, u]) => u.username === p.username);
  if(!hit) throw fail(400, 'USERNAME_NOT_OCCUPIED');
  return {_: 'contacts.resolvedPeer', peer: {_: 'peerUser', user_id: hit[0]}, chats: [], users: [tlUser(hit[0])]};
};
H['contacts.search'] = (p) => {
  const q = (p.q || '').toLowerCase(), hits = [...users].filter(([i, u]) => i !== SELF && (u.first_name + ' ' + (u.last_name || '')).toLowerCase().includes(q));
  return {_: 'contacts.found', my_results: hits.map(([i]) => ({_: 'peerUser', user_id: i})), results: [], chats: [], users: hits.map(([i]) => tlUser(i))};
};
H['contacts.getTopPeers'] = () => ({_: 'contacts.topPeersDisabled'});

// -- updates
H['updates.getState'] = () => state();
H['updates.getDifference'] = () => ({_: 'updates.difference', new_messages: [], new_encrypted_messages: [], other_updates: [], chats: [], users: [], state: state()});

// -- dialogs / history
H['messages.getDialogs'] = (p) => (p.folder_id === 1 || p.offset_date > 0 ? dialogsResult([]) :
  dialogsResult(keys().sort((a, b) => msgsOf(b).pop().id - msgsOf(a).pop().id)));
H['messages.getPinnedDialogs'] = () => dialogsResult([], 'messages.peerDialogs');
H['messages.getPeerDialogs'] = (p) => dialogsResult(p.peers.map((x) => peerKeyOf(x.peer)).filter(Boolean), 'messages.peerDialogs');
H['messages.getDialogFilters'] = () => ({_: 'messages.dialogFilters', pFlags: {}, filters: []});
H['messages.getHistory'] = (p) => {
  const key = peerKeyOf(p.peer);
  let list = msgsOf(key);
  if(p.offset_id > 0) list = list.filter((m) => m.id < p.offset_id);
  if(p.max_id > 0) list = list.filter((m) => m.id < p.max_id);
  if(p.min_id > 0) list = list.filter((m) => m.id > p.min_id);
  const total = msgsOf(key).length, page = list.slice(-(p.limit || 50)).reverse();
  return {_: 'messages.messagesSlice', pFlags: {}, count: total, messages: page.map(tlMessage), topics: [], ...entities(page, [key])};
};
H['messages.getMessages'] = (p) => {
  const ids = new Set(p.id.map((x) => x.id)), page = msgs.filter((m) => ids.has(m.id));
  return {_: 'messages.messages', messages: page.map(tlMessage), topics: [], ...entities(page)};
};

// -- sending  [WA] later: await sock.sendMessage(jid, {text}); then build the same update from its result
H['messages.sendMessage'] = (p) => {
  const key = peerKeyOf(p.peer);
  if(!key) throw fail(400, 'PEER_ID_INVALID');
  const reply = p.reply_to?.reply_to_msg_id;
  const m = addMessage(key, SELF, p.message, now(), {reply_to: reply});
  if(key[0] === 'u' && key !== 'u' + SELF) simulateReply(key, p.message); // fake contact answers
  return updatesOf([{_: 'updateMessageID', id: m.id, random_id: p.random_id},
    {_: 'updateNewMessage', message: tlMessage(m), pts, pts_count: 1}], [m]);
};
H['messages.editMessage'] = (p) => {
  const m = msgs.find((x) => x.id === p.id);
  if(!m) throw fail(400, 'MESSAGE_ID_INVALID');
  m.text = p.message ?? m.text; m.edit_date = now(); pts++;
  return updatesOf([{_: 'updateEditMessage', message: tlMessage(m), pts, pts_count: 1}], [m]);
};
H['messages.deleteMessages'] = (p) => {
  const ids = new Set(p.id);
  for(let i = msgs.length - 1; i >= 0; i--) if(ids.has(msgs[i].id)) msgs.splice(i, 1);
  return {_: 'messages.affectedMessages', pts: ++pts, pts_count: p.id.length};
};
H['messages.readHistory'] = (p) => {
  const key = peerKeyOf(p.peer), rs = readState[key] || (readState[key] = {inbox: 0, outbox: 0});
  rs.inbox = Math.max(rs.inbox, p.max_id || lastId);
  return {_: 'messages.affectedMessages', pts: ++pts, pts_count: 1};
};
H['messages.setTyping'] = () => true;
H['messages.getAllDrafts'] = () => updatesOf([]);
H['upload.getFile'] = () => ({_: 'upload.file', type: {_: 'storage.fileUnknown'}, mtime: now(), bytes: new Uint8Array(0)});

/** Fake contact: "typing…" then echoes. [WA] delete this; real replies arrive via Baileys messages.upsert */
function simulateReply(key, text) {
  const uid = +key.slice(1);
  setTimeout(() => push({_: 'updateShort', update: {_: 'updateUserTyping', pFlags: {}, user_id: uid, action: {_: 'sendMessageTypingAction'}}, date: now()}), 600);
  setTimeout(() => pushIncoming(key, uid, 'Echo: ' + text), 1800);
}

/* ───────────────────────────── fallbacks for everything else ───────────────────────────── */
const EMPTY = () => ({
  'Updates': updatesOf([]),
  'messages.Dialogs': dialogsResult([]),
  'messages.Messages': {_: 'messages.messages', messages: [], topics: [], chats: [], users: []},
  'messages.PeerDialogs': dialogsResult([], 'messages.peerDialogs'),
  'contacts.Contacts': {_: 'contacts.contacts', contacts: [], saved_count: 0, users: []},
  'messages.AllStickers': {_: 'messages.allStickers', hash: 0, sets: []},
  'messages.RecentStickers': {_: 'messages.recentStickers', hash: 0, packs: [], stickers: [], dates: []},
  'messages.SavedGifs': {_: 'messages.savedGifs', hash: 0, gifs: []},
  'messages.FeaturedStickers': {_: 'messages.featuredStickers', pFlags: {}, hash: 0, count: 0, sets: [], unread: []},
  'messages.FavedStickers': {_: 'messages.favedStickers', hash: 0, packs: [], stickers: []},
  'messages.AvailableReactions': {_: 'messages.availableReactions', hash: 0, reactions: []},
  'messages.Reactions': {_: 'messages.reactions', hash: 0, reactions: []},
  'account.PrivacyRules': {_: 'account.privacyRules', rules: [{_: 'privacyValueAllowAll'}], chats: [], users: []},
  'account.WallPapers': {_: 'account.wallPapers', hash: 0, wallpapers: []},
  'account.Themes': {_: 'account.themes', hash: 0, themes: []},
  'help.CountriesList': {_: 'help.countriesList', countries: [], hash: 0},
  'help.PromoData': {_: 'help.promoDataEmpty', expires: now() + 86400},
  'help.TermsOfServiceUpdate': {_: 'help.termsOfServiceUpdateEmpty', expires: now() + 86400},
  'PeerNotifySettings': {_: 'peerNotifySettings', pFlags: {}}
});
const unhandled = {};
function fallback(method) {
  unhandled[method] = (unhandled[method] || 0) + 1;
  const type = SCHEMA?.method[method]?.type;
  if(unhandled[method] === 1) log(`~ unhandled ${method} -> ${type || '?'}`);
  if(!type) return true;
  return EMPTY()[type] ?? synth(type) ?? true;
}

/* ───────────────────────────── HTTP + WS ───────────────────────────── */
function jsonReplacer(_k, v) {
  if(v instanceof Uint8Array) return {__b64: Buffer.from(v).toString('base64')};
  if(typeof v === 'bigint') return v.toString();
  return v;
}
const jsonReviver = (_k, v) => (v && typeof v === 'object' && typeof v.__b64 === 'string' ? new Uint8Array(Buffer.from(v.__b64, 'base64')) : v);

const app = express();
app.use((req, res, next) => {
  res.set({'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'});
  req.method === 'OPTIONS' ? res.sendStatus(204) : next();
});
app.use(express.json({limit: '100mb', reviver: jsonReviver}));

app.post('/api', async(req, res) => {
  const {method, params = {}} = req.body || {};
  try {
    const result = H[method] ? await H[method](params) : fallback(method);
    log(`${H[method] ? '·' : '~'} ${method}`);
    res.type('json').send(JSON.stringify({result}, jsonReplacer));
  } catch(e) {
    log(`! ${method} -> ${e.type || e.message}`);
    res.type('json').send(JSON.stringify({error: {code: e.code || 500, type: e.type || 'INTERNAL', message: e.message}}));
  }
});
app.get('/debug/unhandled', (_req, res) => res.json(unhandled));
app.get('/', (_req, res) => res.send('tweb simulated backend: POST /api, WS /updates, GET /debug/unhandled'));

const server = http.createServer(app);
new WebSocketServer({server, path: '/updates'}).on('connection', (ws) => {
  sockets.add(ws);
  ws.on('close', () => sockets.delete(ws));
  log('updates socket connected');
});

/* seed */
const t = now() - 3600;
addMessage('u1001', 1001, 'Hey! Welcome to the simulated backend.', t);
addMessage('u1001', SELF, 'Thanks, testing the UI.', t + 60);
addMessage('u1001', 1001, 'Say something and I will echo it.', t + 120);
addMessage('u1002', 1002, 'Lunch tomorrow?', t + 1800);
addMessage('c2001', 1002, 'Welcome to the group', t + 2400);
addMessage('c2001', 1001, 'Hi all', t + 2460);
addMessage('u1000', SELF, 'Saved note', t + 3000);
readState['u1001'] = {inbox: lastId - 6 + 1, outbox: 0}; // first message read, last one unread

server.listen(PORT, () => log(`simulated backend on http://localhost:${PORT}  (login code ${LOGIN_CODE})`));
