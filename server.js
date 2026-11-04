
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
 * Login: real WhatsApp pairing (phone number -> 8-char code shown in the UI -> enter it in WhatsApp > Linked devices).
 *
 * Everything marked  [WA]  is where Baileys plugs in later.
 */
const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
let compression = null;
try { compression = require('compression'); } catch(_) { /* optional: npm i compression */ }
const {WebSocketServer} = require('ws');
const WA = require('./wa-auth'); // real WhatsApp login (Baileys)
const createBridge = require('./wa-bridge'); // WhatsApp chats <-> this store

const PORT = +process.env.PORT || 3000;
const TWEB_DIR = process.env.TWEB_DIR || path.join(__dirname, 'tweb');
const SELF = 1000;
const acct = {premium: process.env.PREMIUM !== '0', stars: +process.env.STARS || 5000}; // PREMIUM=0 to start as a free user
const now = () => Math.floor(Date.now() / 1000);
const LOG_REQ = process.env.LOG_REQ === '1'; // set LOG_REQ=1 to log every API call
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
const savedGifts = new Map(); // userId -> [{msg_id, from, date, gift, message, convert_stars, saved}]
const readState = {}; // peerKey -> {inbox, outbox}
let lastId = 0, pts = 0;

const peerKeyOf = (p) => p && (p._ === 'inputPeerSelf' ? 'u' + SELF : p._ === 'inputPeerUser' || p._ === 'peerUser' ? 'u' + p.user_id : p._ === 'inputPeerChat' || p._ === 'peerChat' ? 'c' + p.chat_id : null);
const peerOf = (key) => key[0] === 'u' ? {_: 'peerUser', user_id: +key.slice(1)} : {_: 'peerChat', chat_id: +key.slice(1)};
const keys = () => [...index().keys()];
// per-chat index, rebuilt lazily when messages were added/removed/renumbered (touch()) - msgs.filter per call does not scale to real accounts
let msgVer = 0, idxVer = -1, idxLen = -1, idx = new Map();
const touch = () => { msgVer++; };
const index = () => {
  if(idxVer !== msgVer || idxLen !== msgs.length) {
    idx = new Map();
    for(const m of msgs) (idx.get(m.key) || idx.set(m.key, []).get(m.key)).push(m);
    idxVer = msgVer; idxLen = msgs.length;
  }
  return idx;
};
const msgsOf = (key) => (index().get(key) || []).slice();
const lastOf = (key) => { const l = index().get(key); return l && l[l.length - 1]; };

function addMessage(key, from, text, date = now(), extra = {}) {
  const m = {id: ++lastId, key, from, text, date, out: from === SELF, ...extra};
  msgs.push(m);
  pts++;
  m.pts = pts; // lets updates.getDifference hand back what a client missed
  return m;
}

/* TL builders */
const flags = (o) => Object.fromEntries(Object.entries(o).filter(([, v]) => v));
const tlUser = (id) => {
  const u = users.get(id) || {first_name: 'User ' + id, last_name: '', phone: String(id)};
  return {_: 'user', pFlags: flags({self: id === SELF, contact: id !== SELF && u.contact !== false, mutual_contact: id !== SELF && u.contact !== false, premium: id === SELF ? acct.premium : !!u.premium}), id, access_hash: '1',
    first_name: u.first_name, last_name: u.last_name || '', ...(u.username && {username: u.username}), ...(u.emoji_status && {emoji_status: u.emoji_status}), phone: u.phone,
    ...(u.pic?.id && {photo: {_: 'userProfilePhoto', pFlags: {}, photo_id: String(u.pic.id), dc_id: 2}}),
    status: u.status || {_: 'userStatusRecently', pFlags: {}}};
};
// tweb's hasRights() answers false for a chat with no default_banned_rights/admin_rights => "Sending messages is not allowed in this group".
// WhatsApp "only admins can send" groups (g.announce) ban sending for everybody except admins (g.selfAdmin).
const SEND_BANS = ['send_messages', 'send_plain', 'send_media', 'send_photos', 'send_videos', 'send_roundvideos', 'send_audios',
  'send_voices', 'send_docs', 'send_stickers', 'send_gifs', 'send_games', 'send_inline', 'embed_links', 'send_polls'];
const tlChat = (id) => {
  const g = groups.get(id), muted = !!(g.announce && !g.selfAdmin);
  return {_: 'chat', pFlags: {}, id, title: g.title, photo: g.pic?.id ? {_: 'chatPhoto', pFlags: {}, photo_id: String(g.pic.id), dc_id: 2} : {_: 'chatPhotoEmpty'}, participants_count: g.members.length, date: now(), version: 1,
    default_banned_rights: {_: 'chatBannedRights', pFlags: muted ? Object.fromEntries(SEND_BANS.map((k) => [k, true])) : {}, until_date: 0}};
};
const tlService = (m) => ({_: 'messageService', pFlags: flags({out: m.out}), id: m.id, from_id: {_: 'peerUser', user_id: m.from},
  peer_id: peerOf(m.key), date: m.date, action: m.action});
// WhatsApp tags ("@<number>") are turned into tweb mention entities by the bridge at render time, so the shown name is always current
let renderMentions = null;
const msgBody = (m) => {
  const r = m.mentions?.length && renderMentions ? renderMentions(m) : null;
  return r ? {message: r.text, ...(r.entities.length && {entities: r.entities})} : {message: m.text};
};
// Telegram "@" button: a message is an unread mention while it tags me (or replies to me) in a group and neither the chat nor the mention was read
const isUnreadMention = (m) => !!m.mentionsMe && !m.out && !m.mentionRead && m.id > (readState[m.key]?.inbox || 0);
// -- WhatsApp media -> Telegram photo / document objects (bytes are fetched lazily by upload.getFile)
const b64of = (v) => ({__b64: v || ''});
function tlMedia(m) {
  const d = m.media;
  if(!d) return null;
  const base = {id: String(m.id), access_hash: '1', file_reference: b64of(''), date: m.date, dc_id: 2};
  const r = d.w && d.h ? d.w / d.h : 1, tw = r >= 1 ? 100 : Math.round(100 * r), th = r >= 1 ? Math.round(100 / r) : 100;
  // tweb only accepts a webp document as a sticker when it has thumbs (browsers that cannot encode webp: Safari, Firefox, most webviews) -> always give stickers one
  const PIXEL = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
  const thumbs = d.thumb ? [{_: 'photoCachedSize', type: 'm', w: tw, h: th, bytes: b64of(d.thumb)}]
    : d.type === 'sticker' ? [{_: 'photoCachedSize', type: 'm', w: 100, h: 100, bytes: b64of(PIXEL)}] : [];
  if(d.type === 'image') {
    return {_: 'messageMediaPhoto', pFlags: {}, photo: {_: 'photo', pFlags: {}, ...base,
      sizes: [...thumbs, {_: 'photoSize', type: 'x', w: d.w || 1280, h: d.h || 960, size: d.size || 100000}]}};
  }
  const attrs = [];
  if(d.type === 'sticker') attrs.push({_: 'documentAttributeImageSize', w: d.w || 512, h: d.h || 512}, {_: 'documentAttributeSticker', pFlags: {}, alt: '', stickerset: {_: 'inputStickerSetEmpty'}});
  else if(d.type === 'video') {
    attrs.push({_: 'documentAttributeVideo', pFlags: flags({round_message: d.round, supports_streaming: true}), duration: d.dur || 0, w: d.w || 640, h: d.h || 480});
    if(d.anim) attrs.push({_: 'documentAttributeAnimated'});
  } else if(d.type === 'audio') attrs.push({_: 'documentAttributeAudio', pFlags: flags({voice: d.ptt}), duration: d.dur || 0, ...(d.wave && {waveform: b64of(d.wave)})});
  if(d.type === 'document' || d.name) attrs.push({_: 'documentAttributeFilename', file_name: d.name || 'file'});
  return {_: 'messageMediaDocument', pFlags: flags({video: d.type === 'video' && !d.anim, round: d.round, voice: d.ptt}),
    document: {_: 'document', pFlags: {}, ...base, mime_type: d.type === 'sticker' ? 'image/webp' : (d.mime || (d.type === 'video' ? 'video/mp4' : d.type === 'audio' ? 'audio/ogg' : 'application/octet-stream')), size: d.size || 0, // tweb only treats exactly image/webp + sticker attribute as a sticker
      ...(thumbs.length && {thumbs}), attributes: attrs}};
}
const tlMessage = (m) => m.action ? tlService(m) : ({_: 'message', pFlags: flags({out: m.out, mentioned: m.mentionsMe && !m.out, media_unread: isUnreadMention(m)}), id: m.id, from_id: {_: 'peerUser', user_id: m.from},
  peer_id: peerOf(m.key), date: m.date, ...msgBody(m), ...(m.media && {media: tlMedia(m)}), ...(m.edit_date && {edit_date: m.edit_date}),
  ...(m.reply_to && {reply_to: {_: 'messageReplyHeader', pFlags: {}, reply_to_msg_id: m.reply_to}})});
const tlDialog = (key) => {
  const list = msgsOf(key), top = list[list.length - 1], rs = readState[key] || (readState[key] = {inbox: 0, outbox: 0});
  return {_: 'dialog', pFlags: {}, peer: peerOf(key), top_message: top ? top.id : 0, read_inbox_max_id: rs.inbox,
    read_outbox_max_id: rs.outbox, unread_count: list.filter((m) => !m.out && m.id > rs.inbox).length,
    unread_mentions_count: list.filter(isUnreadMention).length, unread_reactions_count: 0, unread_poll_votes_count: 0,
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
  const dialogs = ks.map(tlDialog), tops = ks.map(lastOf).filter(Boolean);
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
const bridge = createBridge({users, groups, msgs, readState, SELF, push, updatesOf, tlMessage, tlChat, tlUser, addMessage, touch, WA, fail, now, log,
  getLastId: () => lastId, setLastId: (n) => { lastId = n; }, getPts: () => pts, setPts: (n) => { pts = n; }, bumpPts: () => ++pts});
renderMentions = bridge.render;
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
const jb = (key, value) => ({_: 'jsonObjectValue', key, value: {_: 'jsonBool', value}});
const jn = (key, value) => ({_: 'jsonObjectValue', key, value: {_: 'jsonNumber', value}});
H['help.getAppConfig'] = () => ({_: 'help.appConfig', hash: 0, config: {_: 'jsonObject', value: [
  jb('premium_purchase_blocked', false), jb('stargifts_blocked', false), jn('stargifts_convert_period_max', 7776000),
  jn('stargifts_message_length_max', 255), jn('stargifts_pinned_to_top_limit', 6), jn('stars_gifts_enabled', 1),
  jn('stickers_faved_limit_default', 200), jn('stickers_faved_limit_premium', 200)]}});
H['langpack.getDifference'] = H['langpack.getLangPack'] = (p) => ({_: 'langPackDifference', lang_code: p.lang_code || 'en', from_version: 0, version: 0, strings: []});

// -- auth: real WhatsApp pairing through Baileys (see wa-auth.js)
// sendCode  -> starts a Baileys socket, asks WhatsApp for a pairing code and returns it INSIDE the sentCode
//              (WhatsApp shows nothing to type: the user types THIS code on the phone, so the UI displays it)
// signIn    -> polled by the UI's waPairing card; answers WA_PAIRING_PENDING until Baileys reports 'open'
function applyWaUser(entry) {
  const u = entry.user || {};
  const phone = u.phone || entry.phone;
  const {username, ...rest} = users.get(SELF) || {}; // drop the simulated @me
  users.set(SELF, {...rest, first_name: u.name || phone, last_name: '', phone}); // [WA] id stays SELF for now
  push(updatesOf([{_: 'updateUser', user_id: SELF}]));
}
WA.init({
  onOpen: (entry) => { applyWaUser(entry); bridge.onOpen(entry); },
  onSocket: (entry, sock, baileys) => bridge.attach(entry, sock, baileys),
  getMessage: (key) => bridge.getMessage(key),
  cachedGroupMetadata: (jid) => bridge.groupMeta(jid),
  onLogout: () => log('WhatsApp session logged out from the phone')
});
const waAuthorization = () => ({_: 'auth.authorization', pFlags: {}, user: tlUser(SELF)});
const waSentCode = (entry, phone_number) => ({_: 'auth.sentCode', pFlags: {},
  type: {_: 'auth.sentCodeTypeWaPairing', length: 8, pairing_code: entry.code, expires_in: entry.ttl}, phone_code_hash: entry.hash, timeout: 0, phone_number});
async function waStart(phone_number) {
  const {entry, linked} = await WA.startPairing(phone_number);
  if(linked) { applyWaUser(entry); return {_: 'auth.sentCodeSuccess', authorization: waAuthorization()}; }
  return waSentCode(entry, phone_number);
}
H['auth.sendCode'] = (p) => waStart(p.phone_number);
H['auth.resendCode'] = async(p) => { await WA.cancel(p.phone_code_hash); return waStart(p.phone_number); };
H['auth.cancelCode'] = async(p) => { await WA.cancel(p.phone_code_hash); return true; };
H['auth.signIn'] = (p) => {
  const entry = WA.status(p.phone_code_hash);
  if(!entry) throw fail(400, 'PHONE_CODE_INVALID');
  switch(entry.status) {
    case 'open': applyWaUser(entry); return waAuthorization();
    case 'expired': throw fail(400, 'PHONE_CODE_EXPIRED');
    case 'failed': throw fail(400, 'PHONE_CODE_EXPIRED', 'WhatsApp closed the pairing');
    default: throw fail(400, 'WA_PAIRING_PENDING'); // connecting | pending
  }
};
H['auth.exportLoginToken'] = () => { throw fail(400, 'WA_QR_UNAVAILABLE'); }; // QR login: later
H['auth.logOut'] = async() => { await WA.logout(); return {_: 'auth.loggedOut', pFlags: {}}; };

// -- users / contacts
const inputUserId = (u) => (u._ === 'inputUserSelf' ? SELF : u.user_id);
H['users.getUsers'] = (p) => p.id.map((u) => tlUser(inputUserId(u)));
H['users.getFullUser'] = (p) => {
  const id = inputUserId(p.id);
  return {_: 'users.userFull', full_user: {_: 'userFull', pFlags: {display_gifts_button: true}, id, stargifts_count: visibleGifts(id), about: '', settings: {_: 'peerSettings', pFlags: {}},
    notify_settings: {_: 'peerNotifySettings', pFlags: {}}, common_chats_count: 0}, chats: [], users: [tlUser(id)]};
};
H['contacts.getContacts'] = () => {
  const ids = [...users].filter(([i, u]) => i !== SELF && u.contact !== false).map(([i]) => i);
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
H['updates.getDifference'] = (p) => { // a pts hole on the client (answer and push crossed, ws reconnect) must not lose messages
  const since = p.pts || 0, min = now() - 6 * 3600;
  const fresh = msgs.filter((m) => m.pts > since && m.date >= min).slice(-300);
  return {_: 'updates.difference', new_messages: fresh.map(tlMessage), new_encrypted_messages: [], other_updates: [], ...entities(fresh), state: state()};
};

// -- dialogs / history
H['messages.getDialogs'] = (p) => {
  if(p.folder_id === 1) return dialogsResult([]);
  let ks = keys().sort((a, b) => lastOf(b).id - lastOf(a).id);
  if(p.offset_id > 0) ks = ks.filter((k) => lastOf(k).id < p.offset_id);
  else if(p.offset_date > 0) ks = ks.filter((k) => lastOf(k).date < p.offset_date);
  const limit = p.limit > 0 ? p.limit : 100, page = ks.slice(0, limit);
  const r = dialogsResult(page);
  if(ks.length > limit) { r._ = 'messages.dialogsSlice'; r.count = keys().length; }
  return r;
};
H['messages.getPinnedDialogs'] = () => dialogsResult([], 'messages.peerDialogs');
H['messages.getPeerDialogs'] = (p) => dialogsResult(p.peers.map((x) => peerKeyOf(x.peer)).filter(Boolean), 'messages.peerDialogs');
H['messages.getDialogFilters'] = () => ({_: 'messages.dialogFilters', pFlags: {}, filters: []});
H['messages.getHistory'] = (p) => {
  const key = peerKeyOf(p.peer);
  if(key) bridge.openChat(key);
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
H['messages.sendMessage'] = async(p) => {
  const key = peerKeyOf(p.peer);
  if(!key) throw fail(400, 'PEER_ID_INVALID');
  if(bridge.active()) {
    const wm = await bridge.send(key, p);
    return updatesOf([{_: 'updateMessageID', id: wm.id, random_id: p.random_id},
      {_: 'updateNewMessage', message: tlMessage(wm), pts: 0, pts_count: 0}], [wm]);
  }
  const reply = p.reply_to?.reply_to_msg_id;
  const m = addMessage(key, SELF, p.message, now(), {reply_to: reply});
  if(key[0] === 'u' && key !== 'u' + SELF) simulateReply(key, p.message); // fake contact answers
  return updatesOf([{_: 'updateMessageID', id: m.id, random_id: p.random_id},
    {_: 'updateNewMessage', message: tlMessage(m), pts, pts_count: 1}], [m]);
};
H['messages.editMessage'] = async(p) => {
  const m = msgs.find((x) => x.id === p.id);
  if(!m) throw fail(400, 'MESSAGE_ID_INVALID');
  if(bridge.active() && m.wa) await bridge.edit(m, p.message ?? m.text, p.entities);
  else { m.text = p.message ?? m.text; m.edit_date = now(); pts++; }
  return updatesOf([{_: 'updateEditMessage', message: tlMessage(m), pts, pts_count: 1}], [m]);
};
H['messages.deleteMessages'] = async(p) => {
  const ids = new Set(p.id);
  if(bridge.active()) await bridge.del(msgs.filter((m) => ids.has(m.id)), !!p.revoke);
  else for(let i = msgs.length - 1; i >= 0; i--) if(ids.has(msgs[i].id)) msgs.splice(i, 1);
  return {_: 'messages.affectedMessages', pts: ++pts, pts_count: p.id.length};
};
H['messages.readHistory'] = (p) => {
  const key = peerKeyOf(p.peer), rs = readState[key] || (readState[key] = {inbox: 0, outbox: 0});
  if(bridge.active()) bridge.markRead(key, p.max_id);
  rs.inbox = Math.max(rs.inbox, p.max_id || lastId);
  return {_: 'messages.affectedMessages', pts: ++pts, pts_count: 1};
};
// -- "@" button (tags / replies to me)
// TL paging: newest first; offset_id = start after this id, add_offset shifts the window (tweb sends -50 to also get the ones above the offset)
const tlWindow = (desc, p) => {
  const i = p.offset_id > 0 ? desc.findIndex((m) => m.id < p.offset_id) : 0, at = i < 0 ? desc.length : i;
  const start = Math.max(0, at + (p.add_offset || 0));
  return desc.slice(start, start + (p.limit || 20));
};
H['messages.getUnreadMentions'] = (p) => {
  const key = peerKeyOf(p.peer);
  let list = key ? msgsOf(key).filter(isUnreadMention) : [];
  if(p.max_id > 0) list = list.filter((m) => m.id < p.max_id);
  if(p.min_id > 0) list = list.filter((m) => m.id > p.min_id);
  const page = tlWindow([...list].reverse(), p);
  return {_: 'messages.messagesSlice', pFlags: {}, count: list.length, messages: page.map(tlMessage), topics: [], ...entities(page, key ? [key] : [])};
};
H['messages.readMentions'] = (p) => {
  const key = peerKeyOf(p.peer);
  for(const m of key ? msgsOf(key) : []) if(isUnreadMention(m)) m.mentionRead = true;
  bridge.markDirty();
  return {_: 'messages.affectedHistory', pts: ++pts, pts_count: 1, offset: 0};
};
H['messages.readMessageContents'] = (p) => {
  const ids = new Set(p.id || []);
  for(const m of msgs) if(ids.has(m.id) && m.mentionsMe) m.mentionRead = true;
  bridge.markDirty();
  return {_: 'messages.affectedMessages', pts: ++pts, pts_count: (p.id || []).length};
};
H['messages.getUnreadReactions'] = () => noMsgs();
H['messages.setTyping'] = (p) => { const key = peerKeyOf(p.peer); if(key) bridge.setTyping(key, p.action); return true; };
H['messages.getAllDrafts'] = () => updatesOf([]);

/** Fake contact: "typing…" then echoes. [WA] delete this; real replies arrive via Baileys messages.upsert */
function simulateReply(key, text) {
  const uid = +key.slice(1);
  setTimeout(() => push({_: 'updateShort', update: {_: 'updateUserTyping', pFlags: {}, user_id: uid, action: {_: 'sendMessageTypingAction'}}, date: now()}), 600);
  setTimeout(() => pushIncoming(key, uid, 'Echo: ' + text), 1800);
}

/* ───────────────────────────── previously-unhandled methods ───────────────────────────── */
const NS = () => ({_: 'peerNotifySettings', pFlags: {}});
const noMsgs = () => ({_: 'messages.messages', messages: [], topics: [], chats: [], users: []});
const noStories = () => ({_: 'stories.stories', pFlags: {}, count: 0, stories: [], chats: [], users: []});
const privacyAll = () => ({_: 'account.privacyRules', rules: [{_: 'privacyValueAllowAll'}], chats: [], users: []});
const peerFromInput = (p) => { const k = peerKeyOf(p); return k ? peerOf(k) : {_: 'peerUser', user_id: SELF}; };

// -- login screen / langpack
H['help.getCountriesList'] = () => ({_: 'help.countriesList', hash: 1, countries: [
  ['US', 'United States', '1'], ['GB', 'United Kingdom', '44'], ['NG', 'Nigeria', '234'], ['IN', 'India', '91'], ['BR', 'Brazil', '55'],
  ['DE', 'Germany', '49'], ['FR', 'France', '33'], ['ZA', 'South Africa', '27'], ['KE', 'Kenya', '254'], ['GH', 'Ghana', '233']
].map(([iso2, name, cc]) => ({_: 'help.country', pFlags: {}, iso2, default_name: name, name,
  country_codes: [{_: 'help.countryCode', pFlags: {}, country_code: cc, prefixes: [], patterns: []}]}))});
H['langpack.getLanguage'] = (p) => ({_: 'langPackLanguage', pFlags: {}, name: 'English', native_name: 'English', lang_code: p.lang_code || 'en',
  plural_code: 'en', strings_count: 0, translated_count: 0, translations_url: ''});
H['langpack.getStrings'] = () => [];
H['auth.initPasskeyLogin'] = () => { throw fail(400, 'PASSKEY_LOGIN_UNAVAILABLE'); };
H['help.getTimezonesList'] = () => ({_: 'help.timezonesList', hash: 1, timezones: [{_: 'timezone', id: 'UTC', name: 'UTC', utc_offset: 0}]});
H['help.getPeerColors'] = H['help.getPeerProfileColors'] = () => ({_: 'help.peerColors', hash: 1, colors: []});
H['help.getPromoData'] = () => ({_: 'help.promoDataEmpty', expires: now() + 86400});
H['help.getPremiumPromo'] = () => ({_: 'help.premiumPromo', status_text: acct.premium ? 'You already have Telegram Premium (simulated).' : 'Unlock exclusive features.',
  status_entities: [], video_sections: [], videos: [], users: [],
  period_options: [[1, 499], [6, 2399], [12, 3999]].map(([months, amount]) => ({_: 'premiumSubscriptionOption', pFlags: flags({current: acct.premium && months === 1}),
    months, currency: 'USD', amount: String(amount), bot_url: 'https://t.me/PremiumBot?start=sim'}))});

// -- account
H['account.getContentSettings'] = () => ({_: 'account.contentSettings', pFlags: {}});
H['account.getGlobalPrivacySettings'] = () => ({_: 'globalPrivacySettings', pFlags: {}});
H['account.getAuthorizations'] = () => ({_: 'account.authorizations', authorization_ttl_days: 365, authorizations: [{_: 'authorization',
  pFlags: {current: true, official_app: true}, hash: '0', device_model: 'Web', platform: 'Web', system_version: '1', api_id: 1,
  app_name: 'Telegram Web', app_version: '1.0', date_created: now() - 3600, date_active: now(), ip: '127.0.0.1', country: '', region: ''}]});
H['account.getWebAuthorizations'] = () => ({_: 'account.webAuthorizations', authorizations: [], users: []});
H['account.getConnectedBots'] = () => ({_: 'account.connectedBots', connected_bots: [], users: []});
H['account.getNotifySettings'] = NS;
H['account.getNotifyExceptions'] = () => updatesOf([]);
H['account.getContactSignUpNotification'] = () => false;
H['account.getReactionsNotifySettings'] = () => ({_: 'reactionsNotifySettings', pFlags: {}, sound: {_: 'notificationSoundDefault'}, show_previews: true});
H['account.getPrivacy'] = privacyAll;
H['account.getSavedMusicIds'] = () => ({_: 'account.savedMusicIds', ids: []});
H['account.getPasskeys'] = () => ({_: 'account.passkeys', passkeys: []});
H['account.getPassword'] = () => ({_: 'account.password', pFlags: {}, new_algo: {_: 'passwordKdfAlgoUnknown'},
  new_secure_algo: {_: 'securePasswordKdfAlgoUnknown'}, secure_random: new Uint8Array(32)});
H['account.getAccountTTL'] = () => ({_: 'accountDaysTTL', days: 365});
H['account.getWallPapers'] = () => ({_: 'account.wallPapers', hash: 0, wallpapers: []});
H['account.getThemes'] = () => ({_: 'account.themes', hash: 0, themes: []});
H['account.updateStatus'] = (p) => { bridge.setOnline(!p.offline); return true; };
H['account.checkUsername'] = (p) => ![...users].some(([i, u]) => i !== SELF && u.username === p.username);
H['account.updateUsername'] = (p) => { users.get(SELF).username = p.username || undefined; return tlUser(SELF); };
H['account.updateProfile'] = (p) => {
  const u = users.get(SELF);
  if(p.first_name !== undefined) u.first_name = p.first_name;
  if(p.last_name !== undefined) u.last_name = p.last_name;
  return tlUser(SELF);
};

// -- contacts
H['contacts.getBirthdays'] = () => ({_: 'contacts.contactBirthdays', contacts: [], users: []});
H['contacts.getBlocked'] = () => ({_: 'contacts.blocked', blocked: [], chats: [], users: []});
H['contacts.getSponsoredPeers'] = () => ({_: 'contacts.sponsoredPeersEmpty'});

// -- stickers / gifs / reactions / bots
H['messages.getAttachMenuBots'] = () => ({_: 'attachMenuBots', hash: 0, bots: [], users: []});
H['messages.getSavedReactionTags'] = () => ({_: 'messages.savedReactionTags', tags: [], hash: 0});
H['messages.getSavedGifs'] = () => ({_: 'messages.savedGifs', hash: 0, gifs: []});
H['messages.getFavedStickers'] = () => ({_: 'messages.favedStickers', hash: 0, packs: [], stickers: []});
H['messages.getStickerSet'] = () => { throw fail(400, 'STICKERSET_INVALID'); };
H['messages.getStickers'] = H['messages.getEmojiStickers'] = (p) => (p.emoticon !== undefined ?
  {_: 'messages.stickers', hash: 0, stickers: []} : {_: 'messages.allStickers', hash: 0, sets: []});
H['messages.getAvailableReactions'] = () => ({_: 'messages.availableReactions', hash: 0, reactions: []});
H['messages.getTopReactions'] = () => ({_: 'messages.reactions', hash: 0, reactions: []});
H['messages.getEmojiKeywordsDifference'] = (p) => ({_: 'emojiKeywordsDifference', lang_code: p.lang_code || 'en', from_version: 0, version: 0, keywords: []});
H['messages.getPaidReactionPrivacy'] = () => updatesOf([]);

// -- messages misc
H['messages.receivedMessages'] = () => [];
H['messages.getPeerSettings'] = () => ({_: 'messages.peerSettings', settings: {_: 'peerSettings', pFlags: {}}, chats: [], users: []});
H['messages.getScheduledHistory'] = noMsgs;
H['messages.getSavedHistory'] = noMsgs;
H['messages.getSavedDialogs'] = () => ({_: 'messages.savedDialogs', dialogs: [], messages: [], chats: [], users: []});
H['messages.getDefaultHistoryTTL'] = () => ({_: 'defaultHistoryTTL', period: 0});
H['messages.getFullChat'] = (p) => {
  const g = groups.get(p.chat_id);
  if(!g) throw fail(400, 'CHAT_ID_INVALID');
  return {_: 'messages.chatFull', full_chat: {_: 'chatFull', pFlags: {}, id: p.chat_id, about: '',
    participants: {_: 'chatParticipants', chat_id: p.chat_id, version: 1,
      participants: g.members.map((u) => ({_: 'chatParticipant', user_id: u, inviter_id: SELF, date: now()}))},
    notify_settings: NS()}, chats: [tlChat(p.chat_id)], users: g.members.map(tlUser)};
};

// -- search (real, over the in-memory store). Media filters return nothing: only text exists for now.
const isMediaFilter = (f) => f && f._ !== 'inputMessagesFilterEmpty' && f._ !== 'inputMessagesFilterUrl';
function searchMsgs(p, key) {
  const mine = p.filter?._ === 'inputMessagesFilterMyMentions';
  if(!mine && isMediaFilter(p.filter)) return [];
  const q = (p.q || '').toLowerCase();
  let list = key ? msgsOf(key) : msgs;
  if(mine) list = list.filter((m) => m.mentionsMe && !m.out);
  if(q) list = list.filter((m) => m.text.toLowerCase().includes(q));
  if(p.filter?._ === 'inputMessagesFilterUrl') list = list.filter((m) => /https?:\/\//.test(m.text));
  if(p.from_id) { const f = peerKeyOf(p.from_id); if(f) list = list.filter((m) => 'u' + m.from === f); }
  if(p.min_date) list = list.filter((m) => m.date >= p.min_date);
  if(p.max_date) list = list.filter((m) => m.date <= p.max_date);
  if(p.max_id) list = list.filter((m) => m.id < p.max_id);
  if(p.min_id) list = list.filter((m) => m.id > p.min_id);
  return list;
}
function searchResult(list, p) {
  const total = list.length;
  let page = [...list].reverse();
  if(p.offset_id > 0) page = page.filter((m) => m.id < p.offset_id);
  page = page.slice(Math.max(0, p.add_offset || 0), (p.add_offset > 0 ? p.add_offset : 0) + (p.limit || 20));
  return {_: 'messages.messagesSlice', pFlags: {}, count: total, messages: page.map(tlMessage), topics: [], ...entities(page)};
}
H['messages.search'] = (p) => {
  const key = peerKeyOf(p.peer);
  return searchResult(searchMsgs(p, key), p);
};
H['messages.searchGlobal'] = (p) => searchResult(searchMsgs(p, null), {...p, add_offset: 0});
H['messages.getSearchCounters'] = (p) => p.filters.map((f) => ({_: 'messages.searchCounter', pFlags: {},
  filter: f, count: f._ === 'inputMessagesFilterEmpty' ? msgsOf(peerKeyOf(p.peer)).length : f._ === 'inputMessagesFilterMyMentions' ? msgsOf(peerKeyOf(p.peer)).filter((m) => m.mentionsMe && !m.out).length : 0}));

// -- stories / payments / misc new APIs
H['stories.getAllStories'] = () => ({_: 'stories.allStories', pFlags: {}, count: 0, state: '', peer_stories: [], chats: [], users: [],
  stealth_mode: {_: 'storiesStealthMode', pFlags: {}}});
H['stories.getPeerStories'] = (p) => ({_: 'stories.peerStories', stories: {_: 'peerStories', pFlags: {}, peer: peerFromInput(p.peer), stories: []}, chats: [], users: []});
H['stories.getPinnedStories'] = noStories;
H['stories.getStoriesArchive'] = noStories;
H['stories.getAlbums'] = () => ({_: 'stories.albums', hash: 0, albums: []});
H['payments.getPremiumGiftCodeOptions'] = () => [];
H['photos.getUserPhotos'] = () => ({_: 'photos.photos', photos: [], users: []});
H['channels.getChannelRecommendations'] = () => ({_: 'messages.chats', chats: []});
H['folders.editPeerFolders'] = () => updatesOf([]);
H['communities.getJoinedCommunities'] = () => ({_: 'messages.chats', chats: []});


/* ───────────────────────────── premium + star gifts ───────────────────────────── */
// Premium is just a flag on the user (user.pFlags.premium); tweb derives `rootScope.premium` from the self user.
// Gifts: the catalog below uses generated placeholder art. For REAL gifts see the note at the end of this block.
const GIFT_ART = [{"title": "Heart", "alt": "\u2764\ufe0f", "stars": 50, "limited": 0, "b64": "UklGRiAEAABXRUJQVlA4TBQEAAAv/8F/EA8wQLM+UvMf8PBb21Ze29q2DQoGJiEQCqFBaIRCCJgYFIYhCcboSPP8X1dE/yeA/u///yad7CUqM8xkZr9XmZlBGl/NFl8DBt+GnXbDBqHe8c688wjjwa0Vvk8AmR/jWn9gq1efeG0+eb2x4FYKPyc9XgwrbYGNVllJK2PFabUVNgu8GrTGknsqS0mLl8NTW2Kjk9fiU1+zOnWNn+aa1+kb9i7zetQZG+6ubCSdueHv6garZN6Md33HapSddDd2nEbd4bu54zX6lr1k3o0aY8tdylbSmFv+UrdYg7fDpe0ZubwXL33PypW9dBl7Tq7u8WXuebkmYIiI94NcF7BEWSDKDQFHVASS3BTwRFWA5VgwEDUJI5UlIlGXsFJFIhENCSdVJZhoSnipJmKIJcNbdRGbRaLUEHFFJElNEV9F+K1YNDQZI5NlYpexMkUmDRn3TlWGp4x/pyYkHGQ6UnyngZTeaSLxOzG0eaOMZd+oYLlfpGL5N2pY4RfpWPEXGVjpF5lY/EeEwc2fDxnN/vVU0Ny/j2Q0+9cToZk/ISYY/QEZWOkX6VjxF2lY4RepWP6NCpZ7o4YVJCqWlyhYTiJj2TciLCMyoUh0IKV36kjxnRpSkKlI/p0KknunjGRlCMkITSB6qYGTpDpOlGo44a0qjpcqOE4q41gpwjFiE4bEB0qS6yhRrqEEuYri5QqKk8soVo5QjMIEIcWBkTQ6RtSoGF6jYDiNjGE1CMOoTAhSHQhJpyPEd2sIQacieJ2C4HQygtUhBKM0Aejlhl7S6npRq+kFrarntYqe08p69u1Iz6hNNVIfWkmva0W9phX0qpZ/v6Ll9LKW1SMtAzCVCHDoJISuE7+g6QSEquMRio5DyDoWgXQMxFShTxgaCaNrRIymETCqhscoGg4ja9hvIA0DMhUIdMgllC4XUZpcQKly/iuKnEPJchaF5AzMFCPYIZVwulT8jiYVcKqUxylSDidLWRySMkBTiD5kyCSkLhORmkxAqjIeqcg4pCxjv4RkDNQUIeghkbC6RMRqEgGrSvhvKRIOK0tYLJIwYFOAwMdeQut78WvaXkCrex6t7Dm0vGfRaM/AzS36nLGT8PpOxGs7Aa/ueLyy4/Dyjv0e2jEHzA06cKylE/paPKGthRPqmv+isuZOyGv2BFozR8wlOnKspDP6SvymthLOqCv+jLLizsgr9gxaMYfMBfqo8ZRO6U/xlPYUTqlP/pTy5E7JT/ar6MkcMx/o2HGXzul38Zx2F86pd/67yp07J9/Zc+jOHDRv6OBxSSf1S/yydgkn1Ys/qVzcSfliT6KLOWoyM33aYE5ndeZ4VmMOZ1Vmf1ZhdmdlZvttxGwOm0yHj3Raj6e1cFr1X1fcadmeRua4//v/L1A="}, {"title": "Star", "alt": "\u2b50", "stars": 100, "limited": 0, "b64": "UklGRroDAABXRUJQVlA4TK4DAAAv/8F/EA8wxPM/KPMf8PDe2lYm27a1dcSS+IkDS2KZ8b0wDUzDFEzgJz8Q/UdmEr03goz1sT8i+j8B9H////1pTpft6Yo7Xd3Pn66F0/X47Y10usmn4xswZ8vM9mzlDtzZKrM/W7uDsFe/g3i2wZzONu+Az8bMbE6W78HuVO7Bnazeg9+p3UM4Wb+HuNO4h7TTvAc+Gd+E2SffhT1XuQu3T70Lv0/7FsI+/S7iPuNbSPvMu+B9+DbMLvl7sLuU+3C71O/B79LuI+zSjzc+xeOlXebxePF8Zo98J3aPcry64o7n92jH6yvheHGPcby5ks7Ey19ZXjMnKmv2eG6Hery25k/U18J5Cl+2YIMFI1JlYQczWTxhNFb1AKyu1RkwaGQGNWKDYaNMYWgrMBk8XRto8VpHC9cqmrtW0Oy1jGauERoJTqwkMbCiRMPyEhXLSRQsK5GxjARhkehAijIdKcg0JC9TkZxMQbIyGcnI0MRJJDxwolTHCVINx0tVHCdVcKwU4ZD4RElyAyXKdZQg11C8XEGxchnFyBEKKU6MpDEwokbHCBoVw2kUDKuRMYwGYZDqREg6HSHoNASvUxGcTkGwOhnB6BACKQ+9qNX1glbT81pVz2kVPatFeqQ+tZLe0Ip6XSvoNS2vV7WcXtYyeqRFgFMnIQydiNB1AkLVcQhFxyJkHYNAOgQ5NRLG0IgYTcNjVA2HUTQsRtYwGKRBoFMuoXS5gNLkPEqVcyhFzqJkOYNCUyoR7JCKOF0q4DQpj1OlHE6RsjgkRcBTJiENmfhNTZn0pFj4QWUp85yKlH1OVcr9ddKk/HPqUuE5Dan4nKZUek4s/piynPmrpMjZp1Tl3FNqcv4pdbnwjaWxw1iKfSnu4OtuNu9maLNENDaYK4GoraQNPFHdyxLlvYiINuDF9DIWGC+8tJ3cS93JvmS4vEJv54JBi+/GPuFd28e9K2hlwbzLCxaNPs5d4qe+i//UwOon96l8cmDmE22SaHHuEVf6Hn6lYbUPdqV88FhmhbZItDx3CGsdqr/za/VdgLJrZQe6uEG6MpDGm3Clv4lI/krFs1cyHl1Gmi/p2nhJQOFaQ3PXKpq9loH4lQQnMzNOlBhYQaJhOYkKk1+MRH4xMCQ6kaLMQPIyDcnJFJTCzEYmM7MFSSQ8caJUx/FSDaQyW6nC7ECMFMEkEp8oQa5jNPZylT2GlSsopAiSNCZEDxo9QDiNimE1CgapQvyu8zvCjzo/I/yg8xPC//3/73s="}, {"title": "Gift Box", "alt": "\ud83c\udf81", "stars": 250, "limited": 0, "b64": "UklGRpYCAABXRUJQVlA4TIoCAAAv/8F/EBcgIEn8D23uhkAgSXB/nOd//gP+ArKrbW/bBnQ377EEp9AIaXDKCJmCS+g+vWBNFXz/938/oVRE9B+CG0mSIjeoKWoWJef2zPwghR/Df8N/oxbPSslYljBelcPAsoS5fSRjWWIoJ2PCssS473RgWULMZ0xYlgD9czKWJdJcyoRlUbc77xbLIl+fl7EskepSJiyLeN1yi2URr1sylkX9R681sCzinaYJyyLdabvFskh32jKWJVKnFCxLzxTAhGUR3hJxi2Xpl2fIpy1jWYS3hH7tsCzCoP92jSzyCP/t5f+QsNp5Jm5nWu21kslirHZPiD9W658AliXLYuxtPNP26vRNPd8As8ii7O31jWVvG3hteia3/g0DWXpmd/qbCjzULRDuQkU62tYzz5hyzzTX4UiWvwT1//9e/9c9l/+6u//7ff3f7+/+cZ/4x93iH3eMf9w1/nHn+Mfd4593iH/eJfx5p/jn3eKfd+zAedcPFRzvC2l8RVMvRGuFB8tPOPXM8wP3RFLxQZxlzbS+wTuOvcHCrF03/Frdy2oZpC0trmgd50a1mGkd38beZKF1fBtfTaq/Sun4qzbvGLXNFYlv+dXfalSd6zoLeGev5VXzLOCD2ZP1R89qUVCNswhSe3/e29O+CNJdtfF6n2RRQbm5ry3HWVTQBvJ5G/7rhVXbfAlJ2q420Q9paRN9ULaQzjsKcD/xrOwqkRrCaOfdXfA6pIYLXodVykqJNnngzbS2A1afeeHjXIf/hv+G/4b/hv/+v2ZVNP9aSYquNtYPQWljfdCzsK165q2V9FzR/ZCTEn1DNQvfykbp8EsyRu1QkhFqj6HlKrlMXITZ50JlvoYfw3/Df382JA=="}, {"title": "Diamond", "alt": "\ud83d\udc8e", "stars": 500, "limited": 1, "b64": "UklGRqIFAABXRUJQVlA4TJUFAAAv/8F/EBcgEEjyN5tqDYFAktj+WAsEkvxFx1ggkES383YBf9UcBW3bMA5/3DsQImICzKkQLVL4I9m1bcdtoyAz+siEflgaS8AHX5uekvBu4NMR/YfCSJIjKQ+l6nbhmzXdheU2gBtGkkq7JmQcFF14UBUshSExXf6tTA5JRR9F9N8J20iSZB5at//xB/959593//kf7IPO12ednx8662w28k+30ecYNjaPywZ/Gp3secxO+0fRvuZxVoMdzzyOglVPA04w/1xwgJfm+v6YrtqnfDqQVTiaetajPI6qjiaegSaf8gxL9X2aqKpvkH6UZ2gqAk1MjvIVVicTzJeLDBCpJvp+m6JqBFQ7Sy8sMXnejvICYXTMz9IPC3NK43x7JqeKswzCQSWcovxVeiGmyhqYY7ouTPEq/ZA6qfC6lSzfaQXgPFfF36UfPlfJMQOXpJZRUrIjEwokpxPulFHoKJwyChkVHqOKd0mGA0iyikl2bL4MFF+xsxC1DEThlXFoqFToRoJJuIzAvJIPK9xLdMIX4yxBLUlQuGUirAYkmJQLCLTKAHxzZr5KnZ7rbsmF8Wp+mQsrNinU0h2TdKWrVap036wZrgK/ZMPsk2qtEZkEZTqsSC/WE90vC8IR7Zs3oxXlWnKyeGdyoFWCA0QlH76uuY6ZcOWqlslVi8qasMKSFi0ricpMmBpYWGbCVEWTFuqYKVeoapcRlIRlWVihyco5MwPtM2I8LnNhpKJRSzlrWBaG1ROTnTUTA+tUIR6X2TBPtdeIcKLMhnmKuKwNqwUk3Xqc77wZp2jX7neizIdhqiDK6nDcnvSr2xOiLAizAr3ByPJdYWYpmLIiTFK5Q783YcqGcNya3KGSHLPE9Rca5LvGDFLrEqSgypaw2m3JPfp9CVX2hCPGMYtcKSo36Sm+q8wUtZvUTQlXdoXVQpKnylEZgd2lMhyzzBWhcpt+x1m5sjEcEcmqc0YEuE77/06WneH4736fCnDMQleA2oV8We1X5D79v3Pf7nT/557/c///c9vvXvm/d/2/O/y/u3wW/Hev3mWsGLnrvKv7//f58P93+9cdfPzrPv51r0Vj91x3LP+6p49/3dm/7u7fd+Dx7/v4973WzbhF8+978vj3nf377j7+cw/+cx9LR28wbn7s64bnfrqP/9yV/9zZ4tH+5w7H6p97HP2a/9zp8p+71W4jgO7nnof/3LX/3HkCzc/dtw0896+9hv/exQ7eOxm9Wgat7/00/r0j/72rEDrfOxv+e2/+e3/+e488u3jvVPr0XTDQ9t5z89+75vHfe/ff+/fx5y74cycWQNCBPd/9GP7cER9/7gs/98af++Pjz13y505tBmvgzx3z8ee++XPvfPi5g/7cxf0gNbIHj1Z/d7P58HNP/bmvO0Jqv/vbdzR3eFgpf+7znuZeS6UeSuV3x7s/99yf+x7PSs/urubuS53u7z1YwN6H2C+WeeSD7Oz6ez/8vSfBVP3vS/f3zvB7d/y9Qz472/s0arQVIByYmj1/75mPv3fO37vnw+899Pc+7g7J/+9m9/d++vB7V/29s/7eXR9/77G/93kBAAfmDH/vt4+/d93fO8/j7/33ew/skcF5eaT0xUXN7oPH7x3h965YAdSBzOy1bfb+UNaAIIbv+ZH77J0yWA0iHr6Tw0h+7xqOHc7u2GnvncFpfu+grfYuUsbAiIb32ZF+7ya/dxRGMLyniBH83lt+7y+fFfQu074YeiwA7kB3+J6O9HvH+fi97/zeezx+70C/d+EKMA90Zrf5vRt5/N6Tfu9Lv/emD9871O9dumvkrfu9W3343rN+71sfv3ev3zt449iBrh295zGHj9/72e89vXXsOWrvvbvlObrfe9zvfY7zfT5wfqff+333vefl+wJ65/Ofd/9595EBAA=="}, {"title": "Cake", "alt": "\ud83c\udf82", "stars": 150, "limited": 0, "b64": "UklGRuYBAABXRUJQVlA4TNoBAAAv/8F/ECegoG0bpuhK8Y7raAgJEv4PtkqYbTSdfUdy/jI3/wH4X0RzxSGxjSRHktQbIA1QHvw7IP236ra77zCZNQK/1SjRFdH/CXhQ/6v/1f/q//+AfvnmLjp/8UlufjOl5o0ps2pxmXlzSqzaXGLemfKqHj/evDulVX1+tPnAPNfVCL/W+dC8q6uG2KXORuTJrgbYwWZ9Ka7qssPNelJg1WESb0uRVZPJvCWF9vPWj2ep1UtZcqvv7l4luZvqP/Wf+u+OrA9vvuXTeuuPlCywzseC62wswM7Fe0SLiliYmVignYdAtXhYsJ2FwLWOggXcOQhk6xhY0P0ICGzrCrTAu/pP/af+OwcFOLsAGTY/BAKaHQKGzKH9+nf7M7BoxJPRykEEmIx2XL/bAkr0OqzofYcjuwJVdQWMjH4HFQNRxEiHFCMnhhwSiGpIYIjBeCrGTgQ5KtBUjEYQr4ikYvzcvnwNErcv6J1bl/zE3hUEz21LhuLcEhTPPSs5iitLkDzVf+o/9Z/6T/2n/lP/3b+QJNmFxTiau1ZSZKcWY2huXBJkO5fxMzcv6bHtZ2cCSHIMITcTQlJjGJmZIJIYQ5m0GE5WJhBLSgxrEmJwkwxT/6v/1f/qf/X/zgs="}, {"title": "Ball", "alt": "\ud83d\udd2e", "stars": 1000, "limited": 1, "b64": "UklGRuYDAABXRUJQVlA4TNkDAAAv/8F/EBcgEEjy3J9jDYFAkr/AuAsEkvxFx1ggkIS9wdoBf9UcA2nbtP5tb/slRMQE0E0G34BWIUdgGzfC4IlSUDpLXYDMubsR/Z8AyJFk122QJPxBCIziJcH7u7DqcdO0ADzuW5avEf2fAPz7/9//fzw0fNRWjkVvZNjUOo5tLxPIXCWQu0ggew1HupdwPOgVDI9qg3hmFnA87HqGx9UunptyBkJ1AyWbOYebgZS9nMW9QMtWzuNWIGYnZ3KnYJpKBmo1Cq5pBHL2cTb3AT3bGJ/aBN+0QUF2sQrqEhWmC0qyidVQk6gxTVCUPayKekSV6YGybGF11MLruEXUmRYozA5eyR2i0nRAaTawWmrgtdwgVo7eO9s0wOLof28y5tnC0T+TKc8X+vTkcl7MxqzfVJOHeV88uZhms7HSbyql+exr6aJyWkyOvk41LcbG/SKYfm1cVMyyydF3qZTlk7F1MzkrJl9bF9NkYdq3Tyom2eTY61S6nE9Gws3kpJh8JVxMk4RpTzypeDebHBmdSjk+GSk3k3NCxeR9pVyvABlMsVlPPamU4jqcduT0NwgdkwIhDBtJtz5Topv57CvponJGKJmbQQovZgs9+eRSgmvxxULLXAxieC9To3u5Gt8r1My9sPqVdLHxWqZHzxZfGkk3m68VeibpSII4COLhkHOKM0VK+kq5/jm+MVJuPt8qNo4UFExCxvnfwe5XwlWBSSPhfpkjAf8c28LeWUJRX1vX64ytW53vHVso6Sh8bVwvNDbuFzo2IC8S8LV0FZmwsXS/ElZOvNNYuF8AubMTZZl2TPBaGB/uF8PRe0flAuV/kKnS+4Sr8udEqJrvCZDNf5OELHxNcFX+nDBVep9oqvhGEaLwQnFNfqWYJj1Tmia+VEISniquyLcyRXquNEV8soQgHMv1+FqmR8+WpofnCjk4l6vxvUyN7tXU8GAhBgdzLb6YaVFC08KThRSczJU4w5ToZk0JjxZCEOo6fDXToZSmgzEhA2dzFc4xFcppKni4EIFg1+Ak06DLNQ2MCgmIdgXOMgXKagoYFgJwPK/nNKuntFaPcVEO8V7NeVZNea0aC0QxFPRabmC11KDVYoUohYpeyR1aJZaIQijpddzC6qhFq8MaUQY1rYp6tCosEkVQ1GqoSavBKlECVa2CurQKLBMFUNb41KbxsY6zGX3ZWCjIUNi41KgFFSo7kzs1JpZyHqM1D2s5i9GbhcWcw2jOwWrGIHQPApS359Su+VNG/3gIC9oz2qD5E8aOnmdsGWnYM5KwaaRgV98ztrUdYWFfMZa2T8K////9/8s7AA=="}];
const stickerBytes = new Map(); // document id -> Uint8Array (served by upload.getFile)
const tlStars = () => ({_: 'starsAmount', amount: String(acct.stars), nanos: 0});
const FAKE_CATALOG = GIFT_ART.map((g, i) => {
  const bytes = Buffer.from(g.b64, 'base64'), docId = String(9000000 + i);
  stickerBytes.set(docId, new Uint8Array(bytes));
  const doc = {_: 'document', pFlags: {}, id: docId, access_hash: '1', file_reference: new Uint8Array(0), date: now(), mime_type: 'image/webp',
    size: bytes.length, dc_id: 2, attributes: [{_: 'documentAttributeImageSize', w: 512, h: 512},
      {_: 'documentAttributeSticker', pFlags: {}, alt: g.alt, stickerset: {_: 'inputStickerSetEmpty'}}]};
  const total = 1000, remains = 1000 - i * 37;
  return {_: 'starGift', pFlags: flags({limited: g.limited}), id: String(5000 + i), sticker: doc, stars: String(g.stars), convert_stars: String(Math.floor(g.stars / 2)),
    ...(g.limited && {availability_remains: remains, availability_total: total}), title: g.title, upgrade_stars: String(Math.max(10, Math.floor(g.stars / 4)))};
});
/* Real data: produced once by export_real.js (run in a browser logged into a REAL Telegram account) -> real_data.json.
 * {gifts: [starGift], docs: {id: document}, files: {id: base64}, defaultStatuses: [emojiStatus], recentStatuses: [emojiStatus]} */
function loadReal() {
  const file = process.env.REAL_DATA || path.join(__dirname, 'real_data.json');
  if(!fs.existsSync(file)) return null;
  const rev = (_k, v) => (v && typeof v === 'object' && typeof v.__b64 === 'string' ? new Uint8Array(Buffer.from(v.__b64, 'base64')) : v);
  const r = JSON.parse(fs.readFileSync(file, 'utf8'), rev);
  log(`real data: ${r.gifts?.length || 0} gifts, ${Object.keys(r.docs || {}).length} documents, ${Object.keys(r.files || {}).length} files, ` +
    `${r.defaultStatuses?.length || 0} default / ${r.recentStatuses?.length || 0} recent emoji statuses`);
  return r;
}
const REAL = loadReal();
const CATALOG = REAL?.gifts?.length ? REAL.gifts : FAKE_CATALOG;
const docsById = new Map(); // document id -> Document (for messages.getCustomEmojiDocuments)
for(const g of FAKE_CATALOG) docsById.set(g.sticker.id, g.sticker);
if(REAL) {
  for(const [id, bytes] of Object.entries(REAL.files || {})) stickerBytes.set(String(id), new Uint8Array(Buffer.from(bytes, 'base64')));
  for(const [id, doc] of Object.entries(REAL.docs || {})) docsById.set(String(id), doc);
  for(const g of CATALOG) if(g.sticker) docsById.set(String(g.sticker.id), g.sticker);
}
const giftById = (id) => CATALOG.find((g) => g.id === String(id));

H['payments.getStarsStatus'] = () => ({_: 'payments.starsStatus', pFlags: {}, balance: tlStars(), chats: [], users: []});
/** A limited gift lists its resale market the way real Telegram does: availability_resale (how many) + resell_min_stars (floor price). tweb then shows a second "resale" tile for it. */
const withResale = (g) => {
  if(!(g.pFlags?.limited || g.availability_resale) || !g.upgrade_stars) return g;
  const pool = resalePool(g.id);
  if(!pool.length) return g;
  return {...g, availability_resale: String(pool.length), resell_min_stars: String(Math.min(...pool.map((u) => +u.resell_amount[0].amount)))};
};
let shopHash = 1;
H['payments.getStarGifts'] = (p) => (p.hash === shopHash ? {_: 'payments.starGiftsNotModified'} : {_: 'payments.starGifts', hash: shopHash, gifts: CATALOG.map(withResale), chats: [], users: []});
H['payments.getStarGiftCollections'] = () => ({_: 'payments.starGiftCollections', collections: []});
H['payments.checkCanSendGift'] = () => ({_: 'payments.checkCanSendGiftResultOk'});

const visibleGifts = (uid) => (savedGifts.get(uid) || []).filter((g) => !g.unsaved).length; // what a profile shows (count in userFull)
const isUniqueGift = (g) => g.gift._ === 'starGiftUnique';
H['payments.getSavedStarGifts'] = (p) => {
  const key = peerKeyOf(p.peer), uid = key && key[0] === 'u' ? +key.slice(1) : SELF, f = p.pFlags || {};
  let list = (savedGifts.get(uid) || []).slice();
  if(uid !== SELF) list = list.filter((g) => !g.unsaved);            // other people never see what the owner hid
  if(f.exclude_unsaved) list = list.filter((g) => !g.unsaved);
  if(f.exclude_saved) list = list.filter((g) => g.unsaved);
  if(f.exclude_unique) list = list.filter((g) => !isUniqueGift(g));
  if(f.exclude_unlimited) list = list.filter((g) => g.gift.pFlags?.limited || isUniqueGift(g));
  if(f.exclude_upgradable) list = list.filter((g) => !g.gift.upgrade_stars);
  if(f.exclude_unupgradable) list = list.filter((g) => g.gift.upgrade_stars);
  const value = (g) => +(g.gift.stars || 0);
  list.sort((a, b) => (b.pinned ? 1 : 0) - (a.pinned ? 1 : 0) || (f.sort_by_value ? value(b) - value(a) : 0) || b.date - a.date || b.msg_id - a.msg_id);
  const total = list.length, from = +p.offset || 0, limit = p.limit || 50, page = list.slice(from, from + limit);
  return {_: 'payments.savedStarGifts', pFlags: {}, count: total, gifts: page.map(tlSavedGift), ...(from + limit < total && {next_offset: String(from + limit)}),
    chats: [], users: [...new Set([...page.map((g) => g.from), uid])].map(tlUser)};
};
const tlSavedGift = (g) => ({_: 'savedStarGift', pFlags: flags({unsaved: g.unsaved, name_hidden: g.nameHidden, pinned_to_top: g.pinned, can_upgrade: !isUniqueGift(g) && g.canUpgrade}),
  ...(!g.nameHidden && {from_id: {_: 'peerUser', user_id: g.from}}), date: g.date, gift: g.gift, ...(g.message && {message: g.message}),
  msg_id: g.msg_id, saved_id: String(g.msg_id), ...(!isUniqueGift(g) && g.gift.convert_stars && {convert_stars: g.gift.convert_stars}),
  ...(!isUniqueGift(g) && g.canUpgrade && {upgrade_stars: g.prepaid ? undefined : g.gift.upgrade_stars}),
  ...(isUniqueGift(g) && {can_transfer_at: now() - 1, can_resell_at: now() - 1})});
const findSaved = (input) => { for(const list of savedGifts.values()) { const g = list.find((x) => x.msg_id === input?.msg_id); if(g) return g; } };
H['payments.saveStarGift'] = (p) => { const g = findSaved(p.stargift); if(g) g.unsaved = !!p.pFlags?.unsave; return true; };
H['payments.convertStarGift'] = (p) => {
  for(const [uid, list] of savedGifts) { const i = list.findIndex((x) => x.msg_id === p.stargift?.msg_id);
    if(i >= 0) { acct.stars += +list[i].gift.convert_stars; list.splice(i, 1); break; } }
  push(updatesOf([{_: 'updateStarsBalance', balance: tlStars()}]));
  return true;
};
H['payments.toggleChatStarGiftNotifications'] = () => true;
H['payments.toggleStarGiftsPinnedToTop'] = (p) => {
  const key = peerKeyOf(p.peer), uid = key && key[0] === 'u' ? +key.slice(1) : SELF;
  const ids = new Set((p.stargift || []).map((x) => x.msg_id));
  for(const g of savedGifts.get(uid) || []) g.pinned = ids.has(g.msg_id) && isUniqueGift(g);
  return true;
};

// Sending a gift = getPaymentForm (invoice) -> sendStarsForm (pay). Real Telegram does the same dance.
const forms = new Map();
H['payments.getPaymentForm'] = (p) => {
  const inv = p.invoice;
  if(inv?._ === 'inputInvoiceStarGiftResale') return resaleForm(inv);
  if(inv?._ !== 'inputInvoiceStarGift') throw fail(400, 'INVOICE_INVALID');
  const gift = giftById(inv.gift_id);
  if(!gift) throw fail(400, 'GIFT_INVALID'); // tweb shows "STARGIFT_INVALID"-style errors for this
  const id = String(Date.now() + forms.size);
  forms.set(id, inv);
  return {_: 'payments.paymentFormStarGift', form_id: id, invoice: {_: 'invoice', pFlags: {}, currency: 'XTR',
    prices: [{_: 'labeledPrice', label: gift.title, amount: gift.stars},
      ...(inv.pFlags?.include_upgrade && gift.upgrade_stars ? [{_: 'labeledPrice', label: 'Upgrade', amount: gift.upgrade_stars}] : [])]}};
};
H['payments.sendStarsForm'] = (p) => {
  const inv = forms.get(String(p.form_id)) || p.invoice;
  if(inv?._ === 'inputInvoiceStarGiftResale') return buyResale(p, inv);
  const gift = giftById(inv?.gift_id);
  if(!gift) throw fail(400, 'FORM_EXPIRED');
  const prepaid = !!(inv.pFlags?.include_upgrade && gift.upgrade_stars), cost = +gift.stars + (prepaid ? +gift.upgrade_stars : 0);
  if(acct.stars < cost) throw fail(400, 'BALANCE_TOO_LOW');
  const key = peerKeyOf(inv.peer);
  if(!key) throw fail(400, 'PEER_ID_INVALID');
  acct.stars -= cost;
  if(gift.availability_remains) gift.availability_remains--;
  const m = recordGift(key, SELF, gift, inv.message, inv.pFlags?.hide_name, {prepaid});
  forms.delete(String(p.form_id));
  return {_: 'payments.paymentResult', updates: updatesOf([{_: 'updateNewMessage', message: tlMessage(m), pts, pts_count: 1},
    {_: 'updateStarsBalance', balance: tlStars()}], [m])};
};
/** Creates the service message + saved-gift record. recipient = the user in `key` (or SELF when `from` is someone else). */
function recordGift(key, from, gift, message, nameHidden, opts = {}) {
  const to = from === SELF ? +key.slice(1) : SELF;
  const canUpgrade = !!gift.upgrade_stars && to === SELF;
  const m = addMessage(key, from, '', now(), {action: {_: 'messageActionStarGift', pFlags: flags({name_hidden: nameHidden, saved: true, can_upgrade: canUpgrade}),
    gift, ...(message && {message}), convert_stars: gift.convert_stars, ...(opts.prepaid && {upgrade_stars: gift.upgrade_stars}), from_id: {_: 'peerUser', user_id: from}}});
  if(!savedGifts.has(to)) savedGifts.set(to, []);
  savedGifts.get(to).push({msg_id: m.id, from, date: m.date, gift, message, unsaved: false, nameHidden: !!nameHidden, key, canUpgrade, prepaid: !!opts.prepaid});
  return m;
}
// NOTE for REAL gift data: the catalog above is fake art. To mirror Telegram's real catalog, log in once with a real
// session (gramjs / MTProto) and call payments.getStarGifts, then save every gift's `sticker` document bytes
// (upload.getFile) into stickerBytes keyed by document id and use the real starGift objects as CATALOG.
// Real gifts are lottie (application/x-tgsticker): keep their mime_type + attributes untouched.

H['upload.getFile'] = (p) => {
  const bytes = stickerBytes.get(String(p.location?.id)) || new Uint8Array(0);
  const off = p.offset || 0;
  return {_: 'upload.file', type: {_: 'storage.fileWebp'}, mtime: now(), bytes: bytes.subarray(off, off + (p.limit || bytes.length))};
};

// -- WhatsApp media: download (lazy, cached on disk) ...
const prevGetFile = H['upload.getFile'];
const storageOf = (mime = '') => ({_: mime.startsWith('image/jpeg') ? 'storage.fileJpeg' : mime === 'image/png' ? 'storage.filePng' : mime === 'image/webp' ? 'storage.fileWebp'
  : mime === 'video/mp4' ? 'storage.fileMp4' : mime === 'audio/mpeg' ? 'storage.fileMp3' : mime === 'application/pdf' ? 'storage.filePdf' : 'storage.fileUnknown'});
H['upload.getFile'] = async(p) => {
  const loc = p.location || {}, id = String(loc.id ?? '');
  if(loc._ === 'inputDocumentFileLocation' && stkById.has(id)) { // favorite / recent sticker (own copy on disk)
    const buf = await stkBytes(stkById.get(id));
    if(!buf) throw fail(400, 'FILE_REFERENCE_INVALID');
    const off = p.offset || 0, lim = p.limit || buf.length;
    return {_: 'upload.file', type: {_: 'storage.fileWebp'}, mtime: now(), bytes: new Uint8Array(buf.buffer, buf.byteOffset + off, Math.max(0, Math.min(lim, buf.length - off)))};
  }
  if(loc._ === 'inputPeerPhotoFileLocation') { // WhatsApp profile picture of a user / group (small = preview, big = full size)
    const key = peerKeyOf(loc.peer), buf = key && await bridge.pic(key, !!loc.pFlags?.big);
    if(!buf) throw fail(400, 'LOCATION_INVALID');
    const off = p.offset || 0, lim = p.limit || buf.length;
    return {_: 'upload.file', type: {_: 'storage.fileJpeg'}, mtime: now(), bytes: new Uint8Array(buf.buffer, buf.byteOffset + off, Math.max(0, Math.min(lim, buf.length - off)))};
  }
  const m = loc._ && /^input(Photo|Document)FileLocation$/.test(loc._) && !stickerBytes.has(id) ? msgs.find((x) => x.media && String(x.id) === id) : null;
  if(!m) return prevGetFile(p);
  let buf, mime = m.media.mime;
  if(loc.thumb_size === 'm' && m.media.thumb) { buf = Buffer.from(m.media.thumb, 'base64'); mime = 'image/jpeg'; } else buf = await bridge.download(m);
  if(!buf) throw fail(400, 'FILE_REFERENCE_INVALID');
  const off = p.offset || 0, lim = p.limit || buf.length;
  if(m.media.type === 'video' || m.media.type === 'audio') log(`getFile ${m.media.type} #${m.id} offset=${off} limit=${lim} total=${buf.length} declared=${m.media.size} mime=${mime}`);
  return {_: 'upload.file', type: storageOf(m.media.type === 'image' ? 'image/jpeg' : mime), mtime: m.date, bytes: new Uint8Array(buf.buffer, buf.byteOffset + off, Math.max(0, Math.min(lim, buf.length - off)))};
};

// -- ... and upload (tweb sends parts, then references the file id in messages.sendMedia)
const uploads = new Map(); // file_id -> {parts: Map(part -> Buffer), t}
const toBuf = (b) => (b && b.__b64 !== undefined ? Buffer.from(b.__b64, 'base64') : Buffer.from(b || []));
H['upload.saveFilePart'] = H['upload.saveBigFilePart'] = (p) => {
  const k = String(p.file_id);
  let u = uploads.get(k);
  if(!u) { u = {parts: new Map(), t: Date.now()}; uploads.set(k, u); }
  u.parts.set(p.file_part, toBuf(p.bytes));
  for(const [id, x] of uploads) if(Date.now() - x.t > 30 * 60 * 1000) uploads.delete(id); // abandoned uploads
  return true;
};
const takeUpload = (file) => {
  const u = file && uploads.get(String(file.id));
  if(!u) throw fail(400, 'FILE_ID_INVALID');
  uploads.delete(String(file.id));
  return Buffer.concat([...u.parts.keys()].sort((a, b) => a - b).map((i) => u.parts.get(i)));
};
H['messages.sendMedia'] = async(p) => {
  const key = peerKeyOf(p.peer);
  if(!key) throw fail(400, 'PEER_ID_INVALID');
  if(!bridge.active()) throw fail(400, 'WA_NOT_CONNECTED');
  let wm;
  if(p.media?._ === 'inputMediaDocument') {
    const d = await docBytesOf(p.media.id);
    if(!d) throw fail(400, 'DOCUMENT_INVALID');
    wm = await bridge.sendMedia(key, {...p, media: {_: 'inputMediaUploadedDocument', mime_type: d.mime, attributes: d.attributes}}, d.buf);
  } else wm = await bridge.sendMedia(key, p, takeUpload(p.media?.file));
  return updatesOf([{_: 'updateMessageID', id: wm.id, random_id: p.random_id},
    {_: 'updateNewMessage', message: tlMessage(wm), pts: 0, pts_count: 0}], [wm]);
};

/* ── emoji statuses (premium) ── */
const statusList = (list) => ({_: 'account.emojiStatuses', hash: '1', statuses: list || []});
H['account.getDefaultEmojiStatuses'] = (p) => (p.hash === '1' && REAL ? {_: 'account.emojiStatusesNotModified'} : statusList(REAL?.defaultStatuses));
H['account.getRecentEmojiStatuses'] = (p) => (p.hash === '1' && REAL ? {_: 'account.emojiStatusesNotModified'} : statusList(REAL?.recentStatuses));
H['account.clearRecentEmojiStatuses'] = () => true;
H['account.updateEmojiStatus'] = (p) => {
  if(!acct.premium) throw fail(400, 'PREMIUM_ACCOUNT_REQUIRED');
  const st = p.emoji_status, me = users.get(SELF);
  if(!st || st._ === 'emojiStatusEmpty') delete me.emoji_status; else me.emoji_status = st;
  push(updatesOf([{_: 'updateUserEmojiStatus', user_id: SELF, emoji_status: me.emoji_status || {_: 'emojiStatusEmpty'}}]));
  return true;
};
H['messages.getCustomEmojiDocuments'] = (p) => p.document_id.map((id) => docsById.get(String(id))).filter(Boolean);
H['messages.searchCustomEmoji'] = () => ({_: 'emojiList', hash: '0', document_id: []});
H['messages.getEmojiGroups'] = H['messages.getEmojiStickerGroups'] = H['messages.getEmojiStatusGroups'] = H['messages.getEmojiProfilePhotoGroups'] =
  () => ({_: 'messages.emojiGroups', hash: 0, groups: []});

/* ── stars wallet screens + misc ── */
H['payments.getStarsTopupOptions'] = () => [];
H['payments.getStarsGiftOptions'] = () => [];
H['payments.getStarsSubscriptions'] = H['payments.getStarsTransactions'] = () => ({_: 'payments.starsStatus', pFlags: {}, balance: tlStars(), history: [], chats: [], users: []});
H['bots.getPopularAppBots'] = () => ({_: 'bots.popularAppBots', pFlags: {}, users: []});
H['messages.saveDraft'] = () => true;
H['messages.getRecentStickers'] = () => ({_: 'messages.recentStickers', hash: '0', packs: [], stickers: [], dates: []});
H['messages.getAllStickers'] = () => ({_: 'messages.allStickers', hash: '0', sets: []});

/* ───────── sticker library: favorites + recents ─────────
 * WhatsApp has no server-side "faved stickers" list for linked devices, so tweb's Add to favorites / recent lists live here.
 * A sticker is copied (once, by sha1) to WA_STICKER_DIR, so it survives the chat message being deleted or the media link expiring.
 * items: sha -> {id, ids[], w, h, alt, size, date}; faved / recent: [sha] newest first. Document ids handed to tweb are the id of the
 * message it was first saved from (tweb already knows that id), later copies of the same sticker are aliases of it. */
const STK_DIR = process.env.WA_STICKER_DIR || path.join(__dirname, 'wa-stickers');
const STK_FILE = path.join(STK_DIR, 'library.json');
const STK_PIXEL = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';
const STK_FAVED_MAX = 200, STK_RECENT_MAX = 60;
const stkLib = {items: {}, faved: [], recent: []};
const stkById = new Map(); // document id (any alias) -> sha
try { Object.assign(stkLib, JSON.parse(fs.readFileSync(STK_FILE, 'utf8'))); } catch {}
for(const [sha, it] of Object.entries(stkLib.items)) for(const i of it.ids || [it.id]) stkById.set(String(i), sha);
const stkSave = () => { try { fs.mkdirSync(STK_DIR, {recursive: true}); fs.writeFileSync(STK_FILE + '.tmp', JSON.stringify(stkLib)); fs.renameSync(STK_FILE + '.tmp', STK_FILE); } catch(e) { log('sticker library save failed:', e.message); } };
const stkBytes = (sha) => fs.promises.readFile(path.join(STK_DIR, sha + '.webp')).catch(() => null);
const stkDoc = (it) => ({_: 'document', pFlags: {}, id: String(it.id), access_hash: '1', file_reference: b64of(''), date: it.date || now(), dc_id: 2, mime_type: 'image/webp', size: it.size || 0,
  thumbs: [{_: 'photoCachedSize', type: 'm', w: 100, h: 100, bytes: b64of(STK_PIXEL)}],
  attributes: [{_: 'documentAttributeImageSize', w: it.w || 512, h: it.h || 512}, {_: 'documentAttributeSticker', pFlags: {}, alt: it.alt || '', stickerset: {_: 'inputStickerSetEmpty'}}]});

/** library item for a tweb document id: known one, or copy the sticker out of the chat message that has this id */
async function stkEnsure(docId) {
  docId = String(docId);
  if(stkById.has(docId)) return stkLib.items[stkById.get(docId)];
  const m = msgs.find((x) => x.media?.type === 'sticker' && String(x.id) === docId);
  if(!m) return null;
  const buf = await bridge.download(m).catch(() => null);
  if(!buf) return null;
  const sha = require('crypto').createHash('sha1').update(buf).digest('hex');
  let it = stkLib.items[sha];
  if(!it) {
    it = stkLib.items[sha] = {id: docId, ids: [], w: m.media.w || 512, h: m.media.h || 512, alt: m.media.alt || '', size: buf.length, date: now()};
    fs.mkdirSync(STK_DIR, {recursive: true});
    await fs.promises.writeFile(path.join(STK_DIR, sha + '.webp'), buf);
  }
  if(!it.ids.includes(docId)) it.ids.push(docId);
  stkById.set(docId, sha);
  return it;
}
const stkDocsOf = (list) => list.map((sha) => stkLib.items[sha]).filter(Boolean).map(stkDoc);
const unshiftUniq = (arr, v, max) => { const i = arr.indexOf(v); if(i >= 0) arr.splice(i, 1); arr.unshift(v); arr.length = Math.min(arr.length, max); };

H['messages.getFavedStickers'] = () => ({_: 'messages.favedStickers', hash: '0', packs: [], stickers: stkDocsOf(stkLib.faved)});
H['messages.faveSticker'] = async(p) => {
  const it = await stkEnsure(p.id?.id);
  if(!it) throw fail(400, 'STICKER_ID_INVALID');
  const sha = stkById.get(String(p.id.id));
  stkLib.faved = stkLib.faved.filter((x) => x !== sha);
  if(!p.unfave) unshiftUniq(stkLib.faved, sha, STK_FAVED_MAX);
  stkSave();
  return true;
};
H['messages.getRecentStickers'] = () => ({_: 'messages.recentStickers', hash: '0', packs: [], stickers: stkDocsOf(stkLib.recent), dates: stkLib.recent.map(() => now())});
H['messages.saveRecentSticker'] = async(p) => {
  const it = await stkEnsure(p.id?.id);
  if(!it) return true; // not a sticker we can keep: ignore quietly (tweb also calls this for gifs)
  const sha = stkById.get(String(p.id.id));
  stkLib.recent = stkLib.recent.filter((x) => x !== sha);
  if(!p.unsave) unshiftUniq(stkLib.recent, sha, STK_RECENT_MAX);
  stkSave();
  return true;
};
H['messages.clearRecentStickers'] = () => { stkLib.recent = []; stkSave(); return true; };

/** bytes + mime + attributes of a document tweb wants to (re)send: a library sticker, or any media still in a chat */
async function docBytesOf(input) {
  const id = String(input?.id ?? '');
  if(stkById.has(id)) {
    const buf = await stkBytes(stkById.get(id)), it = stkLib.items[stkById.get(id)];
    return buf && {buf, mime: 'image/webp', attributes: stkDoc(it).attributes};
  }
  const m = msgs.find((x) => x.media && x.media.type !== 'image' && String(x.id) === id);
  if(!m) return null;
  const buf = await bridge.download(m).catch(() => null), doc = tlMedia(m)?.document;
  return buf && doc && {buf, mime: doc.mime_type, attributes: doc.attributes};
}

/* ───────────────────────────── collectible gifts ("NFTs": starGiftUnique) ─────────────────────────────
 * A regular gift with upgrade_stars can be upgraded into a unique gift: a random model + pattern + backdrop, a number and a slug.
 * Attribute pools come from real_data.json (upgradePreviews) when present, else from generated placeholders. */
const FAKE_ATTRS = (() => {
  const rar = (i) => ({_: 'starGiftAttributeRarity', permille: 20 + i * 40});
  const colors = [['Emerald', 0x2e8b57, 0x14573a, 0x8fe3b3], ['Sapphire', 0x3a6fd8, 0x1b3c8a, 0x9cbcff], ['Ruby', 0xd83a4f, 0x7d1626, 0xffa3b0], ['Amber', 0xe0a030, 0x7f5510, 0xffe29a],
    ['Violet', 0x8a4fd8, 0x45207f, 0xd3b3ff], ['Onyx', 0x40454f, 0x15171c, 0x9aa1b0]];
  return {
    models: FAKE_CATALOG.map((g, i) => ({_: 'starGiftAttributeModel', pFlags: {}, name: g.title, document: g.sticker, rarity: rar(i)})),
    patterns: FAKE_CATALOG.slice(0, 4).map((g, i) => ({_: 'starGiftAttributePattern', name: g.title + ' Pattern', document: g.sticker, rarity: rar(i)})),
    backdrops: colors.map(([name, center, edge, text], i) => ({_: 'starGiftAttributeBackdrop', name, backdrop_id: 100 + i, center_color: center, edge_color: edge,
      pattern_color: edge, text_color: text, rarity: rar(i)}))
  };
})();
function attrsFor(giftId) {
  const sample = REAL?.upgradePreviews?.[String(giftId)]?.sample_attributes;
  if(!sample?.length) {
    const gift = giftById(giftId);
    if(!gift?.sticker || FAKE_CATALOG.includes(gift)) return FAKE_ATTRS;
    const rar = (permille) => ({_: 'starGiftAttributeRarity', permille});
    return {models: [{_: 'starGiftAttributeModel', pFlags: {}, name: gift.title || 'Classic', document: gift.sticker, rarity: rar(100)}],
      patterns: [{_: 'starGiftAttributePattern', name: gift.title || 'Classic', document: gift.sticker, rarity: rar(100)}], backdrops: FAKE_ATTRS.backdrops};
  }
  const by = (t) => sample.filter((a) => a._ === t);
  const models = by('starGiftAttributeModel'), patterns = by('starGiftAttributePattern'), backdrops = by('starGiftAttributeBackdrop');
  return {models: models.length ? models : FAKE_ATTRS.models, patterns: patterns.length ? patterns : FAKE_ATTRS.patterns, backdrops: backdrops.length ? backdrops : FAKE_ATTRS.backdrops};
}
const pickOne = (a) => a[Math.floor(Math.random() * a.length)];
let uniqueSeq = 0;
const issued = {};
/** Mints a unique gift of `gift` owned by user `owner`. */
function mint(gift, owner, details) {
  const {models, patterns, backdrops} = attrsFor(gift.id), num = (issued[gift.id] = (issued[gift.id] || 0) + 1), title = gift.title || 'Gift';
  return {_: 'starGiftUnique', pFlags: {}, id: String(700000 + ++uniqueSeq), gift_id: gift.id, title, slug: title.replace(/[^A-Za-z0-9]/g, '') + '-' + num, num,
    owner_id: {_: 'peerUser', user_id: owner}, attributes: [pickOne(models), pickOne(patterns), pickOne(backdrops), ...(details ? [details] : [])],
    availability_issued: num + 100, availability_total: gift.availability_total || 1000};
}
const findSavedRec = (input) => {
  for(const [uid, list] of savedGifts) for(const g of list) {
    if(input?._ === 'inputSavedStarGiftSlug' ? g.gift.slug === input.slug : g.msg_id === input?.msg_id) return {uid, g};
  }
};
const usersOf = (ids) => [...new Set(ids)].map(tlUser);

H['payments.getStarGiftUpgradePreview'] = (p) => {
  const gift = giftById(p.gift_id);
  if(!gift) throw fail(400, 'GIFT_INVALID');
  const real = REAL?.upgradePreviews?.[String(p.gift_id)];
  if(real) return {_: 'payments.starGiftUpgradePreview', sample_attributes: real.sample_attributes, prices: real.prices || [], next_prices: real.next_prices || []};
  const a = attrsFor(gift.id);
  return {_: 'payments.starGiftUpgradePreview', sample_attributes: [...a.models, ...a.patterns, ...a.backdrops],
    prices: [{_: 'starGiftUpgradePrice', date: now() - 3600, upgrade_stars: gift.upgrade_stars || '25'}], next_prices: []};
};
H['payments.getStarGiftUpgradeAttributes'] = (p) => {
  const a = attrsFor(p.gift_id);
  return {_: 'payments.starGiftUpgradeAttributes', attributes: [...a.models, ...a.patterns, ...a.backdrops]};
};

H['payments.upgradeStarGift'] = (p) => {
  const hit = findSavedRec(p.stargift);
  if(!hit) throw fail(400, 'STARGIFT_INVALID');
  const {uid, g} = hit;
  if(uid !== SELF) throw fail(400, 'STARGIFT_FORBIDDEN');
  if(isUniqueGift(g)) throw fail(400, 'STARGIFT_ALREADY_UPGRADED');
  if(!g.gift.upgrade_stars) throw fail(400, 'STARGIFT_UPGRADE_UNAVAILABLE');
  const price = g.prepaid ? 0 : +g.gift.upgrade_stars;
  if(acct.stars < price) throw fail(400, 'BALANCE_TOO_LOW');
  acct.stars -= price;
  const details = p.pFlags?.keep_original_details ? {_: 'starGiftAttributeOriginalDetails', pFlags: {}, ...(!g.nameHidden && {sender_id: {_: 'peerUser', user_id: g.from}}),
    recipient_id: {_: 'peerUser', user_id: uid}, date: g.date, ...(g.message && {message: g.message})} : undefined;
  const unique = mint(g.gift, uid, details), old = msgs.find((x) => x.id === g.msg_id);
  g.gift = unique; g.canUpgrade = false;
  const ups = [];
  if(old?.action) { delete old.action.pFlags.can_upgrade; old.action.pFlags.upgraded = true; old.edit_date = now(); pts++;
    ups.push({_: 'updateEditMessage', message: tlMessage(old), pts, pts_count: 1}); }
  const m = addMessage(g.key || 'u' + g.from, SELF, '', now(), {action: {_: 'messageActionStarGiftUnique', pFlags: flags({upgrade: true, saved: !g.unsaved}), gift: unique,
    can_transfer_at: now() - 1, can_resell_at: now() - 1}});
  ups.push({_: 'updateNewMessage', message: tlMessage(m), pts, pts_count: 1}, {_: 'updateStarsBalance', balance: tlStars()});
  return updatesOf(ups, old ? [old, m] : [m]);
};

/* resale marketplace: per gift, either what export_real.js captured or a generated list (owned by the simulated users) */
const resalePools = new Map();
function resalePool(giftId) {
  const id = String(giftId);
  if(resalePools.has(id)) return resalePools.get(id);
  const gift = giftById(id), owners = [...users.keys()].filter((u) => u !== SELF);
  const real = REAL?.resale?.[id]?.gifts;
  const list = real?.length ? real.map((u, i) => ({...u, owner_id: {_: 'peerUser', user_id: owners[i % owners.length]}, resell_amount: u.resell_amount?.length ? u.resell_amount :
    [{_: 'starsAmount', amount: String(200 + i * 75), nanos: 0}]})) :
    Array.from({length: 12}, (_, i) => ({...mint(gift, owners[i % owners.length]), resell_amount: [{_: 'starsAmount', amount: String(100 + ((i * 137) % 900)), nanos: 0}]}));
  resalePools.set(id, list);
  return list;
}
const attrKey = (u, t) => u.attributes.find((a) => a._ === t);
H['payments.getResaleStarGifts'] = (p) => {
  const all = resalePool(p.gift_id);
  let list = all.slice();
  const want = (t, f) => (p.attributes || []).filter((x) => x._ === t).map(f);
  const wm = want('starGiftAttributeIdModel', (x) => String(x.document_id)), wp = want('starGiftAttributeIdPattern', (x) => String(x.document_id)), wb = want('starGiftAttributeIdBackdrop', (x) => x.backdrop_id);
  if(wm.length) list = list.filter((u) => wm.includes(String(attrKey(u, 'starGiftAttributeModel').document.id)));
  if(wp.length) list = list.filter((u) => wp.includes(String(attrKey(u, 'starGiftAttributePattern').document.id)));
  if(wb.length) list = list.filter((u) => wb.includes(attrKey(u, 'starGiftAttributeBackdrop').backdrop_id));
  const price = (u) => +u.resell_amount[0].amount;
  if(p.pFlags?.sort_by_price) list.sort((a, b) => price(a) - price(b)); else if(p.pFlags?.sort_by_num) list.sort((a, b) => a.num - b.num);
  const from = +p.offset || 0, limit = p.limit || 50, page = list.slice(from, from + limit);
  const res = {_: 'payments.resaleStarGifts', pFlags: {}, count: list.length, gifts: page, ...(from + limit < list.length && {next_offset: String(from + limit)}),
    chats: [], users: usersOf(page.map((u) => u.owner_id.user_id))};
  if(String(p.attributes_hash) !== '1') {
    // filters list exactly the attributes that exist among this gift's listings (so every filter finds something)
    const uniq = (arr, k) => [...new Map(arr.map((x) => [k(x), x])).values()];
    const a = {models: uniq(all.map((u) => attrKey(u, 'starGiftAttributeModel')), (x) => String(x.document.id)),
      patterns: uniq(all.map((u) => attrKey(u, 'starGiftAttributePattern')), (x) => String(x.document.id)),
      backdrops: uniq(all.map((u) => attrKey(u, 'starGiftAttributeBackdrop')), (x) => x.backdrop_id)};
    res.attributes = [...a.models, ...a.patterns, ...a.backdrops];
    res.attributes_hash = '1';
    const cnt = (test) => all.filter(test).length;
    res.counters = [
      ...a.models.map((m) => ({_: 'starGiftAttributeCounter', attribute: {_: 'starGiftAttributeIdModel', document_id: m.document.id}, count: cnt((u) => String(attrKey(u, 'starGiftAttributeModel').document.id) === String(m.document.id))})),
      ...a.patterns.map((m) => ({_: 'starGiftAttributeCounter', attribute: {_: 'starGiftAttributeIdPattern', document_id: m.document.id}, count: cnt((u) => String(attrKey(u, 'starGiftAttributePattern').document.id) === String(m.document.id))})),
      ...a.backdrops.map((b) => ({_: 'starGiftAttributeCounter', attribute: {_: 'starGiftAttributeIdBackdrop', backdrop_id: b.backdrop_id}, count: cnt((u) => attrKey(u, 'starGiftAttributeBackdrop').backdrop_id === b.backdrop_id)}))
    ].filter((c) => c.count > 0);
  }
  return res;
};

H['payments.getUniqueStarGift'] = (p) => {
  const hit = findSavedRec({_: 'inputSavedStarGiftSlug', slug: p.slug});
  let gift = hit?.g.gift;
  if(!gift) for(const list of resalePools.values()) { gift = list.find((u) => u.slug === p.slug); if(gift) break; }
  if(!gift) throw fail(400, 'STARGIFT_SLUG_INVALID');
  return {_: 'payments.uniqueStarGift', gift, chats: [], users: usersOf([gift.owner_id?.user_id].filter(Boolean))};
};
H['payments.getSavedStarGift'] = (p) => {
  const found = (p.stargift || []).map(findSavedRec).filter(Boolean);
  return {_: 'payments.savedStarGifts', pFlags: {}, count: found.length, gifts: found.map((r) => tlSavedGift(r.g)), chats: [],
    users: usersOf(found.flatMap((r) => [r.g.from, r.uid]))};
};
H['payments.transferStarGift'] = (p) => {
  const hit = findSavedRec(p.stargift), key = peerKeyOf(p.to_id);
  if(!hit || !isUniqueGift(hit.g)) throw fail(400, 'STARGIFT_INVALID');
  if(!key || key[0] !== 'u') throw fail(400, 'PEER_ID_INVALID');
  const to = +key.slice(1), list = savedGifts.get(hit.uid);
  list.splice(list.indexOf(hit.g), 1);
  hit.g.gift.owner_id = {_: 'peerUser', user_id: to}; hit.g.pinned = false; hit.g.from = hit.uid;
  if(!savedGifts.has(to)) savedGifts.set(to, []);
  savedGifts.get(to).push(hit.g);
  const m = addMessage(key, SELF, '', now(), {action: {_: 'messageActionStarGiftUnique', pFlags: flags({transferred: true, saved: true}), gift: hit.g.gift,
    from_id: {_: 'peerUser', user_id: SELF}}});
  return updatesOf([{_: 'updateNewMessage', message: tlMessage(m), pts, pts_count: 1}], [m]);
};
H['payments.updateStarGiftPrice'] = (p) => {
  const hit = findSavedRec(p.stargift);
  if(!hit || !isUniqueGift(hit.g)) throw fail(400, 'STARGIFT_INVALID');
  if(+p.resell_amount?.amount > 0) hit.g.gift.resell_amount = [p.resell_amount]; else delete hit.g.gift.resell_amount;
  return updatesOf([]);
};

/* ── buying a collectible from the resale market + its value info ── */
function findListing(slug) {
  for(const list of resalePools.values()) { const i = list.findIndex((u) => u.slug === slug); if(i >= 0) return {list, i, gift: list[i]}; }
}
function resaleForm(inv) {
  const hit = findListing(inv.slug);
  if(!hit) throw fail(400, 'STARGIFT_SLUG_INVALID');
  const id = String(Date.now() + forms.size);
  forms.set(id, inv);
  return {_: 'payments.paymentFormStarGift', form_id: id, invoice: {_: 'invoice', pFlags: {}, currency: 'XTR',
    prices: [{_: 'labeledPrice', label: hit.gift.title + ' #' + hit.gift.num, amount: hit.gift.resell_amount[0].amount}]}};
}
function buyResale(p, inv) {
  const hit = findListing(inv.slug);
  if(!hit) throw fail(400, 'FORM_EXPIRED');
  const price = +hit.gift.resell_amount[0].amount, key = peerKeyOf(inv.to_id);
  if(!key || key[0] !== 'u') throw fail(400, 'PEER_ID_INVALID');
  if(acct.stars < price) throw fail(400, 'BALANCE_TOO_LOW');
  const buyer = +key.slice(1), seller = hit.gift.owner_id.user_id;
  acct.stars -= price;
  hit.list.splice(hit.i, 1);
  shopHash++;
  const unique = {...hit.gift, owner_id: {_: 'peerUser', user_id: buyer}};
  delete unique.resell_amount;
  const hidden = !inv.pFlags?.show_name;
  // bought for yourself: the message comes from the seller; bought as a present: it is yours, sent to the recipient
  const chatKey = buyer === SELF ? 'u' + seller : key, sender = buyer === SELF ? seller : SELF;
  const m = addMessage(chatKey, sender, '', now(), {action: {_: 'messageActionStarGiftUnique', pFlags: flags({saved: true, name_hidden: hidden}), gift: unique,
    from_id: {_: 'peerUser', user_id: sender}, resale_amount: {_: 'starsAmount', amount: String(price), nanos: 0}, can_transfer_at: now() - 1, can_resell_at: now() - 1,
    ...(inv.message && {message: inv.message})}});
  if(!savedGifts.has(buyer)) savedGifts.set(buyer, []);
  savedGifts.get(buyer).push({msg_id: m.id, from: sender, date: m.date, gift: unique, message: inv.message, unsaved: false, nameHidden: hidden, key: chatKey});
  forms.delete(String(p.form_id));
  return {_: 'payments.paymentResult', updates: updatesOf([{_: 'updateNewMessage', message: tlMessage(m), pts, pts_count: 1},
    {_: 'updateStarsBalance', balance: tlStars()}], [m])};
}
H['payments.getUniqueStarGiftValueInfo'] = (p) => {
  const gift = findSavedRec({_: 'inputSavedStarGiftSlug', slug: p.slug})?.g.gift || findListing(p.slug)?.gift;
  if(!gift) throw fail(400, 'STARGIFT_SLUG_INVALID');
  const base = giftById(gift.gift_id), prices = resalePool(gift.gift_id).map((u) => +u.resell_amount[0].amount);
  const floor = prices.length ? Math.min(...prices) : +(base?.stars || 100), avg = prices.length ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length) : floor;
  const usd = (stars) => String(Math.round(stars * 1.3)); // US cents, ~1.3 cents per Star
  return {_: 'payments.uniqueStarGiftValueInfo', pFlags: {}, currency: 'USD', value: usd(avg * 1.2), initial_sale_date: now() - 86400 * 30,
    initial_sale_stars: String(base?.stars || 100), initial_sale_price: usd(+(base?.stars || 100)), floor_price: usd(floor), average_price: usd(avg), listed_count: prices.length};
};

/* ── sticker sets tweb asks for by built-in name (emoji status picker, generic animations, ...) ── */
const LOCAL_SETS = ['inputStickerSetAnimatedEmoji', 'inputStickerSetAnimatedEmojiAnimations', 'inputStickerSetPremiumGifts', 'inputStickerSetEmojiGenericAnimations',
  'inputStickerSetEmojiDefaultStatuses', 'inputStickerSetEmojiDefaultTopicIcons', 'inputStickerSetEmojiChannelDefaultStatuses', 'inputStickerSetTonGifts'];
H['messages.getStickerSet'] = (p) => {
  const kind = p.stickerset?._, idx = LOCAL_SETS.indexOf(kind);
  if(idx < 0) throw fail(400, 'STICKERSET_INVALID');
  const documents = kind === 'inputStickerSetEmojiDefaultStatuses' ?
    (REAL?.defaultStatuses || []).map((st) => docsById.get(String(st.document_id))).filter(Boolean) : [];
  return {_: 'messages.stickerSet', set: {_: 'stickerSet', pFlags: flags({emojis: /Emoji|Statuses/.test(kind)}), id: String(8000000 + idx), access_hash: '1',
    title: kind.replace('inputStickerSet', ''), short_name: kind.replace('inputStickerSet', ''), count: documents.length, hash: 0}, packs: [], keywords: [], documents};
};

/* ── premium as a gift (the options list the gift dialog needs: 3/6/12 months, in money and in Stars) ── */
H['payments.getPremiumGiftCodeOptions'] = () => [[3, 1500, 1000], [6, 2500, 1700], [12, 4500, 3000]].flatMap(([months, usd, stars]) => [
  {_: 'premiumGiftCodeOption', pFlags: {}, users: 1, months, currency: 'USD', amount: String(usd)},
  {_: 'premiumGiftCodeOption', pFlags: {}, users: 1, months, currency: 'XTR', amount: String(stars)}]);

/* ── methods that were showing up as "unhandled" ── */
H['stories.getAllReadPeerStories'] = () => updatesOf([]);
H['help.saveAppLog'] = () => true;
H['messages.getArchivedStickers'] = () => ({_: 'messages.archivedStickers', count: 0, sets: []});
const privacyRules = {}; // key predicate -> rules (echoed back so toggles in Settings stick)
H['account.setPrivacy'] = (p) => {
  privacyRules[p.key?._] = p.rules || [];
  return {_: 'account.privacyRules', rules: (p.rules || []).map((r) => ({_: 'p' + r._.slice(6), ...(r.users && {users: r.users.map(inputUserId)}), ...(r.chats && {chats: r.chats})})),
    chats: [], users: []};
};
H['account.getPrivacy'] = (p) => ({_: 'account.privacyRules', rules: (privacyRules[p.key?._] || []).length ?
  privacyRules[p.key._].map((r) => ({_: 'p' + r._.slice(6), ...(r.users && {users: r.users.map(inputUserId)}), ...(r.chats && {chats: r.chats})})) :
  [{_: 'privacyValueAllowAll'}], chats: [], users: []});
H['messages.markDialogUnread'] = () => true;
H['messages.deleteHistory'] = (p) => {
  const key = peerKeyOf(p.peer);
  for(let i = msgs.length - 1; i >= 0; i--) if(msgs[i].key === key && (!p.max_id || msgs[i].id <= p.max_id)) msgs.splice(i, 1);
  return {_: 'messages.affectedHistory', pts: ++pts, pts_count: 1, offset: 0};
};

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


/* ───────────────────────────── dev validator (VALIDATE=1) ───────────────────────────── */
// Checks every handler result against the TL schema tweb ships: constructor exists, belongs to the declared type,
// all non-flag params present, Vectors are arrays, nested constructors recurse. Logs `! schema <method>: ...`.
function validate(value, type, p, errs, depth = 0) {
  if(!SCHEMA || !type || depth > 6) return;
  const v = type.match(/^[Vv]ector<(.+)>$/);
  if(v) { if(!Array.isArray(value)) errs.push(`${p}: expected array`); else value.slice(0, 3).forEach((x, i) => validate(x, v[1], `${p}[${i}]`, errs, depth + 1)); return; }
  if(['int', 'long', 'double', 'string', 'bytes', 'Bool', 'true', 'int128', 'int256', 'Object', '!X', 'X', 'JSONValue'].includes(type) || type === '#') return;
  if(value == null || typeof value !== 'object') { errs.push(`${p}: expected ${type}`); return; }
  const c = SCHEMA.byPredicate[value._];
  if(!c) { errs.push(`${p}: unknown constructor "${value._}"`); return; }
  if(c.type !== type && type !== c.type.replace(/^help\./, 'help.')) errs.push(`${p}: ${value._} is ${c.type}, wanted ${type}`);
  for(const prm of c.params) {
    if(prm.type === '#' || /^flags2?$/.test(prm.name)) continue;
    const opt = prm.type.match(/^flags2?\.\d+\?(.+)$/);
    const t = opt ? opt[1] : prm.type;
    if(opt) { if(t === 'true') { if(value[prm.name] !== undefined && !(value.pFlags && value.pFlags[prm.name]) && value[prm.name] !== true) {} continue; } if(value[prm.name] === undefined) continue; }
    else if(value[prm.name] === undefined) { errs.push(`${p}: ${value._} missing ${prm.name}`); continue; }
    validate(value[prm.name], t, `${p}.${prm.name}`, errs, depth + 1);
  }
}
function checkResult(method, result) {
  const type = SCHEMA?.method[method]?.type;
  if(!type) return;
  const errs = [];
  validate(result, type, method, errs);
  if(errs.length) log(`! schema ${errs.slice(0, 6).join(' | ')}`);
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
if(compression) app.use(compression({threshold: 1024}));
app.use(express.json({limit: '100mb', reviver: jsonReviver}));

// Only calls whose answer depends on WhatsApp's history (message ids are numbered once, when history finishes) wait.
// Everything else (auth check users.*, updates.getState, reactions, stickers, filters, config...) answers instantly so the UI shell shows up right away.
const GATED = /^(messages\.(getDialogs|getPeerDialogs|getHistory|getMessages|getReplies|search|searchGlobal|getSearchCounters|getUnreadMentions|getUnreadReactions|getRecentLocations)|contacts\.(getContacts|search))$/;
app.post('/api', async(req, res) => {
  const {method, params = {}} = req.body || {};
  try {
    if(GATED.test(method) && !bridge.isReady()) await bridge.whenReady(); // first link: wait for WhatsApp's history
    const result = H[method] ? await H[method](params) : fallback(method);
    if(process.env.VALIDATE) checkResult(method, result);
    if(LOG_REQ) log(`${H[method] ? '·' : '~'} ${method}`);
    res.type('json').send(JSON.stringify({result}, jsonReplacer));
  } catch(e) {
    log(`! ${method} -> ${e.type || e.message}`);
    res.type('json').send(JSON.stringify({error: {code: e.code || 500, type: e.type || 'INTERNAL', message: e.message}}));
  }
});
app.get('/debug/unhandled', (_req, res) => res.json(unhandled));
// /debug/premium?on=1|0     -> toggles Premium live (pushes updateUser)
app.get('/debug/premium', (req, res) => {
  acct.premium = req.query.on !== '0';
  push(updatesOf([{_: 'updateUser', user_id: SELF}]));
  res.json({premium: acct.premium});
});
// /debug/stars?add=500      -> adds Stars to your balance
app.get('/debug/stars', (req, res) => {
  acct.stars += +req.query.add || 0;
  push(updatesOf([{_: 'updateStarsBalance', balance: tlStars()}]));
  res.json({stars: acct.stars});
});
// /debug/gift?gift=5001&from=1001&text=hi  -> a fake contact sends YOU a gift
// /debug/seed?n=8&unique=3&to=1000,1001   -> fills profiles with regular + collectible gifts (no chat messages) so the Gifts tab has something to show
let seedId = 900000;
app.get('/debug/seed', (req, res) => {
  const targets = (req.query.to || '1000,1001,1002,1003').split(',').map(Number), n = +req.query.n || 6;
  const senders = [...users.keys()].filter((i) => i !== SELF);
  for(const uid of targets) {
    if(!savedGifts.has(uid)) savedGifts.set(uid, []);
    for(let i = 0; i < n; i++) {
      const gift = CATALOG[(i + uid) % CATALOG.length];
      savedGifts.get(uid).push({msg_id: ++seedId, from: senders.find((x) => x !== uid) ?? SELF, date: now() - i * 3600, gift, unsaved: false, canUpgrade: uid === SELF && !!gift.upgrade_stars,
        message: i % 3 === 0 ? {_: 'textWithEntities', text: 'Enjoy! #' + (i + 1), entities: []} : undefined});
    }
    for(let i = 0; i < (+req.query.unique || 0); i++) {
      const gift = CATALOG[(i + uid + 1) % CATALOG.length];
      savedGifts.get(uid).push({msg_id: ++seedId, from: senders.find((x) => x !== uid) ?? SELF, date: now() - (i + n) * 3600, gift: mint(gift, uid), unsaved: false});
    }
    push(updatesOf([{_: 'updateUser', user_id: uid}]));
  }
  res.json({ok: true, to: targets, each: n, unique: +req.query.unique || 0});
});
app.get('/debug/gift', (req, res) => {
  const gift = giftById(req.query.gift || CATALOG[0].id), from = +req.query.from || 1001;
  if(!gift) return res.status(400).json({error: 'unknown gift', ids: CATALOG.map((g) => g.id)});
  const text = req.query.text ? {_: 'textWithEntities', text: String(req.query.text), entities: []} : undefined;
  const m = recordGift('u' + from, from, gift, text);
  push(updatesOf([{_: 'updateNewMessage', message: tlMessage(m), pts, pts_count: 1}], [m]));
  res.json({ok: true, msg_id: m.id});
});
app.get('/', (_req, res) => res.send('tweb simulated backend: POST /api, WS /updates, GET /debug/unhandled|premium|stars|gift'));

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

server.listen(PORT, () => log('build 84: WhatsApp chats') || log(`backend on http://localhost:${PORT}  (login: WhatsApp pairing code)`));
WA.restore().then((e) => e && log('WhatsApp session restored for', e.phone)).catch((e) => log('! WA restore failed:', e.message));
