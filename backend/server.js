
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
const acct = {premium: process.env.PREMIUM !== '0', stars: +process.env.STARS || 5000}; // PREMIUM=0 to start as a free user
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
const savedGifts = new Map(); // userId -> [{msg_id, from, date, gift, message, convert_stars, saved}]
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
  return {_: 'user', pFlags: flags({self: id === SELF, contact: id !== SELF, mutual_contact: id !== SELF, premium: id === SELF ? acct.premium : !!u.premium}), id, access_hash: '1',
    first_name: u.first_name, last_name: u.last_name || '', ...(u.username && {username: u.username}), phone: u.phone,
    status: {_: 'userStatusRecently', pFlags: {}}};
};
const tlChat = (id) => ({_: 'chat', pFlags: {}, id, title: groups.get(id).title, photo: {_: 'chatPhotoEmpty'},
  participants_count: groups.get(id).members.length, date: now(), version: 1});
const tlService = (m) => ({_: 'messageService', pFlags: flags({out: m.out}), id: m.id, from_id: {_: 'peerUser', user_id: m.from},
  peer_id: peerOf(m.key), date: m.date, action: m.action});
const tlMessage = (m) => m.action ? tlService(m) : ({_: 'message', pFlags: flags({out: m.out}), id: m.id, from_id: {_: 'peerUser', user_id: m.from},
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
const jb = (key, value) => ({_: 'jsonObjectValue', key, value: {_: 'jsonBool', value}});
const jn = (key, value) => ({_: 'jsonObjectValue', key, value: {_: 'jsonNumber', value}});
H['help.getAppConfig'] = () => ({_: 'help.appConfig', hash: 0, config: {_: 'jsonObject', value: [
  jb('premium_purchase_blocked', false), jb('stargifts_blocked', false), jn('stargifts_convert_period_max', 7776000),
  jn('stargifts_message_length_max', 255), jn('stars_gifts_enabled', 1)]}});
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
  return {_: 'users.userFull', full_user: {_: 'userFull', pFlags: {display_gifts_button: true}, id, stargifts_count: (savedGifts.get(id) || []).length, about: '', settings: {_: 'peerSettings', pFlags: {}},
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
H['account.updateStatus'] = () => true;
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
  if(isMediaFilter(p.filter)) return [];
  const q = (p.q || '').toLowerCase();
  let list = key ? msgsOf(key) : msgs;
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
  filter: f, count: f._ === 'inputMessagesFilterEmpty' ? msgsOf(peerKeyOf(p.peer)).length : 0}));

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
const CATALOG = GIFT_ART.map((g, i) => {
  const bytes = Buffer.from(g.b64, 'base64'), docId = String(9000000 + i);
  stickerBytes.set(docId, new Uint8Array(bytes));
  const doc = {_: 'document', pFlags: {}, id: docId, access_hash: '1', file_reference: new Uint8Array(0), date: now(), mime_type: 'image/webp',
    size: bytes.length, dc_id: 2, attributes: [{_: 'documentAttributeImageSize', w: 512, h: 512},
      {_: 'documentAttributeSticker', pFlags: {}, alt: g.alt, stickerset: {_: 'inputStickerSetEmpty'}}]};
  const total = 1000, remains = 1000 - i * 37;
  return {_: 'starGift', pFlags: flags({limited: g.limited}), id: String(5000 + i), sticker: doc, stars: String(g.stars), convert_stars: String(Math.floor(g.stars / 2)),
    ...(g.limited && {availability_remains: remains, availability_total: total}), title: g.title};
});
const giftById = (id) => CATALOG.find((g) => g.id === String(id));

H['payments.getStarsStatus'] = () => ({_: 'payments.starsStatus', pFlags: {}, balance: tlStars(), chats: [], users: []});
H['payments.getStarGifts'] = (p) => (p.hash === 1 ? {_: 'payments.starGiftsNotModified'} : {_: 'payments.starGifts', hash: 1, gifts: CATALOG, chats: [], users: []});
H['payments.getStarGiftCollections'] = () => ({_: 'payments.starGiftCollections', collections: []});
H['payments.checkCanSendGift'] = () => ({_: 'payments.checkCanSendGiftResultOk'});

H['payments.getSavedStarGifts'] = (p) => {
  const key = peerKeyOf(p.peer), uid = key && key[0] === 'u' ? +key.slice(1) : SELF;
  let list = (savedGifts.get(uid) || []).filter((g) => !g.unsaved || !p.pFlags?.exclude_unsaved);
  if(p.pFlags?.exclude_unsaved) list = list.filter((g) => !g.unsaved);
  if(p.pFlags?.exclude_saved) list = list.filter((g) => g.unsaved);
  const page = list.slice().reverse().slice(0, p.limit || 50);
  return {_: 'payments.savedStarGifts', pFlags: {}, count: list.length, gifts: page.map(tlSavedGift), chats: [], users: [...new Set(page.map((g) => g.from))].map(tlUser)};
};
const tlSavedGift = (g) => ({_: 'savedStarGift', pFlags: flags({unsaved: g.unsaved}), from_id: {_: 'peerUser', user_id: g.from}, date: g.date, gift: g.gift,
  ...(g.message && {message: g.message}), msg_id: g.msg_id, saved_id: String(g.msg_id), convert_stars: g.gift.convert_stars});
const findSaved = (input) => { for(const list of savedGifts.values()) { const g = list.find((x) => x.msg_id === input?.msg_id); if(g) return g; } };
H['payments.saveStarGift'] = (p) => { const g = findSaved(p.stargift); if(g) g.unsaved = !!p.pFlags?.unsave; return true; };
H['payments.convertStarGift'] = (p) => {
  for(const [uid, list] of savedGifts) { const i = list.findIndex((x) => x.msg_id === p.stargift?.msg_id);
    if(i >= 0) { acct.stars += +list[i].gift.convert_stars; list.splice(i, 1); break; } }
  push(updatesOf([{_: 'updateStarsBalance', balance: tlStars()}]));
  return true;
};
H['payments.toggleChatStarGiftNotifications'] = () => true;
H['payments.toggleStarGiftsPinnedToTop'] = () => true;

// Sending a gift = getPaymentForm (invoice) -> sendStarsForm (pay). Real Telegram does the same dance.
const forms = new Map();
H['payments.getPaymentForm'] = (p) => {
  const inv = p.invoice;
  if(inv?._ !== 'inputInvoiceStarGift') throw fail(400, 'INVOICE_INVALID');
  const gift = giftById(inv.gift_id);
  if(!gift) throw fail(400, 'GIFT_INVALID'); // tweb shows "STARGIFT_INVALID"-style errors for this
  const id = String(Date.now() + forms.size);
  forms.set(id, inv);
  return {_: 'payments.paymentFormStarGift', form_id: id, invoice: {_: 'invoice', pFlags: {}, currency: 'XTR',
    prices: [{_: 'labeledPrice', label: gift.title, amount: gift.stars}]}};
};
H['payments.sendStarsForm'] = (p) => {
  const inv = forms.get(String(p.form_id)) || p.invoice, gift = giftById(inv?.gift_id);
  if(!gift) throw fail(400, 'FORM_EXPIRED');
  if(acct.stars < +gift.stars) throw fail(400, 'BALANCE_TOO_LOW');
  const key = peerKeyOf(inv.peer);
  if(!key) throw fail(400, 'PEER_ID_INVALID');
  acct.stars -= +gift.stars;
  if(gift.availability_remains) gift.availability_remains--;
  const m = recordGift(key, SELF, gift, inv.message, inv.pFlags?.hide_name);
  forms.delete(String(p.form_id));
  return {_: 'payments.paymentResult', updates: updatesOf([{_: 'updateNewMessage', message: tlMessage(m), pts, pts_count: 1},
    {_: 'updateStarsBalance', balance: tlStars()}], [m])};
};
/** Creates the service message + saved-gift record. recipient = the user in `key` (or SELF when `from` is someone else). */
function recordGift(key, from, gift, message, nameHidden) {
  const to = from === SELF ? +key.slice(1) : SELF;
  const m = addMessage(key, from, '', now(), {action: {_: 'messageActionStarGift', pFlags: flags({name_hidden: nameHidden, saved: true}),
    gift, ...(message && {message}), convert_stars: gift.convert_stars, from_id: {_: 'peerUser', user_id: from}}});
  if(!savedGifts.has(to)) savedGifts.set(to, []);
  savedGifts.get(to).push({msg_id: m.id, from, date: m.date, gift, message, unsaved: false});
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
app.use(express.json({limit: '100mb', reviver: jsonReviver}));

app.post('/api', async(req, res) => {
  const {method, params = {}} = req.body || {};
  try {
    const result = H[method] ? await H[method](params) : fallback(method);
    if(process.env.VALIDATE) checkResult(method, result);
    log(`${H[method] ? '·' : '~'} ${method}`);
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

server.listen(PORT, () => log(`simulated backend on http://localhost:${PORT}  (login code ${LOGIN_CODE})`));
