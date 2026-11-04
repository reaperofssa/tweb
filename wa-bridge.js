'use strict';
/**
 * wa-bridge.js — WhatsApp (Baileys) <-> tweb simulated backend.
 *
 * Fills the server's Telegram-shaped store (users / groups / msgs / readState) from Baileys events and
 * sends what the tweb UI does back through Baileys:
 *
 *   WA -> tweb   history sync, messages.upsert (live chat list), edits, revokes, read receipts,
 *                presence (typing / recording / online), contacts, group metadata, "read on phone"
 *   tweb -> WA   send text (+ reply + bold/italic/... as WhatsApp formatting), edit, delete, mark read,
 *                typing, online/offline
 *
 * Id mapping (Telegram wants numbers):
 *   - phone-number JIDs      -> Number(phone)                       (stable, no storage needed)
 *   - groups, LID-only users -> sequential ids from a persisted registry
 *   - own account            -> SELF
 *   - LID <-> phone numbers are merged as soon as WhatsApp tells us they are the same person
 * Message ids: sequential. The initial history arrives newest-first and in chunks, so it is held back
 * (API calls wait on whenReady()), sorted by time and numbered once, so tweb's "sort by id" is right.
 *
 * State is saved to <session dir>/store.json (debounced) so a restart keeps the chats: WhatsApp only
 * sends the history once, when the device is linked.
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const REG_START = 100000; // registry ids; real phone numbers (>= 7 digits) are all above this
const IDLE_MS = +process.env.WA_HISTORY_IDLE_MS || 5000; // history is "done" this long after the last chunk
const FIRST_CHUNK_MS = +process.env.WA_HISTORY_FIRST_MS || 15000; // ... or this long after open if none came
const MAX_WAIT_MS = +process.env.WA_HISTORY_MAX_MS || 45000;
const SAVE_DEBOUNCE_MS = 3000;
const STATUS_READ = 4; // WAMessageStatus: ERROR 0, PENDING 1, SERVER_ACK 2, DELIVERY_ACK 3, READ 4, PLAYED 5
const STUB_REVOKE = 1; // WAMessageStubType.REVOKE
const WATCH_TTL_MS = 5 * 60 * 1000;
// Telegram ids are numbers, WhatsApp ids are strings: ids are DERIVED from the JID, so they survive a lost store.json / registry.
//   phone JID  -> the phone number (< 1e15)        LID JID -> LID_BASE + lid number (1e15 .. 2e15)
//   group JID  -> GROUP_BASE + 40 bit hash of the JID (2e15 .. 2.002e15)   [all below 2^53]
const LID_BASE = 1e15;
const GROUP_BASE = 2e15;
const GROUPS_TTL_S = +process.env.WA_GROUPS_TTL_S || 1800; // never call groupFetchAllParticipating more often than this (persisted, so crash loops do not spam it)
const META_GAP_MS = 1500; // single-group metadata fetches are serialised with this gap

module.exports = function createBridge(ctx) {
  const {users, groups, msgs, readState, SELF, push, updatesOf, tlMessage, tlChat, tlUser, addMessage,
    getLastId, setLastId, getPts, setPts, bumpPts, touch, log: baseLog, now, WA, fail} = ctx;
  const log = (...a) => baseLog('[bridge]', ...a);

  let B = null; // baileys module (ESM, handed over by wa-auth)
  let sock = null;
  let dir = null;
  let ready = true; // false while a freshly linked account's history is still arriving
  let waiters = [];
  let idleT = null, maxT = null, saveT = null, groupsAt = 0;
  let reg = freshReg();
  const byWaId = new Map(); // `${chatJid}|${waId}` -> message
  const chatUnread = {}; // peerKey -> unread count told by WhatsApp (history)
  const pendingIds = new Set(); // ids of messages we are sending (their echo must not be pushed twice)
  const watched = new Map(); // jid -> when presence was subscribed
  const metaCache = new Map(); // group jid -> latest Baileys GroupMetadata (handed back to Baileys through cachedGroupMetadata)
  let metaChain = Promise.resolve(), metaBackoffUntil = 0;

  function freshReg() {
    return {next: REG_START, byJid: {}, jidById: {}, lidToPn: {}, self: {phones: [], lids: []}};
  }

  /* ───────────────────────────── ids / jids ───────────────────────────── */

  const serverOf = (jid) => String(jid).split('@')[1];
  const isLid = (jid) => serverOf(jid) === 'lid';
  const isPn = (jid) => serverOf(jid) === 's.whatsapp.net';

  /** drops the device part, maps legacy servers, and turns a LID into the phone JID once that is known */
  function norm(jid) {
    if(!jid || typeof jid !== 'string' || !jid.includes('@')) return jid;
    let [u, s] = jid.split('@');
    u = u.split(':')[0];
    if(s === 'c.us' || s === 'hosted') s = 's.whatsapp.net';
    if(s === 'hosted.lid') s = 'lid';
    const j = u + '@' + s;
    return s === 'lid' && reg.lidToPn[j] ? reg.lidToPn[j] : j;
  }

  const isSelfUser = (user) => reg.self.phones.includes(user) || reg.self.lids.includes(user);

  const hashId = (jid) => GROUP_BASE + crypto.createHash('sha1').update(jid).digest().readUIntBE(0, 5);

  function idOf(jid) {
    jid = norm(jid);
    const [user, server] = jid.split('@');
    if(isSelfUser(user)) return SELF;
    if(reg.byJid[jid]) return reg.byJid[jid]; // ids handed out by older versions stay as they are
    if(server === 's.whatsapp.net' && /^\d{7,15}$/.test(user)) return Number(user);
    let id;
    if(server === 'lid' && /^\d{1,15}$/.test(user)) id = LID_BASE + Number(user);
    else { id = hashId(jid); while(reg.jidById[id] && reg.jidById[id] !== jid) id++; }
    reg.byJid[jid] = id;
    reg.jidById[id] = jid;
    return id;
  }

  function learn(lidRaw, pnRaw) {
    if(!lidRaw || !pnRaw) return;
    const lid = lidRaw.split(':')[0].replace(/@.*/, '') + '@lid';
    const pn = norm(pnRaw);
    if(!isPn(pn) || reg.lidToPn[lid] === pn) return;
    reg.lidToPn[lid] = pn;
    // one person = one id: whatever was created under the LID (user, chat, messages) moves to the phone id
    const lidId = reg.byJid[lid];
    if(lidId) {
      const pnId = idOf(pn);
      if(pnId !== lidId) migrate(lidId, pnId, pn);
      delete reg.byJid[lid];
      delete reg.jidById[lidId];
    }
    const [phone] = pn.split('@'), u = users.get(idOf(pn));
    if(u) {
      if(!u.phone) u.phone = phone;
      if(u.first_name === 'WhatsApp user') u.first_name = '+' + phone;
    }
  }

  /** moves everything of a duplicate user id (chat key, messages, group membership, read state, profile) onto the canonical one */
  function migrate(fromId, toId, toJid) {
    if(fromId === toId || fromId === SELF) return;
    const fk = 'u' + fromId, tk = 'u' + toId;
    for(const m of msgs) {
      if(m.key === fk) {
        m.key = tk;
        if(m.waId && m.jid && toJid) { byWaId.delete(waKeyOf(m.jid, m.waId)); m.jid = toJid; byWaId.set(waKeyOf(m.jid, m.waId), m); }
      }
      if(m.from === fromId) m.from = toId;
      if(m.mentions) for(const x of m.mentions) if(x.id === fromId) x.id = toId;
    }
    for(const g of groups.values()) if(g.members.includes(fromId)) g.members = [...new Set(g.members.map((x) => (x === fromId ? toId : x)))];
    const a = readState[fk], b = readState[tk];
    if(a) { readState[tk] = {inbox: Math.max(a.inbox, b?.inbox || 0), outbox: Math.max(a.outbox, b?.outbox || 0)}; delete readState[fk]; }
    if(chatUnread[fk] != null) { chatUnread[tk] = Math.max(chatUnread[fk], chatUnread[tk] || 0); delete chatUnread[fk]; }
    const ua = users.get(fromId), ub = users.get(toId);
    if(ua) {
      if(!ub) users.set(toId, ua);
      else {
        if((ua.rank ?? -1) > (ub.rank ?? -1)) { ub.first_name = ua.first_name; ub.last_name = ua.last_name; ub.rank = ua.rank; }
        ub.contact = ub.contact || ua.contact;
      }
      users.delete(fromId);
    }
    delete reg.jidById[fromId];
    touch();
    log(`merged duplicate id ${fromId} -> ${toId}`);
  }

  const kindOf = (jid) => (serverOf(jid) === 'g.us' ? 'group' : (isPn(jid) || isLid(jid)) ? 'user' : null);

  function chatKeyOf(jid) {
    jid = norm(jid);
    const k = kindOf(jid);
    return k ? (k === 'group' ? 'c' : 'u') + idOf(jid) : null;
  }

  const selfJid = () => (reg.self.phones[0] ? reg.self.phones[0] + '@s.whatsapp.net' : sock?.user?.id && norm(sock.user.id));

  function jidOfKey(key) {
    const id = +key.slice(1);
    if(id === SELF) return selfJid();
    return reg.jidById[id] || (key[0] === 'u' ? id + '@s.whatsapp.net' : null);
  }

  /* ───────────────────────────── users / groups ───────────────────────────── */

  const NAME_RANK = {contact: 3, notify: 2, verified: 1};

  /** creates the user if needed; returns its id. */
  function ensureUser(jid, name, rank = 0) {
    jid = norm(jid);
    const id = idOf(jid);
    if(id === SELF) return id;
    const [user, server] = jid.split('@');
    let u = users.get(id);
    if(!u) {
      u = {first_name: '', last_name: '', phone: server === 's.whatsapp.net' ? user : '', rank: -1, contact: false, ...(server !== 's.whatsapp.net' && {jid})};
      users.set(id, u);
    }
    if(name && rank > u.rank) { u.first_name = String(name); u.last_name = ''; u.rank = rank; }
    if(!u.first_name) u.first_name = u.phone ? '+' + u.phone : 'WhatsApp user';
    return id;
  }

  function pushUser(id) {
    push({_: 'updates', updates: [{_: 'updateUser', user_id: id}], users: [tlUser(id)], chats: [], date: now(), seq: 0});
  }

  function ensureGroup(jid, title) {
    jid = norm(jid);
    const id = idOf(jid);
    let g = groups.get(id);
    if(!g) { g = {title: title || 'WhatsApp group', members: [SELF], jid}; groups.set(id, g); }
    else if(title && g.title !== title) {
      g.title = title;
      if(ready) push({_: 'updates', updates: [{_: 'updateChat', chat_id: id}], users: [], chats: [tlChat(id)], date: now(), seq: 0});
    }
    return id;
  }

  function addMember(chatId, userId) {
    const g = groups.get(chatId);
    if(g && !g.members.includes(userId)) g.members.push(userId);
  }

  function upsertContact(c, live) {
    if(!c?.id) return;
    let jid = c.id;
    if(isLid(jid) && c.phoneNumber) { learn(jid, c.phoneNumber); jid = c.phoneNumber; }
    else if(isPn(jid) && c.lid) learn(c.lid, jid);
    if(!isPn(norm(jid)) && !isLid(norm(jid))) return;
    const id = ensureUser(jid, null);
    if(id === SELF) return;
    const u = users.get(id), before = u.first_name + '|' + u.contact;
    if(c.verifiedName) ensureUser(jid, c.verifiedName, NAME_RANK.verified);
    if(c.notify) ensureUser(jid, c.notify, NAME_RANK.notify);
    if(c.name) { ensureUser(jid, c.name, NAME_RANK.contact); u.contact = true; }
    if(live && ready && before !== u.first_name + '|' + u.contact) pushUser(id);
  }

  function upsertChat(chat) {
    if(!chat?.id) return;
    const jid = norm(chat.id), k = kindOf(jid);
    if(!k) return;
    let key;
    if(k === 'group') key = 'c' + ensureGroup(jid, chat.name || chat.subject);
    else { const id = ensureUser(jid, chat.name, NAME_RANK.notify); key = 'u' + id; }
    if(typeof chat.unreadCount === 'number') chatUnread[key] = chat.unreadCount;
  }

  function upsertGroupMeta(meta) {
    if(!meta?.id) return;
    const id = ensureGroup(meta.id, meta.subject);
    metaCache.set(norm(meta.id), meta);
    const g = groups.get(id), members = new Set([SELF]);
    g.announce = !!meta.announce;
    g.selfAdmin = false;
    for(const p of meta.participants || []) {
      let jid = p.id;
      if(isLid(jid) && p.phoneNumber) { learn(jid, p.phoneNumber); jid = p.phoneNumber; }
      else if(isPn(jid) && p.lid) learn(p.lid, jid);
      const uid = ensureUser(jid, p.name || p.notify, NAME_RANK.notify);
      members.add(uid);
      if(uid === SELF && p.admin) g.selfAdmin = true;
    }
    g.members = [...members];
    if(ready) push({_: 'updates', updates: [{_: 'updateChat', chat_id: id}], users: [], chats: [tlChat(id)], date: now(), seq: 0});
  }

  const isRateLimit = (e) => /rate|overlimit|429|too many/i.test(String(e?.message || e) + String(e?.data || ''));

  /** ONE call for every group. Persisted TTL: a crash loop / reconnect storm must not hammer WhatsApp's participants endpoint. */
  async function refreshGroups(force) {
    if(!sock || now() < metaBackoffUntil) return;
    if(!force && groups.size && now() - (reg.groupsAt || 0) < GROUPS_TTL_S) return;
    reg.groupsAt = now();
    dirty();
    try {
      const all = await sock.groupFetchAllParticipating();
      for(const meta of Object.values(all || {})) upsertGroupMeta(meta);
      log(`groups: ${Object.keys(all || {}).length}`);
      dirty();
    } catch(e) {
      log('groupFetchAllParticipating failed:', e.message);
      if(isRateLimit(e)) metaBackoffUntil = now() + 3600;
    }
  }

  /** one group, queued (1 per META_GAP_MS), cached, never while backing off */
  function fetchMeta(jid) {
    jid = norm(jid);
    if(metaCache.has(jid)) return Promise.resolve(metaCache.get(jid));
    const run = async() => {
      if(metaCache.has(jid)) return metaCache.get(jid);
      if(!sock || now() < metaBackoffUntil) return undefined;
      try {
        const meta = await sock.groupMetadata(jid);
        upsertGroupMeta(meta);
        return meta;
      } catch(e) {
        log(`groupMetadata ${jid} failed:`, e.message);
        if(isRateLimit(e)) metaBackoffUntil = now() + 3600;
        return undefined;
      } finally { await new Promise((r) => setTimeout(r, META_GAP_MS)); }
    };
    const p = metaChain.then(run, run);
    metaChain = p.catch(() => {});
    return p;
  }

  /** Baileys asks this before every group send / receive instead of hitting WhatsApp */
  const groupMeta = (jid) => metaCache.get(norm(jid)) || fetchMeta(jid);

  /* ───────────────────────────── message content ───────────────────────────── */

  const unwrap = (message) => (B?.normalizeMessageContent ? B.normalizeMessageContent(message) : message);

  /* ───────────────────────────── media ───────────────────────────── */
  const MEDIA_KEYS = {imageMessage: 'image', videoMessage: 'video', ptvMessage: 'video', audioMessage: 'audio', documentMessage: 'document', stickerMessage: 'sticker'};
  const num = (v) => Number(v?.toNumber ? v.toNumber() : v) || 0;
  const b64 = (v) => (v && v.length ? Buffer.from(v).toString('base64') : undefined);
  const MEDIA_DIR = process.env.WA_MEDIA_DIR || path.join(__dirname, 'wa-media');
  const noLog = {level: 'silent', info() {}, debug() {}, warn() {}, error() {}, trace() {}, fatal() {}, child() { return noLog; }};

  /** JSON-safe description of a WhatsApp media message (enough to rebuild it for downloadMediaMessage after a restart) */
  function mediaOf(message) {
    const c = unwrap(message);
    if(!c) return undefined;
    for(const k of Object.keys(MEDIA_KEYS)) {
      const x = c[k];
      if(!x) continue;
      return {
        type: MEDIA_KEYS[k], key: k, mime: x.mimetype || '', size: num(x.fileLength), w: x.width || 0, h: x.height || 0, dur: x.seconds || 0,
        ptt: !!x.ptt, anim: !!(x.gifPlayback || x.isAnimated), round: k === 'ptvMessage', name: x.fileName || '', caption: x.caption || '',
        thumb: b64(x.jpegThumbnail), wave: b64(x.waveform),
        raw: {url: x.url, directPath: x.directPath, mediaKey: b64(x.mediaKey), fileSha256: b64(x.fileSha256), fileEncSha256: b64(x.fileEncSha256), fileLength: num(x.fileLength), mimetype: x.mimetype}
      };
    }
    return undefined;
  }

  const dlInflight = new Map();
  /** the media bytes of a message: disk cache first, else decrypt from WhatsApp (and ask the phone to re-upload if the link expired) */
  function download(m) {
    const md = m?.media;
    if(!md) return Promise.resolve(null);
    const file = path.join(MEDIA_DIR, `${m.key}-${m.waId || m.id}.bin`.replace(/[^\w.-]/g, '_'));
    if(dlInflight.has(file)) return dlInflight.get(file);
    const job = (async() => {
      try { return await fs.promises.readFile(file); } catch {}
      const s = requireSock(), r = md.raw, un = (v) => (v ? Buffer.from(v, 'base64') : undefined);
      const wm = {key: m.wa?.key || {remoteJid: m.jid, id: m.waId, fromMe: !!m.out},
        message: {[md.key]: {url: r.url, directPath: r.directPath, mediaKey: un(r.mediaKey), fileSha256: un(r.fileSha256), fileEncSha256: un(r.fileEncSha256), fileLength: r.fileLength, mimetype: r.mimetype}}};
      let buf;
      try { buf = await B.downloadMediaMessage(wm, 'buffer', {}, {logger: noLog, reuploadRequest: s.updateMediaMessage}); } catch(e) {
        log('media download failed:', m.id, e.message);
        throw fail(500, 'WA_MEDIA_UNAVAILABLE', e.message);
      }
      try { await fs.promises.mkdir(MEDIA_DIR, {recursive: true}); await fs.promises.writeFile(file, buf); } catch(e) { log('media cache write failed:', e.message); }
      return buf;
    })().finally(() => dlInflight.delete(file));
    dlInflight.set(file, job);
    return job;
  }

  /** text shown in the chat for a WhatsApp message; null = not a chat message (protocol, key distribution, ...) */
  function describe(message) {
    const c = unwrap(message);
    if(!c) return null;
    const cap = (icon, fallback, x) => icon + ' ' + (x.caption || fallback);
    if(c.conversation) return {text: c.conversation, kind: 'text'};
    if(c.extendedTextMessage) return {text: c.extendedTextMessage.text || '', kind: 'text'};
    if(c.imageMessage) return {text: cap('📷', 'Photo', c.imageMessage), kind: 'image'};
    if(c.videoMessage) return {text: cap('🎥', 'Video', c.videoMessage), kind: 'video'};
    if(c.audioMessage) return {text: c.audioMessage.ptt ? '🎤 Voice message' : '🎵 Audio', kind: 'audio'};
    if(c.documentMessage) return {text: '📄 ' + (c.documentMessage.fileName || 'Document') + (c.documentMessage.caption ? '\n' + c.documentMessage.caption : ''), kind: 'document'};
    if(c.stickerMessage) return {text: '🖼 Sticker', kind: 'sticker'};
    if(c.locationMessage || c.liveLocationMessage) return {text: '📍 Location', kind: 'location'};
    if(c.contactMessage) return {text: '👤 ' + (c.contactMessage.displayName || 'Contact'), kind: 'contact'};
    if(c.contactsArrayMessage) return {text: '👤 ' + (c.contactsArrayMessage.displayName || 'Contacts'), kind: 'contact'};
    if(c.pollCreationMessage || c.pollCreationMessageV2 || c.pollCreationMessageV3) {
      const p = c.pollCreationMessage || c.pollCreationMessageV2 || c.pollCreationMessageV3;
      return {text: '📊 ' + (p.name || 'Poll'), kind: 'poll'};
    }
    if(c.buttonsResponseMessage) return {text: c.buttonsResponseMessage.selectedDisplayText || '', kind: 'text'};
    if(c.listResponseMessage) return {text: c.listResponseMessage.title || '', kind: 'text'};
    if(c.templateButtonReplyMessage) return {text: c.templateButtonReplyMessage.selectedDisplayText || '', kind: 'text'};
    return null;
  }

  const contextOf = (message) => {
    const c = unwrap(message);
    if(!c) return null;
    for(const v of Object.values(c)) if(v && typeof v === 'object' && v.contextInfo) return v.contextInfo;
    return null;
  };

  const tsOf = (t) => {
    if(t == null) return now();
    if(typeof t === 'object') return Number(t.toNumber ? t.toNumber() : t.low) || now();
    return Number(t) || now();
  };

  /* ───────────────────────────── mentions (tags) ───────────────────────────── */

  const userPart = (jid) => String(jid).split('@')[0].split(':')[0];

  /**
   * WhatsApp writes a tag as "@<number>" in the text and lists the tagged JIDs in contextInfo.mentionedJid.
   * We keep {tag: <number as written in the text>, id: <tweb user id>} pairs on the message.
   */
  function mentionPairs(jids) {
    const out = [];
    for(const raw of jids || []) {
      if(typeof raw !== 'string' || !raw.includes('@')) continue;
      const jid = norm(raw);
      if(kindOf(jid) !== 'user') continue;
      out.push({tag: userPart(raw), id: ensureUser(jid, null)});
    }
    return out.length ? out : undefined;
  }

  const mentionsOf = (message) => mentionPairs(contextOf(message)?.mentionedJid);

  /** Telegram's `mentioned`: I am tagged, or someone replied to one of my messages (groups only, like Telegram) */
  function mentionsMeOf(message) {
    const c = contextOf(message);
    if(!c) return false;
    if((c.mentionedJid || []).some((j) => typeof j === 'string' && isSelfUser(userPart(j)))) return true;
    return !!(c.quotedMessage && c.participant && isSelfUser(userPart(c.participant)));
  }

  function displayName(id) {
    const u = users.get(id);
    const name = u && [u.first_name, u.last_name].filter(Boolean).join(' ');
    return name || (u?.phone ? '+' + u.phone : 'user');
  }

  /** text + tweb entities for a message with tags: "@1234567" -> "@Name" covered by a clickable messageEntityMentionName */
  function render(m) {
    const text = m.text || '', byTag = new Map(m.mentions.map((x) => [x.tag, x.id]));
    const entities = [];
    let out = '', last = 0;
    for(const hit of text.matchAll(/@(\d+)/g)) {
      const id = byTag.get(hit[1]);
      if(id == null) continue;
      const name = '@' + displayName(id);
      out += text.slice(last, hit.index);
      entities.push({_: 'messageEntityMentionName', offset: out.length, length: name.length, user_id: id});
      out += name;
      last = hit.index + hit[0].length;
    }
    return {text: out + text.slice(last), entities};
  }

  /** the chat a message key belongs to (phone JID when WhatsApp gives both) */
  function chatJidOfKey(key) {
    if(key.remoteJidAlt && isLid(key.remoteJid)) learn(key.remoteJid, key.remoteJidAlt);
    return norm(key.remoteJid);
  }

  function senderJidOfKey(key) {
    if(key.participantAlt && key.participant && isLid(key.participant)) learn(key.participant, key.participantAlt);
    return key.participant ? norm(key.participant) : null;
  }

  const waKeyOf = (chatJid, id) => chatJid + '|' + id;

  /**
   * Adds a WhatsApp message to the store. Returns {m, dup} or null when it is not a chat message.
   * `quiet` messages are not announced to the UI by the caller.
   */
  function ingest(wm) {
    const key = wm?.key;
    if(!key?.remoteJid || !key.id) return null;
    const chatJid = chatJidOfKey(key), kind = kindOf(chatJid);
    if(!kind) return null; // status@broadcast, newsletters, broadcast lists
    const known = byWaId.get(waKeyOf(chatJid, key.id));
    if(known) return {m: known, dup: true};

    const d = describe(wm.message);
    if(!d) return null;

    const isGroup = kind === 'group';
    let from = SELF;
    if(!key.fromMe) {
      const sender = isGroup ? senderJidOfKey(key) : chatJid;
      if(!sender) return null;
      from = ensureUser(sender, wm.pushName, NAME_RANK.notify);
    }
    let peerKey;
    if(isGroup) {
      const cid = ensureGroup(chatJid);
      peerKey = 'c' + cid;
      addMember(cid, from);
      if(ready && !metaCache.has(chatJid)) fetchMeta(chatJid);
    } else {
      peerKey = 'u' + ensureUser(chatJid, null);
    }

    const ctxInfo = contextOf(wm.message);
    const replyWaId = ctxInfo?.stanzaId;
    const replied = replyWaId && byWaId.get(waKeyOf(chatJid, replyWaId));
    const media = mediaOf(wm.message);
    const m = addMessage(peerKey, from, media ? media.caption : d.text, tsOf(wm.messageTimestamp), {
      ...(media && {media}),
      reply_to: replied ? replied.id : undefined,
      replyWaId, jid: chatJid, waId: key.id, kind: d.kind, wstatus: wm.status, mentions: mentionsOf(wm.message),
      ...(isGroup && !key.fromMe && mentionsMeOf(wm.message) && {mentionsMe: true}),
      wa: {key: {...key}, message: wm.message}
    });
    byWaId.set(waKeyOf(chatJid, key.id), m);
    return {m, dup: false};
  }

  const announce = (m) => push(updatesOf([{_: 'updateNewMessage', message: tlMessage(m), pts: getPts(), pts_count: 1}], [m]));

  /* ───────────────────────────── history ───────────────────────────── */

  const chatLastDate = (key) => { let d = 0; for(const m of msgs) if(m.key === key && m.date > d) d = m.date; return d; };

  function onHistory({chats = [], contacts = [], messages = [], lidPnMappings = [], isLatest, progress}) {
    for(const mp of lidPnMappings) learn(mp.lid, mp.pn);
    contacts.forEach((c) => upsertContact(c, false));
    chats.forEach(upsertChat);
    let added = 0, skipped = 0;
    for(const wm of messages) {
      if(ready) {
        // after the first numbering only newer-than-known messages can be added without breaking the order
        const cj = wm.key?.remoteJid && chatJidOfKey(wm.key), k = cj && chatKeyOf(cj);
        if(k && tsOf(wm.messageTimestamp) < chatLastDate(k)) { skipped++; continue; }
      }
      const r = ingest(wm);
      if(r && !r.dup) added++;
    }
    log(`history chunk: ${chats.length} chats, ${contacts.length} contacts, ${messages.length} msgs (+${added}${skipped ? ', ' + skipped + ' older skipped' : ''}) progress=${progress ?? '-'}`);
    if(!ready) armIdle(isLatest || progress === 100 ? 1500 : IDLE_MS);
    dirty();
  }

  function armIdle(ms) {
    clearTimeout(idleT);
    idleT = setTimeout(finishHistory, ms);
  }

  function finishHistory() {
    if(ready) return;
    clearTimeout(idleT); clearTimeout(maxT);
    msgs.sort((a, b) => a.date - b.date); // stable: ties keep arrival order
    let id = 0;
    for(const m of msgs) m.id = ++id;
    setLastId(id);
    for(const m of msgs) if(m.replyWaId) m.reply_to = byWaId.get(waKeyOf(m.jid, m.replyWaId))?.id;
    touch();

    const byKey = new Map();
    for(const m of msgs) (byKey.get(m.key) || byKey.set(m.key, []).get(m.key)).push(m);
    for(const [key, list] of byKey) {
      const incoming = list.filter((m) => !m.out);
      const n = Math.min(Math.max(chatUnread[key] || 0, 0), incoming.length);
      const unread = n ? incoming.slice(-n) : [];
      const inbox = unread.length ? unread[0].id - 1 : list[list.length - 1].id;
      const outbox = Math.max(0, ...list.filter((m) => m.out && m.wstatus >= STATUS_READ).map((m) => m.id));
      readState[key] = {inbox, outbox};
    }
    ready = true;
    log(`history ready: ${msgs.length} messages, ${byKey.size} chats`);
    waiters.forEach((r) => r());
    waiters = [];
    dirty();
    refreshGroups();
    watchTop();
  }

  const whenReady = () => (ready ? Promise.resolve() : new Promise((r) => waiters.push(r)));

  /* ───────────────────────────── live events ───────────────────────────── */

  function onUpsert({messages}) {
    for(const wm of messages) {
      const r = ingest(wm);
      if(!r || r.dup) continue;
      // our own sends are announced by the API answer, not by their echo
      if(ready && !pendingIds.has(wm.key.id)) announce(r.m);
    }
    dirty();
  }

  const findMsg = (key) => key?.remoteJid && key.id && byWaId.get(waKeyOf(chatJidOfKey(key), key.id));

  function onMessagesUpdate(list) {
    for(const {key, update} of list) {
      const m = findMsg(key);
      if(!m || !update) continue;

      if(update.messageStubType === STUB_REVOKE && update.message === null) {
        removeMessage(m, true);
        continue;
      }
      const edited = update.message?.editedMessage?.message;
      if(edited) {
        const d = describe(edited);
        if(d) {
          m.text = d.text;
          m.mentions = mentionsOf(edited);
          if(m.key[0] === 'c' && !m.out) m.mentionsMe = mentionsMeOf(edited) || undefined;
          m.edit_date = now();
          if(m.wa) m.wa.message = edited;
          bumpPts();
          if(ready) push(updatesOf([{_: 'updateEditMessage', message: tlMessage(m), pts: getPts(), pts_count: 1}], [m]));
        }
      }
      if(typeof update.status === 'number' && m.out) {
        m.wstatus = update.status;
        if(update.status >= STATUS_READ && m.key[0] === 'u') {
          const rs = readState[m.key] || (readState[m.key] = {inbox: 0, outbox: 0});
          if(m.id > rs.outbox) {
            rs.outbox = m.id;
            bumpPts();
            if(ready) push(updatesOf([{_: 'updateReadHistoryOutbox', peer: peerOf(m.key), max_id: m.id, pts: getPts(), pts_count: 1}]));
          }
        }
      }
    }
    dirty();
  }

  const peerOf = (key) => (key[0] === 'u' ? {_: 'peerUser', user_id: +key.slice(1)} : {_: 'peerChat', chat_id: +key.slice(1)});

  function removeMessage(m, announceIt) {
    const i = msgs.indexOf(m);
    if(i >= 0) msgs.splice(i, 1);
    byWaId.delete(waKeyOf(m.jid, m.waId));
    touch();
    bumpPts();
    if(announceIt && ready) push(updatesOf([{_: 'updateDeleteMessages', messages: [m.id], pts: getPts(), pts_count: 1}]));
  }

  const TYPING = {composing: 'sendMessageTypingAction', recording: 'sendMessageRecordAudioAction', paused: 'sendMessageCancelAction'};

  function onPresence({id, presences}) {
    if(!ready) return;
    const chatJid = norm(id), kind = kindOf(chatJid);
    if(!kind) return;
    for(const [who, p] of Object.entries(presences || {})) {
      const uid = idOf(kind === 'group' ? who : chatJid);
      if(uid === SELF) continue;
      const state = p.lastKnownPresence;
      if(TYPING[state]) {
        const action = {_: TYPING[state]};
        push({_: 'updateShort', date: now(), update: kind === 'group' ?
          {_: 'updateChatUserTyping', chat_id: idOf(chatJid), from_id: {_: 'peerUser', user_id: uid}, action} :
          {_: 'updateUserTyping', pFlags: {}, user_id: uid, action}});
      } else if(kind === 'user' && (state === 'available' || state === 'unavailable')) {
        const u = users.get(uid);
        if(!u) continue;
        u.status = state === 'available' ?
          {_: 'userStatusOnline', expires: now() + 60} :
          {_: 'userStatusOffline', was_online: p.lastSeen || now()};
        push({_: 'updateShort', date: now(), update: {_: 'updateUserStatus', user_id: uid, status: u.status}});
      }
    }
  }

  function onChatsUpdate(list) {
    for(const c of list) {
      if(!c?.id) continue;
      const key = chatKeyOf(c.id);
      if(!key) continue;
      if(c.name) upsertChat(c);
      if(ready && c.unreadCount === 0) markReadFromPhone(key);
      else if(typeof c.unreadCount === 'number') chatUnread[key] = c.unreadCount;
    }
  }

  function markReadFromPhone(key) {
    const last = lastOf(key);
    if(!last) return;
    const rs = readState[key] || (readState[key] = {inbox: 0, outbox: 0});
    if(rs.inbox >= last.id) return;
    rs.inbox = last.id;
    bumpPts();
    push(updatesOf([{_: 'updateReadHistoryInbox', peer: peerOf(key), max_id: last.id, still_unread_count: 0, pts: getPts(), pts_count: 1}]));
    dirty();
  }

  const lastOf = (key) => { for(let i = msgs.length - 1; i >= 0; i--) if(msgs[i].key === key) return msgs[i]; return null; };

  /** subscribe so WhatsApp sends us this contact's typing / online presence */
  function watch(jid) {
    if(!sock || !jid || !isPn(jid) && !isLid(jid)) return;
    const t = watched.get(jid);
    if(t && Date.now() - t < WATCH_TTL_MS) return;
    watched.set(jid, Date.now());
    sock.presenceSubscribe(jid).catch(() => watched.delete(jid));
  }

  function watchTop() {
    const keys = [...new Set(msgs.slice(-400).map((m) => m.key))].filter((k) => k[0] === 'u' && +k.slice(1) !== SELF).slice(-15);
    keys.forEach((k) => watch(jidOfKey(k)));
  }

  /* ───────────────────────────── attach / lifecycle ───────────────────────────── */

  function bind(s) {
    const safe = (name, fn) => s.ev.on(name, (...a) => { try { fn(...a); } catch(e) { log(`${name} handler:`, e.stack || e.message); } });
    safe('messaging-history.set', onHistory);
    safe('messages.upsert', onUpsert);
    safe('messages.update', onMessagesUpdate);
    safe('presence.update', onPresence);
    safe('contacts.upsert', (l) => { l.forEach((c) => upsertContact(c, true)); dirty(); });
    safe('contacts.update', (l) => { l.forEach((c) => upsertContact(c, true)); dirty(); });
    safe('chats.upsert', (l) => { l.forEach(upsertChat); dirty(); });
    safe('chats.update', (l) => { onChatsUpdate(l); dirty(); });
    safe('lid-mapping.update', ({lid, pn}) => learn(lid, pn));
    safe('groups.upsert', (l) => { l.forEach(upsertGroupMeta); dirty(); });
    safe('groups.update', (l) => {
      for(const meta of l) {
        if(!meta.id) continue;
        const id = ensureGroup(meta.id, meta.subject), g = groups.get(id);
        const cached = metaCache.get(norm(meta.id));
        if(cached) Object.assign(cached, Object.fromEntries(Object.entries(meta).filter(([k, v]) => v != null && k !== 'participants')));
        if(typeof meta.announce === 'boolean' && g.announce !== meta.announce) {
          g.announce = meta.announce;
          if(ready) push({_: 'updates', updates: [{_: 'updateChat', chat_id: id}], users: [], chats: [tlChat(id)], date: now(), seq: 0});
        }
      }
      dirty();
    });
    safe('group-participants.update', ({id, participants, action}) => {
      const cid = ensureGroup(id);
      const cached = metaCache.get(norm(id));
      if(cached) {
        const raw = (participants || []).map((p) => (typeof p === 'string' ? {id: p} : p));
        const has = new Set((cached.participants || []).map((x) => x.id));
        if(action === 'add') cached.participants = [...(cached.participants || []), ...raw.filter((p) => !has.has(p.id))];
        else if(action === 'remove') cached.participants = (cached.participants || []).filter((x) => !raw.some((p) => p.id === x.id));
        else if(action === 'promote' || action === 'demote') for(const x of cached.participants || []) if(raw.some((p) => p.id === x.id)) x.admin = action === 'promote' ? 'admin' : null;
      }
      for(const p of participants || []) {
        const uid = ensureUser(norm(p.phoneNumber || p.id || p), p.name || p.notify, NAME_RANK.notify);
        if(action === 'add') addMember(cid, uid);
        if(action === 'remove') { const g = groups.get(cid); if(g) g.members = g.members.filter((x) => x !== uid); }
      }
      dirty();
    });
  }

  /** called by wa-auth for every socket (a new one is made after the pairing restart) */
  function attach(entry, s, b) {
    B = b;
    sock = s;
    if(dir !== entry.dir) { dir = entry.dir; loadStore(); }
    bind(s);
  }

  /** called when the socket reports 'open' */
  function onOpen(entry) {
    const u = entry.user || {};
    const phone = u.phone || entry.phone;
    if(phone && !reg.self.phones.includes(phone)) reg.self.phones.push(phone);
    const lid = sock?.user?.lid && String(sock.user.lid).split(/[:@]/)[0];
    if(lid && !reg.self.lids.includes(lid)) reg.self.lids.push(lid);
    if(!ready) {
      armIdle(FIRST_CHUNK_MS);
      clearTimeout(maxT);
      maxT = setTimeout(finishHistory, MAX_WAIT_MS);
    } else {
      refreshGroups();
      watchTop();
    }
    dirty();
  }

  /* ───────────────────────────── store on disk ───────────────────────────── */

  function resetSim() {
    for(const id of [...users.keys()]) if(id !== SELF) users.delete(id);
    groups.clear();
    msgs.length = 0;
    for(const k of Object.keys(readState)) delete readState[k];
    byWaId.clear();
    reg = freshReg();
    setLastId(0);
    touch();
  }

  const storeFile = () => path.join(dir, 'store.json');

  function loadStore() {
    resetSim();
    let raw = null;
    try { raw = fs.readFileSync(storeFile(), 'utf8'); } catch(_) { /* no store yet */ }
    if(!raw) { ready = false; log('no store.json: waiting for the history WhatsApp sends when a device is linked'); return; }
    try {
      const s = JSON.parse(raw, B?.BufferJSON?.reviver);
      reg = {...freshReg(), ...s.reg};
      for(const [id, u] of s.users || []) users.set(+id, u);
      for(const [id, g] of s.groups || []) { groups.set(+id, g); if(g.jid) reg.jidById[+id] ||= g.jid; }
      for(const [id, u] of users) if(u.jid) reg.jidById[id] ||= u.jid;
      msgs.push(...(s.msgs || []));
      Object.assign(readState, s.readState || {});
      for(const m of msgs) if(m.waId && m.jid) byWaId.set(waKeyOf(m.jid, m.waId), m);
      for(const [jid, id] of Object.entries(reg.byJid)) { // old stores aliased phone JIDs to registry ids: fold them back onto Number(phone)
        const [user, server] = jid.split('@');
        if(server === 's.whatsapp.net' && /^\d{7,15}$/.test(user) && id !== Number(user)) { migrate(id, Number(user), jid); delete reg.byJid[jid]; delete reg.jidById[id]; }
      }
      setLastId(s.lastId || msgs.reduce((n, m) => Math.max(n, m.id), 0));
      setPts(Math.max(getPts(), s.pts || 0));
      touch();
      ready = true;
      log(`store loaded: ${msgs.length} messages, ${groups.size} groups, ${users.size} users`);
    } catch(e) {
      log('store.json unreadable, starting empty:', e.message);
      resetSim();
      ready = false;
    }
  }

  function save() {
    if(!dir || !ready) return;
    try {
      const data = JSON.stringify({reg, users: [...users], groups: [...groups], msgs, readState, lastId: getLastId(), pts: getPts()}, B?.BufferJSON?.replacer);
      fs.mkdirSync(dir, {recursive: true});
      fs.writeFileSync(storeFile() + '.tmp', data);
      fs.renameSync(storeFile() + '.tmp', storeFile());
    } catch(e) { log('save failed:', e.message); }
  }

  function dirty() {
    clearTimeout(saveT);
    saveT = setTimeout(save, SAVE_DEBOUNCE_MS);
    saveT.unref?.();
  }

  process.on('exit', save);
  for(const sig of ['SIGTERM', 'SIGINT']) process.once(sig, () => { save(); process.exit(0); });

  /* ───────────────────────────── tweb -> WhatsApp ───────────────────────────── */

  const active = () => !!(WA.getActive() && sock && WA.getActive().sock === sock);

  const MENTION_ENT = {messageEntityMentionName: 1, inputMessageEntityMentionName: 1};
  const entUserId = (u) => (u && typeof u === 'object' ? (u._ === 'inputUserSelf' ? SELF : u.user_id) : u);

  /**
   * Telegram entities -> WhatsApp text. Returns {text, mentions}:
   *  - bold/italic/... become WhatsApp markdown (WhatsApp has no entity list)
   *  - a mention entity (the shown name) becomes "@<number>" and its JID goes into `mentions` (-> contextInfo.mentionedJid)
   */
  function toWaText(text, entities = []) {
    const ents = entities.map((e) => ({...e}));
    const mentions = [];
    let out = text;
    // mentions first, from the end, shifting the formatting entities around them
    for(const e of ents.filter((x) => MENTION_ENT[x._]).sort((a, b) => b.offset - a.offset)) {
      const id = entUserId(e.user_id), jid = id != null && jidOfKey('u' + id);
      if(!jid) continue; // unknown user: leave the typed name as plain text
      const tag = '@' + userPart(jid), end = e.offset + e.length, delta = tag.length - e.length;
      out = out.slice(0, e.offset) + tag + out.slice(end);
      mentions.unshift(jid);
      for(const o of ents) {
        if(o === e || MENTION_ENT[o._]) continue;
        if(o.offset >= end) o.offset += delta;
        else if(o.offset <= e.offset && o.offset + o.length >= end) o.length += delta; // the mention sits inside it
      }
    }
    const mark = {messageEntityBold: '*', messageEntityItalic: '_', messageEntityStrike: '~', messageEntityCode: '`', messageEntityPre: '```'};
    const ops = [];
    for(const e of ents) {
      const m = mark[e._];
      if(m) ops.push({at: e.offset, text: m, open: true, end: e.offset + e.length}, {at: e.offset + e.length, text: m, open: false});
    }
    ops.sort((a, b) => b.at - a.at || (a.open ? -1 : 1)); // from the end so offsets stay valid; at a tie the opener goes in first, so it ends up after the closer
    for(const o of ops) out = out.slice(0, o.at) + o.text + out.slice(o.at);
    return {text: out, mentions: [...new Set(mentions)]};
  }

  function requireSock() {
    if(!active()) throw fail(500, 'WA_NOT_CONNECTED', 'WhatsApp is not connected');
    return sock;
  }

  async function send(peerKey, p) {
    const s = requireSock();
    const jid = jidOfKey(peerKey);
    if(!jid) throw fail(400, 'PEER_ID_INVALID');
    const opts = {};
    const replyId = p.reply_to?.reply_to_msg_id;
    if(replyId) {
      const q = msgs.find((m) => m.id === replyId);
      if(q?.wa) opts.quoted = {key: q.wa.key, message: q.wa.message};
    }
    const id = B.generateMessageIDV2 ? B.generateMessageIDV2(s.user?.id) : B.generateMessageID();
    const w = toWaText(p.message, p.entities);
    pendingIds.add(id);
    try {
      const sent = await s.sendMessage(jid, {text: w.text, ...(w.mentions.length && {mentions: w.mentions})}, {...opts, messageId: id});
      const r = ingest(sent || {key: {remoteJid: jid, fromMe: true, id}, status: 2, messageTimestamp: now(),
        message: w.mentions.length ? {extendedTextMessage: {text: w.text, contextInfo: {mentionedJid: w.mentions}}} : {conversation: w.text}});
      if(!r) throw fail(500, 'WA_SEND_FAILED');
      dirty();
      return r.m;
    } catch(e) {
      if(e.type) throw e;
      log('send failed:', e.message);
      throw fail(500, 'WA_SEND_FAILED', e.message);
    } finally {
      setTimeout(() => pendingIds.delete(id), 30000).unref?.();
    }
  }

  /** upload from tweb (inputMediaUploadedPhoto / inputMediaUploadedDocument) -> WhatsApp message */
  async function sendMedia(peerKey, p, buf) {
    const s = requireSock();
    const jid = jidOfKey(peerKey);
    if(!jid) throw fail(400, 'PEER_ID_INVALID');
    const im = p.media || {}, attrs = im.attributes || [], at = (n) => attrs.find((a) => a._ === n);
    const w = toWaText(p.message || '', p.entities), caption = w.text || undefined;
    let content;
    if(im._ === 'inputMediaUploadedPhoto') content = {image: buf, caption};
    else if(im._ === 'inputMediaUploadedDocument') {
      const mime = im.mime_type || 'application/octet-stream', audio = at('documentAttributeAudio'), video = at('documentAttributeVideo');
      if(at('documentAttributeSticker') || mime === 'image/webp') content = {sticker: buf};
      else if(audio?.pFlags?.voice || audio?.voice) content = {audio: buf, ptt: true, mimetype: 'audio/ogg; codecs=opus'};
      else if(audio) content = {audio: buf, mimetype: mime};
      else if(video && !im.pFlags?.force_file) content = {video: buf, caption, mimetype: mime, ...(at('documentAttributeAnimated') && {gifPlayback: true}), ...(video.pFlags?.round_message && {ptv: true})};
      else if(mime.startsWith('image/') && !im.pFlags?.force_file && !at('documentAttributeFilename')) content = {image: buf, caption};
      else content = {document: buf, mimetype: mime, fileName: at('documentAttributeFilename')?.file_name || 'file', caption};
    } else throw fail(400, 'MEDIA_INVALID');
    if(w.mentions.length) content.mentions = w.mentions;
    const opts = {}, replyId = p.reply_to?.reply_to_msg_id;
    if(replyId) { const q = msgs.find((m) => m.id === replyId); if(q?.wa) opts.quoted = {key: q.wa.key, message: q.wa.message}; }
    const id = B.generateMessageIDV2 ? B.generateMessageIDV2(s.user?.id) : B.generateMessageID();
    pendingIds.add(id);
    try {
      const sent = await s.sendMessage(jid, content, {...opts, messageId: id});
      const r = sent && ingest(sent);
      if(!r) throw fail(500, 'WA_SEND_FAILED');
      dirty();
      return r.m;
    } catch(e) {
      if(e.type) throw e;
      log('sendMedia failed:', e.message);
      throw fail(500, 'WA_SEND_FAILED', e.message);
    } finally { setTimeout(() => pendingIds.delete(id), 30000).unref?.(); }
  }

  async function edit(m, text, entities) {
    const s = requireSock();
    if(!m.wa || !m.out) throw fail(400, 'MESSAGE_EDIT_FORBIDDEN');
    const w = toWaText(text, entities);
    try { await s.sendMessage(m.jid, {text: w.text, edit: m.wa.key, ...(w.mentions.length && {mentions: w.mentions})}); } catch(e) { throw fail(500, 'WA_EDIT_FAILED', e.message); }
    m.text = w.text;
    m.mentions = mentionPairs(w.mentions);
    m.edit_date = now();
    bumpPts();
    dirty();
    return m;
  }

  /** revoke = "delete for everyone" (only possible for our own messages); otherwise just drop it locally */
  async function del(list, revoke) {
    for(const m of list) {
      if(revoke && m.out && m.wa && active()) {
        try { await sock.sendMessage(m.jid, {delete: m.wa.key}); } catch(e) { log('revoke failed:', e.message); }
      }
      removeMessage(m, false);
    }
    dirty();
  }

  function markRead(peerKey, maxId) {
    const rs = readState[peerKey] || (readState[peerKey] = {inbox: 0, outbox: 0});
    const upTo = maxId || getLastId();
    const keys = msgs.filter((m) => m.key === peerKey && !m.out && m.id > rs.inbox && m.id <= upTo && m.wa).map((m) => m.wa.key);
    rs.inbox = Math.max(rs.inbox, upTo);
    if(keys.length && active()) sock.readMessages(keys).catch((e) => log('readMessages failed:', e.message));
    dirty();
  }

  const ACTIONS = {sendMessageTypingAction: 'composing', sendMessageRecordAudioAction: 'recording', sendMessageRecordRoundAction: 'recording', sendMessageCancelAction: 'paused'};

  function setTyping(peerKey, action) {
    const state = ACTIONS[action?._];
    const jid = jidOfKey(peerKey);
    if(!state || !jid || !active()) return;
    sock.sendPresenceUpdate(state, jid).catch(() => {});
  }

  function setOnline(online) {
    if(active()) sock.sendPresenceUpdate(online ? 'available' : 'unavailable').catch(() => {});
  }

  /** the UI opened a chat: ask WhatsApp for that contact's presence */
  function openChat(peerKey) {
    if(active() && peerKey[0] === 'u' && +peerKey.slice(1) !== SELF) watch(jidOfKey(peerKey));
  }

  return {attach, onOpen, whenReady, isReady: () => ready, active, send, edit, del, markRead, setTyping, setOnline, openChat, render, groupMeta, download, sendMedia, markDirty: dirty,
    getMessage: async(key) => findMsg(key)?.wa?.message, byId: (id) => msgs.find((m) => m.id === id)};
};
 