const express = require('express');
const cors = require('cors');
const path = require('path');
const http = require('http');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';

// Webhook verify token (from .env or default)
const WEBHOOK_VERIFY_TOKEN = process.env.WEBHOOK_VERIFY_TOKEN || '194bca0fac0c4e645513770521c9ea8c2ec2f0848ee5c1328cafe1371b891a0f';

// Keep-alive configuration
let pingTargetUrl = process.env.RENDER_EXTERNAL_URL || process.env.APP_URL || process.env.SELF_URL || '';
const PING_INTERVAL_MS = parseInt(process.env.PING_INTERVAL_MS || '60000', 10); // Default 1 minute
let isPingEnabled = true;
let pingTimer = null;
const pingHistory = [];
const MAX_PING_HISTORY = 60;

// Packet Storage (In-Memory Ring Buffer)
const MAX_PACKETS = 500;
const packets = [];
const sseClients = new Set();

// Statistics
const stats = {
  totalPackets: 0,
  totalMessages: 0,
  totalStatuses: 0,
  totalSimulated: 0,
  totalVerifications: 0,
  serverStartTime: new Date().toISOString(),
  lastPacketReceivedAt: null,
  lastPingAt: null,
  lastPingStatus: null,
  lastPingLatencyMs: null
};

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Request logging middleware (excluding repetitive ping noise)
app.use((req, res, next) => {
  if (req.path !== '/ping' && req.path !== '/health' && req.path !== '/api/events/stream') {
    console.log(`[${new Date().toLocaleTimeString()}] ${req.method} ${req.originalUrl} - IP: ${req.ip}`);
  }
  next();
});

// Serve frontend static files
app.use(express.static(path.join(__dirname, 'public')));

// Helper: Broadcast packet to all connected SSE clients
function broadcastEvent(eventType, data) {
  const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(payload);
    } catch (err) {
      console.error('Error sending SSE to client:', err.message);
      sseClients.delete(client);
    }
  }
}

// Helper: Parse WhatsApp payload into rich summary
function parseWhatsAppPayload(body) {
  const result = {
    category: 'other',
    type: 'unknown',
    senderPhone: null,
    senderName: null,
    recipientPhone: null,
    wabaId: null,
    phoneNumberId: null,
    messageId: null,
    messageType: null,
    messageText: null,
    mediaUrl: null,
    mediaMimeType: null,
    caption: null,
    status: null,
    timestamp: null,
    errors: [],
    details: {}
  };

  if (!body || typeof body !== 'object') {
    return result;
  }

  // Detect WhatsApp Cloud API structure
  if (body.object === 'whatsapp_business_account' || Array.isArray(body.entry)) {
    const entry = body.entry && body.entry[0];
    if (entry) {
      result.wabaId = entry.id;
      const change = entry.changes && entry.changes[0];
      if (change && change.value) {
        const val = change.value;
        result.details.messaging_product = val.messaging_product;
        result.details.metadata = val.metadata;
        if (val.metadata) {
          result.phoneNumberId = val.metadata.phone_number_id;
          result.recipientPhone = val.metadata.display_phone_number;
        }

        // Contact info
        if (val.contacts && val.contacts[0]) {
          result.senderName = val.contacts[0].profile ? val.contacts[0].profile.name : null;
          result.senderPhone = val.contacts[0].wa_id;
        }

        // 1. Incoming Messages
        if (val.messages && val.messages.length > 0) {
          result.category = 'message';
          const msg = val.messages[0];
          result.messageId = msg.id;
          result.messageType = msg.type;
          result.timestamp = msg.timestamp ? new Date(parseInt(msg.timestamp, 10) * 1000).toISOString() : new Date().toISOString();
          result.senderPhone = result.senderPhone || msg.from;

          if (msg.type === 'text' && msg.text) {
            result.messageText = msg.text.body;
            result.type = 'message:text';
          } else if (msg.type === 'image' && msg.image) {
            result.type = 'message:image';
            result.caption = msg.image.caption || null;
            result.mediaMimeType = msg.image.mime_type;
            result.messageText = `[Imagen] ${msg.image.caption || ''}`.trim();
          } else if (msg.type === 'audio' && msg.audio) {
            result.type = 'message:audio';
            result.mediaMimeType = msg.audio.mime_type;
            result.messageText = `[Audio/Nota de voz]`;
          } else if (msg.type === 'video' && msg.video) {
            result.type = 'message:video';
            result.caption = msg.video.caption || null;
            result.mediaMimeType = msg.video.mime_type;
            result.messageText = `[Video] ${msg.video.caption || ''}`.trim();
          } else if (msg.type === 'document' && msg.document) {
            result.type = 'message:document';
            result.caption = msg.document.filename || msg.document.caption || null;
            result.mediaMimeType = msg.document.mime_type;
            result.messageText = `[Documento: ${msg.document.filename || 'archivo'}]`;
          } else if (msg.type === 'interactive' && msg.interactive) {
            result.type = 'message:interactive';
            if (msg.interactive.type === 'button_reply') {
              result.messageText = `[Botón]: ${msg.interactive.button_reply.title} (ID: ${msg.interactive.button_reply.id})`;
            } else if (msg.interactive.type === 'list_reply') {
              result.messageText = `[Lista]: ${msg.interactive.list_reply.title} (ID: ${msg.interactive.list_reply.id})`;
            } else {
              result.messageText = `[Interactivo: ${msg.interactive.type}]`;
            }
          } else if (msg.type === 'button' && msg.button) {
            result.type = 'message:button';
            result.messageText = `[Botón]: ${msg.button.text}`;
          } else if (msg.type === 'reaction' && msg.reaction) {
            result.type = 'message:reaction';
            result.messageText = `[Reacción]: ${msg.reaction.emoji || 'icono'} a mensaje ${msg.reaction.message_id}`;
          } else if (msg.type === 'location' && msg.location) {
            result.type = 'message:location';
            result.messageText = `[Ubicación]: Lat ${msg.location.latitude}, Long ${msg.location.longitude} (${msg.location.name || ''})`;
          } else if (msg.type === 'contacts' && msg.contacts) {
            result.type = 'message:contacts';
            result.messageText = `[Contactos]: ${msg.contacts.length} contacto(s)`;
          } else {
            result.type = `message:${msg.type || 'unknown'}`;
            result.messageText = `[Mensaje tipo: ${msg.type}]`;
          }

          if (msg.errors && msg.errors.length > 0) {
            result.errors = msg.errors;
          }
        }

        // 2. Status Updates (sent, delivered, read, failed)
        else if (val.statuses && val.statuses.length > 0) {
          result.category = 'status';
          const st = val.statuses[0];
          result.messageId = st.id;
          result.status = st.status;
          result.type = `status:${st.status}`;
          result.recipientPhone = st.recipient_id;
          result.timestamp = st.timestamp ? new Date(parseInt(st.timestamp, 10) * 1000).toISOString() : new Date().toISOString();
          result.messageText = `Estado: ${st.status.toUpperCase()} para ${st.recipient_id}`;

          if (st.errors && st.errors.length > 0) {
            result.errors = st.errors;
            result.type = 'status:failed';
          }
          if (st.pricing) {
            result.details.pricing = st.pricing;
          }
          if (st.conversation) {
            result.details.conversation = st.conversation;
          }
        }

        // 3. Other changes (e.g. template status updates)
        else if (change.field) {
          result.category = 'system';
          result.type = `field:${change.field}`;
          result.messageText = `Evento de campo: ${change.field}`;
        }
      }
    }
  }

  return result;
}

// -------------------------------------------------------------
// WEBHOOK VERIFICATION (GET /webhook, GET /api/webhook, GET /)
// -------------------------------------------------------------
function handleVerification(req, res) {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  stats.totalVerifications++;

  console.log(`[VERIFICACION] Solicitud recibida: mode=${mode}, token=${token ? token.substring(0, 8) + '...' : 'none'}`);

  if (mode && token) {
    if (mode === 'subscribe' && token === WEBHOOK_VERIFY_TOKEN) {
      console.log('✅ [VERIFICACION EXITOSA] Token validado correctamente. Retornando challenge a Meta.');
      broadcastEvent('verification', {
        success: true,
        timestamp: new Date().toISOString(),
        mode,
        ip: req.ip
      });
      return res.status(200).send(challenge);
    } else {
      console.warn('❌ [VERIFICACION FALLIDA] Token no coincide con WEBHOOK_VERIFY_TOKEN configurado.');
      broadcastEvent('verification', {
        success: false,
        timestamp: new Date().toISOString(),
        mode,
        tokenProvided: token,
        ip: req.ip
      });
      return res.status(403).json({
        error: 'Forbidden',
        message: 'El verify_token proporcionado no coincide con el token del servidor.'
      });
    }
  }

  // If no query parameters, redirect or serve status
  return res.status(400).json({
    error: 'Bad Request',
    message: 'Parámetros hub.mode, hub.verify_token y hub.challenge requeridos para verificación.'
  });
}

app.get('/webhook', handleVerification);
app.get('/api/webhook', handleVerification);

// -------------------------------------------------------------
// WEBHOOK RECEIVER (POST /webhook, POST /api/webhook)
// -------------------------------------------------------------
function handleIncomingPacket(req, res) {
  const rawBody = req.body;
  const parsed = parseWhatsAppPayload(rawBody);

  const packet = {
    id: `pkt_${Date.now()}_${Math.random().toString(36).substr(2, 7)}`,
    receivedAt: new Date().toISOString(),
    ip: req.ip || req.connection.remoteAddress,
    headers: {
      'user-agent': req.headers['user-agent'] || '',
      'x-hub-signature-256': req.headers['x-hub-signature-256'] || '',
      'content-type': req.headers['content-type'] || ''
    },
    parsed,
    raw: rawBody
  };

  // Update statistics
  stats.totalPackets++;
  stats.lastPacketReceivedAt = packet.receivedAt;
  if (parsed.category === 'message') stats.totalMessages++;
  if (parsed.category === 'status') stats.totalStatuses++;

  // Store in memory (ring buffer)
  packets.unshift(packet);
  if (packets.length > MAX_PACKETS) {
    packets.pop();
  }

  console.log(`📥 [PAQUETE RECIBIDO] ID: ${packet.id} | Tipo: ${parsed.type} | De: ${parsed.senderPhone || parsed.recipientPhone || 'N/A'} | Texto: "${parsed.messageText || ''}"`);

  // Broadcast to all active SSE dashboards
  broadcastEvent('packet', packet);
  broadcastEvent('stats', stats);

  // Meta requires immediate 200 OK
  return res.status(200).json({
    status: 'EVENT_RECEIVED',
    id: packet.id,
    timestamp: packet.receivedAt
  });
}

app.post('/webhook', handleIncomingPacket);
app.post('/api/webhook', handleIncomingPacket);

// -------------------------------------------------------------
// KEEP-ALIVE PING & HEALTH ENDPOINTS
// -------------------------------------------------------------
app.get(['/health', '/ping', '/api/ping'], (req, res) => {
  res.status(200).json({
    status: 'ok',
    service: 'whatsapp-webhook-receiver',
    uptimeSeconds: Math.floor(process.uptime()),
    timestamp: new Date().toISOString(),
    totalPackets: stats.totalPackets,
    message: 'pong'
  });
});

// Execute keep-alive self ping
async function executeKeepAlivePing() {
  if (!isPingEnabled) return;

  const effectiveUrl = (pingTargetUrl && pingTargetUrl.trim().length > 0)
    ? pingTargetUrl.replace(/\/$/, '')
    : `http://127.0.0.1:${PORT}`;

  const targetPingUrl = `${effectiveUrl}/api/ping`;
  const startTime = Date.now();

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);

    const response = await fetch(targetPingUrl, {
      method: 'GET',
      headers: {
        'User-Agent': 'WhatsApp-Webhook-KeepAlive/1.0',
        'X-KeepAlive-Ping': 'true'
      },
      signal: controller.signal
    });

    clearTimeout(timeout);
    const latency = Date.now() - startTime;
    const isSuccess = response.ok;

    stats.lastPingAt = new Date().toISOString();
    stats.lastPingStatus = isSuccess ? 'success' : `http_${response.status}`;
    stats.lastPingLatencyMs = latency;

    const pingEntry = {
      id: `png_${Date.now()}`,
      timestamp: stats.lastPingAt,
      url: targetPingUrl,
      status: response.status,
      success: isSuccess,
      latencyMs: latency
    };

    pingHistory.unshift(pingEntry);
    if (pingHistory.length > MAX_PING_HISTORY) pingHistory.pop();

    broadcastEvent('ping', pingEntry);
    broadcastEvent('stats', stats);

    console.log(`⏱️ [KEEP-ALIVE PING] ${isSuccess ? '✅ OK' : '⚠️ Fallo'} (${response.status}) -> ${targetPingUrl} [${latency}ms]`);
  } catch (error) {
    const latency = Date.now() - startTime;
    stats.lastPingAt = new Date().toISOString();
    stats.lastPingStatus = 'error: ' + error.message;
    stats.lastPingLatencyMs = latency;

    const pingEntry = {
      id: `png_${Date.now()}`,
      timestamp: stats.lastPingAt,
      url: targetPingUrl,
      status: 0,
      success: false,
      error: error.message,
      latencyMs: latency
    };

    pingHistory.unshift(pingEntry);
    if (pingHistory.length > MAX_PING_HISTORY) pingHistory.pop();

    broadcastEvent('ping', pingEntry);
    broadcastEvent('stats', stats);

    console.warn(`⏱️ [KEEP-ALIVE PING ERROR] -> ${targetPingUrl}: ${error.message}`);
  }
}

function startPingScheduler() {
  if (pingTimer) clearInterval(pingTimer);
  console.log(`⚡ [KEEP-ALIVE] Iniciando temporizador cada ${PING_INTERVAL_MS / 1000}s hacia: ${pingTargetUrl || 'localhost'}`);
  pingTimer = setInterval(executeKeepAlivePing, PING_INTERVAL_MS);
  // Initial ping after 5 seconds to test
  setTimeout(executeKeepAlivePing, 5000);
}

// -------------------------------------------------------------
// REST API FOR DASHBOARD
// -------------------------------------------------------------

// SSE stream for real-time live events
app.get('/api/events/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();

  // Send initial state
  res.write(`event: init\ndata: ${JSON.stringify({
    stats,
    verifyToken: WEBHOOK_VERIFY_TOKEN,
    pingConfig: {
      targetUrl: pingTargetUrl,
      intervalMs: PING_INTERVAL_MS,
      enabled: isPingEnabled
    }
  })}\n\n`);

  sseClients.add(res);

  req.on('close', () => {
    sseClients.delete(res);
  });
});

// List packets with optional search / filter
app.get('/api/packets', (req, res) => {
  const { category, search, limit = 100 } = req.query;
  let filtered = [...packets];

  if (category && category !== 'all') {
    filtered = filtered.filter(p => p.parsed.category === category);
  }

  if (search && search.trim().length > 0) {
    const q = search.toLowerCase();
    filtered = filtered.filter(p => {
      const parsed = p.parsed;
      return (
        (parsed.senderPhone && parsed.senderPhone.includes(q)) ||
        (parsed.senderName && parsed.senderName.toLowerCase().includes(q)) ||
        (parsed.messageText && parsed.messageText.toLowerCase().includes(q)) ||
        (parsed.messageId && parsed.messageId.toLowerCase().includes(q)) ||
        (parsed.type && parsed.type.toLowerCase().includes(q)) ||
        (p.id && p.id.toLowerCase().includes(q))
      );
    });
  }

  const max = Math.min(parseInt(limit, 10) || 100, MAX_PACKETS);
  res.json({
    total: packets.length,
    filteredCount: filtered.length,
    packets: filtered.slice(0, max)
  });
});

// Get single packet
app.get('/api/packets/:id', (req, res) => {
  const packet = packets.find(p => p.id === req.params.id);
  if (!packet) {
    return res.status(404).json({ error: 'Packet not found' });
  }
  res.json(packet);
});

// Clear packets
app.delete('/api/packets', (req, res) => {
  packets.length = 0;
  stats.totalPackets = 0;
  stats.totalMessages = 0;
  stats.totalStatuses = 0;
  stats.totalSimulated = 0;
  broadcastEvent('cleared', { timestamp: new Date().toISOString() });
  broadcastEvent('stats', stats);
  res.json({ success: true, message: 'Registro de paquetes vaciado' });
});

// Export packets as JSON
app.get('/api/export', (req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Content-Disposition', `attachment; filename=whatsapp-webhook-export-${Date.now()}.json`);
  res.send(JSON.stringify({
    exportedAt: new Date().toISOString(),
    totalPackets: packets.length,
    packets
  }, null, 2));
});

// Get status & config
app.get('/api/status', (req, res) => {
  res.json({
    stats,
    config: {
      port: PORT,
      verifyToken: WEBHOOK_VERIFY_TOKEN,
      pingTargetUrl,
      pingIntervalMs: PING_INTERVAL_MS,
      isPingEnabled
    },
    pingHistory: pingHistory.slice(0, 20)
  });
});

// Update Ping configuration from UI
app.post('/api/ping/config', (req, res) => {
  const { url, enabled } = req.body;
  if (typeof url === 'string') {
    pingTargetUrl = url.trim();
  }
  if (typeof enabled === 'boolean') {
    isPingEnabled = enabled;
  }
  broadcastEvent('pingConfig', {
    targetUrl: pingTargetUrl,
    enabled: isPingEnabled,
    intervalMs: PING_INTERVAL_MS
  });
  res.json({ success: true, pingTargetUrl, isPingEnabled });
});

// Manual Ping trigger
app.post('/api/ping/trigger', async (req, res) => {
  await executeKeepAlivePing();
  res.json({
    success: true,
    lastPingStatus: stats.lastPingStatus,
    lastPingLatencyMs: stats.lastPingLatencyMs,
    lastPingAt: stats.lastPingAt
  });
});

// Simulator: inject fake WhatsApp webhook payload for testing
app.post('/api/simulate', (req, res) => {
  const { scenario = 'text_message', customText, customPhone, customName } = req.body;

  const phone = customPhone || '56951431978';
  const name = customName || 'Cliente Demostración';
  const nowUnix = Math.floor(Date.now() / 1000).toString();
  const mockMsgId = `wamid.HBgL${Date.now()}SIMULATED==`;

  let mockPayload = {};

  switch (scenario) {
    case 'text_message':
      mockPayload = {
        object: 'whatsapp_business_account',
        entry: [{
          id: '2501763706975264',
          changes: [{
            value: {
              messaging_product: 'whatsapp',
              metadata: {
                display_phone_number: '+56 9 5143 1978',
                phone_number_id: '1361059717079550'
              },
              contacts: [{
                profile: { name: name },
                wa_id: phone
              }],
              messages: [{
                from: phone,
                id: mockMsgId,
                timestamp: nowUnix,
                text: { body: customText || '¡Hola! Me gustaría cotizar un servicio y obtener más información.' },
                type: 'text'
              }]
            },
            field: 'messages'
          }]
        }]
      };
      break;

    case 'image_message':
      mockPayload = {
        object: 'whatsapp_business_account',
        entry: [{
          id: '2501763706975264',
          changes: [{
            value: {
              messaging_product: 'whatsapp',
              metadata: {
                display_phone_number: '+56 9 5143 1978',
                phone_number_id: '1361059717079550'
              },
              contacts: [{
                profile: { name: name },
                wa_id: phone
              }],
              messages: [{
                from: phone,
                id: mockMsgId,
                timestamp: nowUnix,
                type: 'image',
                image: {
                  mime_type: 'image/jpeg',
                  sha256: 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6',
                  id: 'media_1234567890',
                  caption: customText || 'Comprobante de transferencia adjunto'
                }
              }]
            },
            field: 'messages'
          }]
        }]
      };
      break;

    case 'button_reply':
      mockPayload = {
        object: 'whatsapp_business_account',
        entry: [{
          id: '2501763706975264',
          changes: [{
            value: {
              messaging_product: 'whatsapp',
              metadata: {
                display_phone_number: '+56 9 5143 1978',
                phone_number_id: '1361059717079550'
              },
              contacts: [{
                profile: { name: name },
                wa_id: phone
              }],
              messages: [{
                from: phone,
                id: mockMsgId,
                timestamp: nowUnix,
                type: 'interactive',
                interactive: {
                  type: 'button_reply',
                  button_reply: {
                    id: 'btn_confirmar_pedido',
                    title: customText || 'Confirmar Pedido'
                  }
                }
              }]
            },
            field: 'messages'
          }]
        }]
      };
      break;

    case 'status_delivered':
      mockPayload = {
        object: 'whatsapp_business_account',
        entry: [{
          id: '2501763706975264',
          changes: [{
            value: {
              messaging_product: 'whatsapp',
              metadata: {
                display_phone_number: '+56 9 5143 1978',
                phone_number_id: '1361059717079550'
              },
              statuses: [{
                id: mockMsgId,
                status: 'delivered',
                timestamp: nowUnix,
                recipient_id: phone,
                conversation: {
                  id: 'conv_123456789',
                  origin: { type: 'service' }
                },
                pricing: {
                  billable: true,
                  pricing_model: 'CBP',
                  category: 'service'
                }
              }]
            },
            field: 'messages'
          }]
        }]
      };
      break;

    case 'status_read':
      mockPayload = {
        object: 'whatsapp_business_account',
        entry: [{
          id: '2501763706975264',
          changes: [{
            value: {
              messaging_product: 'whatsapp',
              metadata: {
                display_phone_number: '+56 9 5143 1978',
                phone_number_id: '1361059717079550'
              },
              statuses: [{
                id: mockMsgId,
                status: 'read',
                timestamp: nowUnix,
                recipient_id: phone
              }]
            },
            field: 'messages'
          }]
        }]
      };
      break;

    case 'status_failed':
      mockPayload = {
        object: 'whatsapp_business_account',
        entry: [{
          id: '2501763706975264',
          changes: [{
            value: {
              messaging_product: 'whatsapp',
              metadata: {
                display_phone_number: '+56 9 5143 1978',
                phone_number_id: '1361059717079550'
              },
              statuses: [{
                id: mockMsgId,
                status: 'failed',
                timestamp: nowUnix,
                recipient_id: phone,
                errors: [{
                  code: 131026,
                  title: 'Message Undeliverable',
                  message: 'El número de destino no está registrado en WhatsApp o tiene bloqueada la recepción.'
                }]
              }]
            },
            field: 'messages'
          }]
        }]
      };
      break;

    default:
      mockPayload = req.body.payload || {
        object: 'whatsapp_business_account',
        entry: [{
          id: '2501763706975264',
          changes: [{
            value: {
              messaging_product: 'whatsapp',
              metadata: { display_phone_number: '+56 9 5143 1978', phone_number_id: '1361059717079550' }
            },
            field: 'messages'
          }]
        }]
      };
  }

  // Inject simulation flag into parsed result
  const parsed = parseWhatsAppPayload(mockPayload);
  parsed.isSimulated = true;

  const packet = {
    id: `sim_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`,
    receivedAt: new Date().toISOString(),
    ip: '127.0.0.1 (Simulador)',
    headers: {
      'user-agent': 'WhatsApp-Simulator/1.0 (UI Test)',
      'content-type': 'application/json'
    },
    parsed,
    raw: mockPayload
  };

  stats.totalPackets++;
  stats.totalSimulated++;
  stats.lastPacketReceivedAt = packet.receivedAt;
  if (parsed.category === 'message') stats.totalMessages++;
  if (parsed.category === 'status') stats.totalStatuses++;

  packets.unshift(packet);
  if (packets.length > MAX_PACKETS) packets.pop();

  broadcastEvent('packet', packet);
  broadcastEvent('stats', stats);

  res.json({
    success: true,
    message: 'Paquete simulado inyectado exitosamente',
    packet
  });
});

// Fallback: serve index.html for single-page app
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start Server
const server = app.listen(PORT, HOST, () => {
  console.log('===========================================================');
  console.log(`🚀 WHATSAPP WEBHOOK RECEIVER INICIADO EN http://${HOST === '0.0.0.0' ? 'localhost' : HOST}:${PORT}`);
  console.log(`🔑 Verify Token: "${WEBHOOK_VERIFY_TOKEN}"`);
  console.log(`📡 URL Webhook para Meta: https://<tu-dominio>.onrender.com/webhook`);
  console.log(`⏱️ Auto-Ping Keep-Alive: ${isPingEnabled ? 'Activado (cada ' + (PING_INTERVAL_MS / 1000) + 's)' : 'Desactivado'}`);
  console.log('===========================================================');

  startPingScheduler();
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Recibida señal SIGTERM, cerrando servidor...');
  if (pingTimer) clearInterval(pingTimer);
  server.close(() => {
    process.exit(0);
  });
});
