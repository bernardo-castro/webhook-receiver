// State Management
let allPackets = [];
let currentCategory = 'all';
let searchQuery = '';
let verifyToken = '';
let pingConfig = { targetUrl: '', intervalMs: 60000, enabled: true };
let eventSource = null;
let lastPingTime = null;

// DOM Elements
const packetsFeed = document.getElementById('packetsFeed');
const emptyState = document.getElementById('emptyState');
const connectionStatus = document.getElementById('connectionStatus');
const connectionText = document.getElementById('connectionText');
const keepAliveText = document.getElementById('keepAliveText');
const btnKeepAliveStatus = document.getElementById('btnKeepAliveStatus');

// Stats Elements
const statTotalPackets = document.getElementById('statTotalPackets');
const statTotalMessages = document.getElementById('statTotalMessages');
const statTotalStatuses = document.getElementById('statTotalStatuses');
const statLastPing = document.getElementById('statLastPing');

// Tab Counts
const countAll = document.getElementById('countAll');
const countMessages = document.getElementById('countMessages');
const countStatuses = document.getElementById('countStatuses');
const countSystem = document.getElementById('countSystem');

// Guides & Inputs
const guideWebhookUrl = document.getElementById('guideWebhookUrl');
const guideVerifyToken = document.getElementById('guideVerifyToken');
const searchInput = document.getElementById('searchInput');

// Buttons
const btnCopyWebhookUrl = document.getElementById('btnCopyWebhookUrl');
const btnCopyToken = document.getElementById('btnCopyToken');
const btnExport = document.getElementById('btnExport');
const btnClear = document.getElementById('btnClear');
const btnSimulateDropdown = document.getElementById('btnSimulateDropdown');
const simulateMenu = document.getElementById('simulateMenu');

// Modals
const packetModal = document.getElementById('packetModal');
const btnCloseModal = document.getElementById('btnCloseModal');
const btnCopyModalJson = document.getElementById('btnCopyModalJson');
const modalJsonContent = document.getElementById('modalJsonContent');
const modalTitle = document.getElementById('modalTitle');
const modalSender = document.getElementById('modalSender');
const modalType = document.getElementById('modalType');
const modalTimestamp = document.getElementById('modalTimestamp');
const modalMessageId = document.getElementById('modalMessageId');
const modalCategoryBadge = document.getElementById('modalCategoryBadge');

// Keep Alive Modal Elements
const keepAliveModal = document.getElementById('keepAliveModal');
const btnCloseKeepAlive = document.getElementById('btnCloseKeepAlive');
const inputPingUrl = document.getElementById('inputPingUrl');
const btnSavePingUrl = document.getElementById('btnSavePingUrl');
const chkPingEnabled = document.getElementById('chkPingEnabled');
const btnTriggerPingNow = document.getElementById('btnTriggerPingNow');
const pingHistoryBody = document.getElementById('pingHistoryBody');

let currentActiveJson = null;

// =========================================================
// INITIALIZATION & SERVER-SENT EVENTS (SSE)
// =========================================================
function initApp() {
  updateWebhookUrls();
  setupEventListeners();
  connectSSE();
  fetchInitialPackets();
  fetchPingStatus();

  // Relative time updater every 10s
  setInterval(updateRelativeTimestamps, 10000);
}

function updateWebhookUrls() {
  const currentOrigin = window.location.origin;
  const webhookUrl = `${currentOrigin}/webhook`;
  if (guideWebhookUrl) guideWebhookUrl.textContent = webhookUrl;
}

function connectSSE() {
  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource('/api/events/stream');

  eventSource.onopen = () => {
    connectionStatus.className = 'status-pill connected';
    connectionText.textContent = 'En Vivo (SSE)';
  };

  eventSource.onerror = () => {
    connectionStatus.className = 'status-pill disconnected';
    connectionText.textContent = 'Reconectando...';
  };

  eventSource.addEventListener('init', (e) => {
    const data = JSON.parse(e.data);
    if (data.verifyToken) {
      verifyToken = data.verifyToken;
      if (guideVerifyToken) guideVerifyToken.textContent = verifyToken;
    }
    if (data.pingConfig) {
      pingConfig = data.pingConfig;
      updatePingUI();
    }
    if (data.stats) {
      updateStatsUI(data.stats);
    }
  });

  eventSource.addEventListener('packet', (e) => {
    const packet = JSON.parse(e.data);
    addPacketToFeed(packet, true);
  });

  eventSource.addEventListener('stats', (e) => {
    const stats = JSON.parse(e.data);
    updateStatsUI(stats);
  });

  eventSource.addEventListener('ping', (e) => {
    const ping = JSON.parse(e.data);
    addPingToHistory(ping);
  });

  eventSource.addEventListener('verification', (e) => {
    const data = JSON.parse(e.data);
    if (data.success) {
      showToast('✅ Verificación de Webhook exitosa con Meta', 'success');
    } else {
      showToast('❌ Intento de verificación con token inválido', 'error');
    }
  });

  eventSource.addEventListener('cleared', () => {
    allPackets = [];
    renderFeed();
    showToast('Registro de paquetes vaciado', 'info');
  });

  eventSource.addEventListener('pingConfig', (e) => {
    pingConfig = JSON.parse(e.data);
    updatePingUI();
  });
}

async function fetchInitialPackets() {
  try {
    const res = await fetch('/api/packets?limit=100');
    const data = await res.json();
    if (data.packets && Array.isArray(data.packets)) {
      allPackets = data.packets;
      renderFeed();
    }
  } catch (err) {
    console.error('Error cargando paquetes iniciales:', err);
  }
}

async function fetchPingStatus() {
  try {
    const res = await fetch('/api/status');
    const data = await res.json();
    if (data.config) {
      verifyToken = data.config.verifyToken;
      if (guideVerifyToken) guideVerifyToken.textContent = verifyToken;
      pingConfig.targetUrl = data.config.pingTargetUrl;
      pingConfig.enabled = data.config.isPingEnabled;
      pingConfig.intervalMs = data.config.pingIntervalMs;
      updatePingUI();
    }
    if (data.stats) {
      updateStatsUI(data.stats);
    }
    if (data.pingHistory && Array.isArray(data.pingHistory)) {
      renderPingHistory(data.pingHistory);
    }
  } catch (err) {
    console.error('Error cargando estado del ping:', err);
  }
}

// =========================================================
// PACKET RENDERING & FEED
// =========================================================
function addPacketToFeed(packet, isNew = false) {
  // Prevent duplicate insertion
  if (!allPackets.some(p => p.id === packet.id)) {
    allPackets.unshift(packet);
    if (allPackets.length > 500) allPackets.pop();
  }
  renderFeed(isNew ? packet.id : null);
}

function renderFeed(newPacketId = null) {
  updateCounts();

  let filtered = allPackets;

  // Filter category
  if (currentCategory !== 'all') {
    filtered = filtered.filter(p => p.parsed.category === currentCategory);
  }

  // Filter search
  if (searchQuery.trim().length > 0) {
    const q = searchQuery.toLowerCase();
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

  if (filtered.length === 0) {
    emptyState.style.display = 'flex';
    packetsFeed.innerHTML = '';
    packetsFeed.appendChild(emptyState);
    return;
  }

  emptyState.style.display = 'none';

  const html = filtered.map(packet => createPacketCardHtml(packet, packet.id === newPacketId)).join('');
  packetsFeed.innerHTML = html;

  // Attach click events to card buttons
  filtered.forEach(packet => {
    const viewBtn = document.getElementById(`btn-view-${packet.id}`);
    const copyBtn = document.getElementById(`btn-copy-${packet.id}`);
    if (viewBtn) {
      viewBtn.addEventListener('click', () => openPacketModal(packet));
    }
    if (copyBtn) {
      copyBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        navigator.clipboard.writeText(JSON.stringify(packet.raw, null, 2));
        showToast('JSON copiado al portapapeles', 'success');
      });
    }
  });
}

function createPacketCardHtml(packet, isNew) {
  const p = packet.parsed;
  const raw = packet.raw;
  const timeAgo = formatTimeAgo(packet.receivedAt);
  const exactTime = new Date(packet.receivedAt).toLocaleTimeString();

  let categoryBadgeClass = 'badge-message';
  let categoryLabel = 'Mensaje';

  if (p.category === 'status') {
    categoryBadgeClass = 'badge-status';
    categoryLabel = `Estado: ${p.status || 'actualizado'}`;
  } else if (p.category === 'system') {
    categoryBadgeClass = 'badge-system';
    categoryLabel = 'Sistema';
  }

  if (p.isSimulated) {
    categoryLabel += ' (Simulado)';
  }

  let senderDisplay = p.senderPhone || p.recipientPhone || 'Meta Cloud API';
  if (p.senderName) {
    senderDisplay += ` <span class="packet-sender-name">(${escapeHtml(p.senderName)})</span>`;
  }

  let statusBadgeHtml = '';
  if (p.status) {
    statusBadgeHtml = `<span class="status-indicator ${p.status}">✓ ${p.status.toUpperCase()}</span>`;
  }

  return `
    <div class="packet-card ${isNew ? 'is-new' : ''}" id="card-${packet.id}">
      <div class="packet-header">
        <div class="packet-meta-left">
          <span class="badge ${categoryBadgeClass}">${categoryLabel}</span>
          <span class="packet-sender">📱 ${senderDisplay}</span>
          ${statusBadgeHtml}
        </div>
        <div class="packet-time" title="${packet.receivedAt}">${timeAgo} (${exactTime})</div>
      </div>

      <div class="packet-content-box">
        <span class="packet-msg-text">${escapeHtml(p.messageText || '[Sin contenido de texto]')}</span>
        <button class="btn btn-sm btn-outline" id="btn-view-${packet.id}">
          🔍 Ver JSON
        </button>
      </div>

      <div class="packet-footer">
        <span class="packet-id-tag">ID: ${p.messageId || packet.id}</span>
        <div class="packet-actions">
          <button class="btn btn-sm btn-secondary" id="btn-copy-${packet.id}" title="Copiar JSON crudo">
            📋 Copiar JSON
          </button>
        </div>
      </div>
    </div>
  `;
}

function updateCounts() {
  countAll.textContent = allPackets.length;
  countMessages.textContent = allPackets.filter(p => p.parsed.category === 'message').length;
  countStatuses.textContent = allPackets.filter(p => p.parsed.category === 'status').length;
  countSystem.textContent = allPackets.filter(p => p.parsed.category === 'system' || p.parsed.category === 'other').length;
}

function updateStatsUI(stats) {
  if (!stats) return;
  statTotalPackets.textContent = stats.totalPackets || 0;
  statTotalMessages.textContent = stats.totalMessages || 0;
  statTotalStatuses.textContent = stats.totalStatuses || 0;

  if (stats.lastPingAt) {
    lastPingTime = stats.lastPingAt;
    const timeAgo = formatTimeAgo(stats.lastPingAt);
    const latency = stats.lastPingLatencyMs != null ? ` [${stats.lastPingLatencyMs}ms]` : '';
    const statusText = stats.lastPingStatus === 'success' ? '200 OK' : stats.lastPingStatus;
    statLastPing.textContent = `${statusText} (${timeAgo})${latency}`;
  }
}

// =========================================================
// KEEP ALIVE PING UI & LOGIC
// =========================================================
function updatePingUI() {
  if (inputPingUrl) {
    inputPingUrl.value = pingConfig.targetUrl || '';
  }
  if (chkPingEnabled) {
    chkPingEnabled.checked = pingConfig.enabled;
  }
  if (keepAliveText) {
    keepAliveText.textContent = pingConfig.enabled ? 'Ping Activo (1m)' : 'Ping Pausado';
  }
}

function addPingToHistory(ping) {
  const row = document.createElement('tr');
  const exactTime = new Date(ping.timestamp).toLocaleTimeString();
  const statusColor = ping.success ? '#4ade80' : '#f87171';
  const statusBadge = ping.success ? `✅ ${ping.status}` : `❌ ${ping.error || ping.status}`;

  row.innerHTML = `
    <td>${exactTime}</td>
    <td style="max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${escapeHtml(ping.url)}</td>
    <td style="color: ${statusColor}; font-weight: 600;">${statusBadge}</td>
    <td>${ping.latencyMs} ms</td>
  `;

  if (pingHistoryBody.querySelector('td[colspan]')) {
    pingHistoryBody.innerHTML = '';
  }
  pingHistoryBody.insertBefore(row, pingHistoryBody.firstChild);

  // Keep max 20 in UI
  while (pingHistoryBody.children.length > 20) {
    pingHistoryBody.removeChild(pingHistoryBody.lastChild);
  }
}

function renderPingHistory(history) {
  if (!history || history.length === 0) {
    pingHistoryBody.innerHTML = '<tr><td colspan="4" class="text-center">Sin registros de pings aún</td></tr>';
    return;
  }
  pingHistoryBody.innerHTML = '';
  history.forEach(ping => addPingToHistory(ping));
}

// =========================================================
// MODAL & INTERACTIVE JSON VIEWER
// =========================================================
function openPacketModal(packet) {
  currentActiveJson = packet.raw;
  const p = packet.parsed;

  modalCategoryBadge.textContent = p.category.toUpperCase();
  modalCategoryBadge.className = `badge badge-${p.category === 'message' ? 'message' : p.category === 'status' ? 'status' : 'system'}`;

  modalTitle.textContent = `Paquete: ${packet.id}`;
  modalSender.textContent = `${p.senderPhone || p.recipientPhone || 'N/A'} ${p.senderName ? '(' + p.senderName + ')' : ''}`;
  modalType.textContent = p.type || 'unknown';
  modalTimestamp.textContent = `${packet.receivedAt} (${formatTimeAgo(packet.receivedAt)})`;
  modalMessageId.textContent = p.messageId || 'N/A';

  modalJsonContent.innerHTML = syntaxHighlightJson(packet.raw);
  packetModal.classList.remove('hidden');
}

function closeModal() {
  packetModal.classList.add('hidden');
}

function syntaxHighlightJson(json) {
  if (typeof json !== 'string') {
    json = JSON.stringify(json, null, 2);
  }
  json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, (match) => {
    let cls = 'json-number';
    if (/^"/.test(match)) {
      if (/:$/.test(match)) {
        cls = 'json-key';
      } else {
        cls = 'json-string';
      }
    } else if (/true|false/.test(match)) {
      cls = 'json-boolean';
    } else if (/null/.test(match)) {
      cls = 'json-null';
    }
    return `<span class="${cls}">${match}</span>`;
  });
}

// =========================================================
// EVENT LISTENERS & ACTIONS
// =========================================================
function setupEventListeners() {
  // Category tabs
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentCategory = btn.dataset.category;
      renderFeed();
    });
  });

  // Search input
  searchInput.addEventListener('input', (e) => {
    searchQuery = e.target.value;
    renderFeed();
  });

  // Simulator Dropdown
  btnSimulateDropdown.addEventListener('click', (e) => {
    e.stopPropagation();
    document.querySelector('.dropdown').classList.toggle('open');
  });

  document.addEventListener('click', () => {
    document.querySelector('.dropdown').classList.remove('open');
  });

  simulateMenu.querySelectorAll('button').forEach(btn => {
    btn.addEventListener('click', async (e) => {
      const scenario = btn.dataset.sim;
      try {
        const res = await fetch('/api/simulate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ scenario })
        });
        const data = await res.json();
        if (data.success) {
          showToast(`Evento simulado inyectado: ${scenario}`, 'success');
        }
      } catch (err) {
        showToast('Error inyectando simulación', 'error');
      }
    });
  });

  // Copy Webhook URL
  btnCopyWebhookUrl.addEventListener('click', () => {
    const url = `${window.location.origin}/webhook`;
    navigator.clipboard.writeText(url);
    showToast(`URL Webhook copiada: ${url}`, 'success');
  });

  // Copy Token
  btnCopyToken.addEventListener('click', () => {
    navigator.clipboard.writeText(verifyToken);
    showToast(`Verify Token copiado: ${verifyToken}`, 'success');
  });

  // Export JSON
  btnExport.addEventListener('click', () => {
    window.location.href = '/api/export';
  });

  // Clear Packets
  btnClear.addEventListener('click', async () => {
    if (confirm('¿Estás seguro de que deseas vaciar todos los paquetes recibidos?')) {
      await fetch('/api/packets', { method: 'DELETE' });
    }
  });

  // Modal close handlers
  btnCloseModal.addEventListener('click', closeModal);
  packetModal.addEventListener('click', (e) => {
    if (e.target === packetModal) closeModal();
  });

  btnCopyModalJson.addEventListener('click', () => {
    if (currentActiveJson) {
      navigator.clipboard.writeText(JSON.stringify(currentActiveJson, null, 2));
      showToast('JSON copiado al portapapeles', 'success');
    }
  });

  // Keep Alive Modal handlers
  btnKeepAliveStatus.addEventListener('click', () => {
    keepAliveModal.classList.remove('hidden');
  });

  btnCloseKeepAlive.addEventListener('click', () => {
    keepAliveModal.classList.add('hidden');
  });

  keepAliveModal.addEventListener('click', (e) => {
    if (e.target === keepAliveModal) keepAliveModal.classList.add('hidden');
  });

  btnSavePingUrl.addEventListener('click', async () => {
    const url = inputPingUrl.value.trim();
    const enabled = chkPingEnabled.checked;
    try {
      const res = await fetch('/api/ping/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url, enabled })
      });
      const data = await res.json();
      if (data.success) {
        showToast('Configuración de ping guardada', 'success');
      }
    } catch (err) {
      showToast('Error guardando configuración', 'error');
    }
  });

  chkPingEnabled.addEventListener('change', async () => {
    const enabled = chkPingEnabled.checked;
    await fetch('/api/ping/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled })
    });
  });

  btnTriggerPingNow.addEventListener('click', async () => {
    showToast('Enviando ping manual...', 'info');
    try {
      const res = await fetch('/api/ping/trigger', { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        showToast(`Ping completado (${data.lastPingLatencyMs}ms)`, 'success');
      }
    } catch (err) {
      showToast('Error ejecutando ping', 'error');
    }
  });

  // ESC key to close modals
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      closeModal();
      keepAliveModal.classList.add('hidden');
    }
  });
}

// =========================================================
// UTILITIES
// =========================================================
function formatTimeAgo(isoString) {
  if (!isoString) return 'hace un momento';
  const seconds = Math.floor((new Date() - new Date(isoString)) / 1000);
  if (seconds < 5) return 'justo ahora';
  if (seconds < 60) return `hace ${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `hace ${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `hace ${hours}h`;
  return new Date(isoString).toLocaleDateString();
}

function updateRelativeTimestamps() {
  if (lastPingTime) {
    const timeAgo = formatTimeAgo(lastPingTime);
    const currentText = statLastPing.textContent;
    if (currentText.includes('(')) {
      statLastPing.textContent = currentText.replace(/\(hace [^)]+\)/, `(${timeAgo})`);
    }
  }
}

function escapeHtml(str) {
  if (typeof str !== 'string') return '';
  return str.replace(/[&<>'"]/g, 
    tag => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      "'": '&#39;',
      '"': '&quot;'
    }[tag] || tag)
  );
}

function showToast(message, type = 'info') {
  const container = document.getElementById('toastContainer');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span>${message}</span>`;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateX(100%)';
    toast.style.transition = 'all 0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}

// Run on load
document.addEventListener('DOMContentLoaded', initApp);
