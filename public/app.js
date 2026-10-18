/**
 * SSE Realtime Hub - Client Application
 * Server-Sent Events (EventSource API) の実装とリアルタイムビジュアライザ
 */

let eventSource = null;
let clientId = null;
let lastEventId = null;
let totalEventsCount = 0;
let connectionStartTime = null;
let uptimeInterval = null;

// チャート用データ（CPU履歴）
const chartHistory = [];
const MAX_CHART_POINTS = 30;

// DOM要素の参照キャッシュ
const elements = {
  statusDot: document.getElementById('status-dot'),
  statusLabel: document.getElementById('status-label'),
  statusCode: document.getElementById('status-code'),
  btnToggleConnection: document.getElementById('btn-toggle-connection'),
  btnToggleText: document.getElementById('btn-toggle-text'),
  btnClearLogs: document.getElementById('btn-clear-logs'),

  clientIdDisplay: document.getElementById('client-id-display'),
  lastEventIdDisplay: document.getElementById('last-event-id-display'),
  totalEventsCount: document.getElementById('total-events-count'),
  uptimeDisplay: document.getElementById('uptime-display'),

  cpuValue: document.getElementById('cpu-value'),
  cpuBar: document.getElementById('cpu-bar'),
  memValue: document.getElementById('mem-value'),
  clientsCount: document.getElementById('clients-count'),
  reqValue: document.getElementById('req-value'),

  canvasChart: document.getElementById('realtime-chart'),

  broadcastForm: document.getElementById('broadcast-form'),
  senderName: document.getElementById('sender-name'),
  messageLevel: document.getElementById('message-level'),
  broadcastMessage: document.getElementById('broadcast-message'),

  btnTriggerWarning: document.getElementById('btn-trigger-warning'),
  btnTriggerDanger: document.getElementById('btn-trigger-danger'),
  btnTriggerInfo: document.getElementById('btn-trigger-info'),

  tabBtnVisual: document.getElementById('tab-btn-visual'),
  tabBtnRaw: document.getElementById('tab-btn-raw'),
  tabVisualContent: document.getElementById('tab-visual-content'),
  tabRawContent: document.getElementById('tab-raw-content'),
  filterControls: document.getElementById('filter-controls'),
  eventFilter: document.getElementById('event-filter'),
  eventsFeedList: document.getElementById('events-feed-list'),
  feedCount: document.getElementById('feed-count'),
  rawLogs: document.getElementById('raw-logs'),
};

// ---------------------------------------------------------------------
// 1. SSE 接続の初期化とイベントリスナーの設定
// ---------------------------------------------------------------------
function connectSSE() {
  if (eventSource) {
    eventSource.close();
  }

  updateConnectionStatus('connecting');

  // EventSource インスタンスの生成（ブラウザ標準API）
  eventSource = new EventSource('/api/sse');

  // [ライフサイクル 1] 接続確立
  eventSource.onopen = (e) => {
    console.log('[SSE] 接続オープン:', e);
    updateConnectionStatus('open');
    startUptimeTimer();
  };

  // [ライフサイクル 2] エラー・切断（自動再接続が行われます）
  eventSource.onerror = (err) => {
    console.warn('[SSE] エラー / 再接続試行中:', err);
    if (eventSource.readyState === EventSource.CONNECTING) {
      updateConnectionStatus('connecting');
    } else if (eventSource.readyState === EventSource.CLOSED) {
      updateConnectionStatus('closed');
      stopUptimeTimer();
    }
  };

  // [カスタムイベント 1] 'connected' - 初回ウェルカムメッセージ
  eventSource.addEventListener('connected', (e) => {
    const data = JSON.parse(e.data);
    clientId = data.clientId;
    elements.clientIdDisplay.textContent = clientId;
    if (data.activeClients !== undefined) {
      elements.clientsCount.textContent = data.activeClients;
    }

    recordEvent('connected', e.lastEventId, data, 'サーバーへの接続が確立されました');
  });

  // [カスタムイベント 2] 'metrics' - サーバーの負荷テレメトリ
  eventSource.addEventListener('metrics', (e) => {
    const data = JSON.parse(e.data);
    updateMetricsUI(data);
    recordEvent('metrics', e.lastEventId, data, null, false);
  });

  // [カスタムイベント 3] 'chat' - クライアント同士のブロードキャスト
  eventSource.addEventListener('chat', (e) => {
    const data = JSON.parse(e.data);
    recordEvent('chat', e.lastEventId, data, `${data.sender}: ${data.message}`);
  });

  // [カスタムイベント 4] 'alert' - 緊急・システムアラート
  eventSource.addEventListener('alert', (e) => {
    const data = JSON.parse(e.data);
    recordEvent('alert', e.lastEventId, data, `【${data.title}】 ${data.message}`);
  });

  // [カスタムイベント 5] 'news' - 定期トピック配信
  eventSource.addEventListener('news', (e) => {
    const data = JSON.parse(e.data);
    recordEvent('news', e.lastEventId, data, data.headline);
  });

  // [カスタムイベント 6] 'system' - クライアント参加・離脱
  eventSource.addEventListener('system', (e) => {
    const data = JSON.parse(e.data);
    if (data.activeClients !== undefined) {
      elements.clientsCount.textContent = data.activeClients;
    }
    const msg = data.type === 'client_joined'
      ? `クライアントが参加しました (${data.clientId})`
      : `クライアントが退出しました (${data.clientId})`;
    recordEvent('system', e.lastEventId, data, msg);
  });

  // [標準メッセージ] 'message' - イベント名未指定のメッセージ
  eventSource.onmessage = (e) => {
    recordEvent('message', e.lastEventId, e.data, e.data);
  };
}

// 接続の切断
function disconnectSSE() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
  updateConnectionStatus('closed');
  stopUptimeTimer();
}

// 接続ステータスUIの更新
function updateConnectionStatus(state) {
  elements.statusDot.className = `pulse-dot ${state}`;
  if (state === 'open') {
    elements.statusLabel.textContent = '接続中 (STREAMING)';
    elements.statusCode.textContent = '(readyState: 1)';
    elements.btnToggleText.textContent = '切断する';
    elements.btnToggleConnection.classList.remove('btn-accent');
    elements.btnToggleConnection.classList.add('btn-primary');
  } else if (state === 'connecting') {
    elements.statusLabel.textContent = '再接続待機中...';
    elements.statusCode.textContent = '(readyState: 0)';
    elements.btnToggleText.textContent = '切断する';
  } else {
    elements.statusLabel.textContent = '切断状態';
    elements.statusCode.textContent = '(readyState: 2)';
    elements.btnToggleText.textContent = '再接続する';
    elements.btnToggleConnection.classList.remove('btn-primary');
    elements.btnToggleConnection.classList.add('btn-accent');
  }
}

// ---------------------------------------------------------------------
// 2. メトリクスUI & Canvasリアルタイムチャートの更新
// ---------------------------------------------------------------------
function updateMetricsUI(data) {
  // 数値の反映
  elements.cpuValue.textContent = data.cpuUsage.toFixed(1);
  elements.cpuBar.style.width = `${Math.min(100, data.cpuUsage)}%`;
  elements.memValue.textContent = data.memoryMb.toFixed(1);
  elements.reqValue.textContent = data.requestsPerSec;
  if (data.activeClients !== undefined) {
    elements.clientsCount.textContent = data.activeClients;
  }

  // チャート履歴に追加
  chartHistory.push(data.cpuUsage);
  if (chartHistory.length > MAX_CHART_POINTS) {
    chartHistory.shift();
  }

  drawRealtimeChart();
}

function drawRealtimeChart() {
  const canvas = elements.canvasChart;
  if (!canvas) return;

  // 高DPIディスプレイ対応
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const width = rect.width;
  const height = rect.height;

  if (canvas.width !== width * dpr || canvas.height !== height * dpr) {
    canvas.width = width * dpr;
    canvas.height = height * dpr;
  }

  const ctx = canvas.getContext('2d');
  ctx.save();
  ctx.scale(dpr, dpr);

  ctx.clearRect(0, 0, width, height);

  // 背景グリッド線
  ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
  ctx.lineWidth = 1;
  for (let y = 0; y <= height; y += height / 4) {
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(width, y);
    ctx.stroke();
  }

  if (chartHistory.length < 2) {
    ctx.restore();
    return;
  }

  // 折れ線グラフの座標計算
  const stepX = width / (MAX_CHART_POINTS - 1);
  const startOffsetIndex = MAX_CHART_POINTS - chartHistory.length;

  const points = chartHistory.map((val, idx) => {
    const x = (startOffsetIndex + idx) * stepX;
    // 0〜100%をY軸にマップ (上側が100%, 下側が0%)
    const y = height - (val / 100) * (height - 20) - 10;
    return { x, y, val };
  });

  // 1. 塗りつぶしグラデーション
  const grad = ctx.createLinearGradient(0, 0, 0, height);
  grad.addColorStop(0, 'rgba(56, 189, 248, 0.35)');
  grad.addColorStop(1, 'rgba(56, 189, 248, 0.0)');

  ctx.beginPath();
  ctx.moveTo(points[0].x, height);
  ctx.lineTo(points[0].x, points[0].y);
  for (let i = 1; i < points.length; i++) {
    // スムーズな曲線
    const prev = points[i - 1];
    const curr = points[i];
    const cx = (prev.x + curr.x) / 2;
    ctx.quadraticCurveTo(prev.x, prev.y, cx, (prev.y + curr.y) / 2);
  }
  ctx.lineTo(points[points.length - 1].x, points[points.length - 1].y);
  ctx.lineTo(points[points.length - 1].x, height);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();

  // 2. メインライン
  ctx.beginPath();
  ctx.moveTo(points[0].x, points[0].y);
  for (let i = 1; i < points.length; i++) {
    const prev = points[i - 1];
    const curr = points[i];
    const cx = (prev.x + curr.x) / 2;
    ctx.quadraticCurveTo(prev.x, prev.y, cx, (prev.y + curr.y) / 2);
  }
  ctx.lineTo(points[points.length - 1].x, points[points.length - 1].y);
  ctx.strokeStyle = '#38bdf8';
  ctx.lineWidth = 2.5;
  ctx.stroke();

  // 3. 最新ポイントの強調ドット
  const lastPoint = points[points.length - 1];
  ctx.beginPath();
  ctx.arc(lastPoint.x, lastPoint.y, 4, 0, Math.PI * 2);
  ctx.fillStyle = '#fff';
  ctx.shadowColor = '#38bdf8';
  ctx.shadowBlur = 10;
  ctx.fill();

  ctx.restore();
}

// ---------------------------------------------------------------------
// 3. イベント受信ログ & Rawインスペクターの更新
// ---------------------------------------------------------------------
function recordEvent(eventType, eventId, payload, summaryText, showInVisualFeed = true) {
  totalEventsCount++;
  elements.totalEventsCount.textContent = totalEventsCount;

  if (eventId) {
    lastEventId = eventId;
    elements.lastEventIdDisplay.textContent = eventId;
  }

  const timeStr = new Date().toLocaleTimeString();

  // 1. Raw SSE パケットの生成と蓄積（教育用インスペクター）
  appendRawSseLog({
    id: eventId,
    event: eventType,
    data: payload,
  });

  // 2. ビジュアルフィードへの追加
  if (showInVisualFeed) {
    appendVisualEventCard({
      eventType,
      eventId,
      payload,
      summaryText,
      timeStr,
    });
  }
}

function appendRawSseLog({ id, event, data }) {
  let rawText = '';
  if (id) rawText += `id: ${id}\n`;
  if (event) rawText += `event: ${event}\n`;
  const dataStr = typeof data === 'object' ? JSON.stringify(data) : String(data);
  const lines = dataStr.split(/\r?\n/);
  for (const line of lines) {
    rawText += `data: ${line}\n`;
  }
  rawText += '\n';

  const logElem = elements.rawLogs;
  // 空文字初期状態なら上書き
  if (logElem.textContent.includes('// 接続待機中...')) {
    logElem.textContent = '';
  }

  // 最新を上部に表示
  logElem.textContent = rawText + logElem.textContent;

  // 最大文字数制限
  if (logElem.textContent.length > 30000) {
    logElem.textContent = logElem.textContent.substring(0, 25000) + '\n...[古いログは切り詰められました]';
  }
}

function appendVisualEventCard({ eventType, eventId, payload, summaryText, timeStr }) {
  const feedList = elements.eventsFeedList;
  const emptyState = feedList.querySelector('.feed-empty-state');
  if (emptyState) {
    emptyState.remove();
  }

  const card = document.createElement('div');
  card.className = `event-card event-${eventType}`;
  card.dataset.eventType = eventType;

  let bodyContent = '';
  if (eventType === 'chat') {
    bodyContent = `
      <span class="chat-sender">${escapeHtml(payload.sender)}:</span>
      <span class="chat-text">${escapeHtml(payload.message)}</span>
    `;
  } else if (eventType === 'alert') {
    bodyContent = `
      <strong>${escapeHtml(payload.title)}</strong>: ${escapeHtml(payload.message)}
    `;
  } else {
    bodyContent = escapeHtml(summaryText || JSON.stringify(payload));
  }

  card.innerHTML = `
    <div class="event-card-header">
      <div class="event-badge-group">
        <span class="event-type-badge">${eventType}</span>
        ${eventId ? `<span class="event-id-badge">#${eventId}</span>` : ''}
      </div>
      <span class="event-timestamp">${timeStr}</span>
    </div>
    <div class="event-card-body">${bodyContent}</div>
  `;

  // フィルター判定
  const currentFilter = elements.eventFilter.value;
  applyFilterToCard(card, currentFilter);

  // 最上部に追加
  feedList.insertBefore(card, feedList.firstChild);

  // フィード件数の更新
  const totalCards = feedList.querySelectorAll('.event-card').length;
  elements.feedCount.textContent = totalCards;

  // 上限100件で古いものを削除
  if (totalCards > 100) {
    feedList.lastElementChild.remove();
  }
}

function applyFilterToCard(card, filter) {
  const type = card.dataset.eventType;
  if (filter === 'all') {
    card.style.display = '';
  } else if (filter === 'no-metrics') {
    card.style.display = type === 'metrics' ? 'none' : '';
  } else {
    card.style.display = type === filter ? '' : 'none';
  }
}

function escapeHtml(str) {
  if (str === undefined || str === null) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

// ---------------------------------------------------------------------
// 4. タイマー管理 (接続経過時間)
// ---------------------------------------------------------------------
function startUptimeTimer() {
  connectionStartTime = Date.now();
  if (uptimeInterval) clearInterval(uptimeInterval);
  uptimeInterval = setInterval(() => {
    if (!connectionStartTime) return;
    const diffSec = Math.floor((Date.now() - connectionStartTime) / 1000);
    const hrs = String(Math.floor(diffSec / 3600)).padStart(2, '0');
    const mins = String(Math.floor((diffSec % 3600) / 60)).padStart(2, '0');
    const secs = String(diffSec % 60).padStart(2, '0');
    elements.uptimeDisplay.textContent = `${hrs}:${mins}:${secs}`;
  }, 1000);
}

function stopUptimeTimer() {
  if (uptimeInterval) {
    clearInterval(uptimeInterval);
    uptimeInterval = null;
  }
}

// ---------------------------------------------------------------------
// 5. イベントリスナーとユーザーインタラクション
// ---------------------------------------------------------------------
// 接続 / 切断 切り替えボタン
elements.btnToggleConnection.addEventListener('click', () => {
  if (eventSource) {
    disconnectSSE();
  } else {
    connectSSE();
  }
});

// ログクリア
elements.btnClearLogs.addEventListener('click', () => {
  elements.eventsFeedList.innerHTML = `
    <div class="feed-empty-state">
      <div class="empty-icon">📡</div>
      <p>ログがクリアされました。新規イベントを待機中...</p>
    </div>
  `;
  elements.rawLogs.textContent = '// ログがクリアされました\n';
  elements.feedCount.textContent = '0';
});

// タブ切り替え（ビジュアルフィード vs Raw SSE）
elements.tabBtnVisual.addEventListener('click', () => {
  elements.tabBtnVisual.classList.add('active');
  elements.tabBtnRaw.classList.remove('active');
  elements.tabVisualContent.classList.add('active');
  elements.tabRawContent.classList.remove('active');
  elements.filterControls.style.display = 'flex';
});

elements.tabBtnRaw.addEventListener('click', () => {
  elements.tabBtnRaw.classList.add('active');
  elements.tabBtnVisual.classList.remove('active');
  elements.tabRawContent.classList.add('active');
  elements.tabVisualContent.classList.remove('active');
  elements.filterControls.style.display = 'none';
});

// フィルター選択
elements.eventFilter.addEventListener('change', (e) => {
  const filter = e.target.value;
  const cards = elements.eventsFeedList.querySelectorAll('.event-card');
  cards.forEach((card) => applyFilterToCard(card, filter));
});

// ブロードキャストメッセージ送信フォーム
elements.broadcastForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const message = elements.broadcastMessage.value.trim();
  const sender = elements.senderName.value.trim();
  const level = elements.messageLevel.value;

  if (!message) return;

  try {
    elements.broadcastMessage.disabled = true;
    const res = await fetch('/api/broadcast', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sender, message, level }),
    });
    if (res.ok) {
      elements.broadcastMessage.value = '';
    } else {
      alert('メッセージ送信に失敗しました');
    }
  } catch (err) {
    console.error('送信エラー:', err);
    alert('通信エラーが発生しました');
  } finally {
    elements.broadcastMessage.disabled = false;
    elements.broadcastMessage.focus();
  }
});

// アラートトリガーボタン群
async function triggerAlert(type, title, message) {
  try {
    await fetch('/api/alert', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, title, message }),
    });
  } catch (err) {
    console.error('アラート発火エラー:', err);
  }
}

elements.btnTriggerWarning.addEventListener('click', () => {
  triggerAlert('warning', 'トラフィック急増警告', '毎秒アクセス数がしきい値を超過しました。オートスケールが起動します。');
});

elements.btnTriggerDanger.addEventListener('click', () => {
  triggerAlert('danger', '緊急システムメンテナンス', '深夜2:00〜4:00にデータベース移行に伴うサービス停止を予定しています。');
});

elements.btnTriggerInfo.addEventListener('click', () => {
  triggerAlert('info', 'システムバージョンアップ完了', '新エンジン v2.4.0 への切り替えが正常に完了しました。');
});

// ウィンドウリサイズ時のグラフ再描画
window.addEventListener('resize', () => {
  drawRealtimeChart();
});

// 初回起動
connectSSE();
