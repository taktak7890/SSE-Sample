const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// 接続中クライアントの管理
// key: clientId, value: { id, res, connectedAt, ip, lastEventId }
const clients = new Map();
let globalEventId = 0;

/**
 * SSE形式のメッセージを文字列化するヘルパー関数
 * @param {Object} options
 * @param {string|number} [options.id] イベントID
 * @param {string} [options.event] イベント種別（カスタムイベント名）
 * @param {any} options.data 送信するデータ（オブジェクトまたは文字列）
 * @param {number} [options.retry] 再接続までの待機ミリ秒数
 * @returns {string} SSEフォーマットの文字列
 */
function formatSseMessage({ id, event, data, retry }) {
  let message = '';

  if (retry !== undefined) {
    message += `retry: ${retry}\n`;
  }
  if (id !== undefined) {
    message += `id: ${id}\n`;
  }
  if (event) {
    message += `event: ${event}\n`;
  }

  // dataがオブジェクトの場合はJSON文字列化、改行を含む場合は各行に "data: " を付与
  const dataString = typeof data === 'object' ? JSON.stringify(data) : String(data);
  const lines = dataString.split(/\r?\n/);
  for (const line of lines) {
    message += `data: ${line}\n`;
  }

  message += '\n'; // メッセージの終端は空行（\n\n）
  return message;
}

/**
 * 接続中の全クライアントにメッセージをブロードキャストする
 * @param {Object} ssePayload
 */
function broadcast(ssePayload) {
  globalEventId++;
  const payloadWithId = { id: globalEventId, ...ssePayload };
  const rawMessage = formatSseMessage(payloadWithId);

  for (const [clientId, client] of clients.entries()) {
    try {
      client.res.write(rawMessage);
    } catch (err) {
      console.error(`クライアント [${clientId}] への送信エラー:`, err.message);
      clients.delete(clientId);
    }
  }
}

// ----------------------------------------------------
// SSE エンドポイント: /api/sse
// ----------------------------------------------------
app.get('/api/sse', (req, res) => {
  // 1. SSEに必要なHTTPレスポンスヘッダーの設定
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no', // Nginxのリバースプロキシバッファリングを無効化
  });

  // クライアントIDの発行
  const clientId = `client_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
  const lastEventId = req.headers['last-event-id'] || null;

  console.log(`[SSE 接続] クライアントID: ${clientId} (Last-Event-ID: ${lastEventId || 'なし'})`);

  // クライアント管理に追加
  clients.set(clientId, {
    id: clientId,
    res,
    connectedAt: new Date().toISOString(),
    ip: req.ip || req.socket.remoteAddress,
  });

  // 2. 初回接続メッセージをクライアントへ送信
  globalEventId++;
  const welcomePayload = {
    id: globalEventId,
    event: 'connected',
    retry: 3000, // 切断時の自動再接続インターバルを3秒に指定
    data: {
      clientId,
      message: 'SSEストリームへの接続に成功しました。',
      serverTime: new Date().toISOString(),
      activeClients: clients.size,
      reconnected: !!lastEventId,
      lastEventIdReceived: lastEventId,
    },
  };
  res.write(formatSseMessage(welcomePayload));

  // 他のクライアントにも接続数変動を通知
  broadcast({
    event: 'system',
    data: {
      type: 'client_joined',
      clientId,
      activeClients: clients.size,
      timestamp: new Date().toISOString(),
    },
  });

  // 3. 切断時のクリーンアップ処理
  req.on('close', () => {
    console.log(`[SSE 切断] クライアントID: ${clientId}`);
    clients.delete(clientId);

    // 他のクライアントへ切断を通知
    broadcast({
      event: 'system',
      data: {
        type: 'client_left',
        clientId,
        activeClients: clients.size,
        timestamp: new Date().toISOString(),
      },
    });
  });
});

// ----------------------------------------------------
// バックグラウンドでの定期データ配信（サーバーメトリクス & ハートビート）
// ----------------------------------------------------
// 1秒ごとのメトリクス配信 (CPU、メモリ、リアルタイムカウンター)
let tick = 0;
setInterval(() => {
  if (clients.size === 0) return;

  tick++;
  // 擬似メトリクスの生成
  const baseCpu = 25 + Math.sin(tick * 0.2) * 15;
  const cpuVariation = (Math.random() - 0.5) * 8;
  const cpuUsage = Math.max(5, Math.min(95, Math.round((baseCpu + cpuVariation) * 10) / 10));

  const memUsage = Math.round((140 + Math.cos(tick * 0.1) * 20 + Math.random() * 5) * 10) / 10;
  const requestsPerSec = Math.floor(40 + Math.random() * 30 + clients.size * 5);

  broadcast({
    event: 'metrics',
    data: {
      tick,
      cpuUsage,
      memoryMb: memUsage,
      requestsPerSec,
      activeClients: clients.size,
      timestamp: new Date().toLocaleTimeString(),
    },
  });
}, 1000);

// 10秒ごとの疑似ニューストピック配信
const sampleTopics = [
  'クラウドインフラの自動スケーリングが完了しました',
  '新バージョン v2.4.0 のデプロイ準備が整いました',
  'DBレプリケーションの整合性チェック: 正常 (遅延 0.4ms)',
  'リアルタイムアナリティクスの集計バッチが完了しました',
  'CDNキャッシュヒット率: 98.4% を記録',
  'セキュリティ監査スキャン: 脆弱性は検出されませんでした',
];
let topicIndex = 0;
setInterval(() => {
  if (clients.size === 0) return;

  const topic = sampleTopics[topicIndex % sampleTopics.length];
  topicIndex++;

  broadcast({
    event: 'news',
    data: {
      category: 'INFO',
      headline: topic,
      timestamp: new Date().toLocaleTimeString(),
    },
  });
}, 8000);

// 15秒ごとのハートビートコメント送信（通信路のキープアライブ用）
setInterval(() => {
  if (clients.size === 0) return;
  for (const client of clients.values()) {
    try {
      // コメント行は先頭にコロン `:` を置く。クライアントのEventSourceはこれを無視するが接続維持に有効
      client.res.write(`: keepalive ${Date.now()}\n\n`);
    } catch {
      // 無視
    }
  }
}, 15000);

// ----------------------------------------------------
// REST API エンドポイント（クライアント側からのトリガー用）
// ----------------------------------------------------

// 全クライアントへのブロードキャストメッセージ送信
app.post('/api/broadcast', (req, res) => {
  const { sender, message, level = 'info' } = req.body;
  if (!message || message.trim() === '') {
    return res.status(400).json({ error: 'メッセージは必須です' });
  }

  broadcast({
    event: 'chat',
    data: {
      sender: sender?.trim() || '匿名ユーザー',
      message: message.trim(),
      level,
      timestamp: new Date().toLocaleTimeString(),
    },
  });

  res.json({ success: true, clientsReached: clients.size });
});

// 手動アラートのトリガー
app.post('/api/alert', (req, res) => {
  const { type = 'warning', title, message } = req.body;

  broadcast({
    event: 'alert',
    data: {
      type,
      title: title || 'システム警告',
      message: message || '管理者が手動でアラートイベントを発生させました',
      timestamp: new Date().toLocaleTimeString(),
    },
  });

  res.json({ success: true, clientsReached: clients.size });
});

// サーバー情報取得
app.get('/api/status', (req, res) => {
  res.json({
    activeClients: clients.size,
    uptimeSeconds: Math.floor(process.uptime()),
    serverTime: new Date().toISOString(),
    globalEventId,
  });
});

app.listen(PORT, () => {
  console.log(`=========================================`);
  console.log(`🚀 SSE リアルタイム通信サーバーが起動しました`);
  console.log(`📍 URL: http://localhost:${PORT}`);
  console.log(`📡 SSE エンドポイント: http://localhost:${PORT}/api/sse`);
  console.log(`=========================================`);
});
