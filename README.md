# SSE (Server-Sent Events) リアルタイム通信サンプル

本リポジトリは、**Server-Sent Events (SSE)** を活用したリアルタイム単方向ストリーミング通信の完全なサンプルアプリケーションです。
ブラウザ標準の `EventSource` API と Node.js (Express) を使用し、リアルタイムメトリクスの受信、カスタムイベントハンドリング、複数クライアント間でのブロードキャスト通信、生パケットの可視化を体験できます。

---

## 🌟 主な機能と特徴

1. **リアルタイム・サーバーメトリクス配信**
   - サーバーの負荷（CPU使用率、メモリ、アクティブ接続数など）を1秒ごとにストリーミング配信。
   - Vanilla JS & Canvas によるスムーズなリアルタイムグラフラベル描画。
2. **カスタムイベント名（`event: <name>`）の活用**
   - `connected`: 初回接続・再接続検知
   - `metrics`: テレメトリ数値データ
   - `news`: ニューストピック配信
   - `chat`: 接続中クライアント同士のメッセージブロードキャスト
   - `alert`: 緊急警告・システム通知
   - `system`: クライアントの参加/離脱通知
3. **SSE プロトコル Raw インスペクター**
   - HTTPレスポンスストリーム上に流れる未加工の `id: ...\nevent: ...\ndata: ...\n\n` をリアルタイムで可視化。
4. **自動再接続 & Last-Event-ID**
   - 切断時にブラウザ標準機能で自動再試行（`retry: 3000`）。
   - 再接続時に `Last-Event-ID` ヘッダーがサーバーへ送信される仕組みを確認可能。
5. **洗練されたモダンUI**
   - ダークテーマ、グラスモフィズム、アニメーションインジケーターを搭載。

---

## 🚀 クイックスタート

### 1. 依存パッケージのインストール
```bash
npm install
```

### 2. サーバーの起動
```bash
npm start
```
起動後、ブラウザで [http://localhost:3000](http://localhost:3000) を開いてください。

### 3. 複数クライアントでの検証
ブラウザの別タブや別ウィンドウで [http://localhost:3000](http://localhost:3000) を複数開くと、同時接続クライアント数の増減や、片方から送信したメッセージが即座に全画面へ配信される様子を確認できます。

---

## 📖 SSE (Server-Sent Events) とは？

サーバーからクライアント（ブラウザ）へ向けて、持続的な1本のHTTP接続を通じてテキストデータをリアルタイムにプッシュ配信する技術規格です。

### WebSocket との違い・比較

| 項目 | Server-Sent Events (SSE) | WebSocket |
| :--- | :--- | :--- |
| **通信方向** | **単方向** (サーバー → クライアント) | **双方向** (サーバー ⇄ クライアント) |
| **プロトコル** | 通常の **HTTP / HTTPS** | 独自プロトコル (`ws://`, `wss://`) |
| **HTTP/2対応** | **完全対応** (同一TCP接続で多重化可能) | プロトコルアップグレードが必要 |
| **自動再接続** | **ブラウザ標準機能**で内包 (`retry`) | 自前で再接続ロジックの実装が必要 |
| **イベントID/復元** | **標準仕様** (`Last-Event-ID`) | 自前でシーケンス番号等を管理 |
| **ファイアウォール** | 通常のHTTPポート(80/443)を通るため容易 | プロキシやロードバランサで遮断される場合あり |
| **適したユースケース** | 株価・為替レート、ダッシュボード、AIチャットストリーミング(ChatGPT等)、プッシュ通知 | オンラインゲーム、コラボ描画、ビデオ通話のシグナリング |

---

## 💻 実装コード解説

### 1. サーバー側 (Express)
SSE を確立するためには、以下の3つのHTTPレスポンスヘッダーが必須です。

```javascript
app.get('/api/sse', (req, res) => {
  // 1. 必須レスポンスヘッダーの設定
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no', // Nginxのリバースプロキシバッファリングを無効化
  });

  // 2. メッセージの送信フォーマット (終端に \n\n が必須)
  res.write(`id: 1\n`);
  res.write(`event: news\n`);
  res.write(`data: {"message": "Hello World"}\n\n`);

  // 3. クライアント切断の検知
  req.on('close', () => {
    console.log('クライアントが切断しました');
  });
});
```

### 2. クライアント側 (JavaScript)
ブラウザ標準の `EventSource` API を利用するだけで、追加の外部ライブラリなしで通信できます。

```javascript
// 接続の作成
const evtSource = new EventSource('/api/sse');

// 通常メッセージの受信
evtSource.onmessage = (event) => {
  console.log('通常データ:', event.data);
};

// カスタムイベント (event: alert) の受信
evtSource.addEventListener('alert', (event) => {
  const alertData = JSON.parse(event.data);
  console.warn('アラート受信:', alertData);
});

// 接続エラーや切断（ブラウザが自動的に再接続を試みます）
evtSource.onerror = (err) => {
  console.error('SSEエラー:', err);
};

// 接続を明示的に終了する場合
// evtSource.close();
```

---

## 🛡️ 本番環境運用のTips

1. **HTTP/2 の利用を推奨**:
   - HTTP/1.1 ではブラウザのドメインあたり最大接続数制限（約6本）に達しやすくなりますが、HTTP/2 では1本のTCPコネクション上で多重化されるため、多数のタブを開いても制限に引っかかりません。
2. **Nginx などのリバースプロキシ設定**:
   - プロキシがレスポンスをバッファリングすると、リアルタイムにクライアントへ届かなくなります。
   - `proxy_buffering off;` や `X-Accel-Buffering: no;` を指定してください。
3. **キープアライブ（ハートビート）**:
   - 一部のロードバランサ（AWS ALBなど）はアイドル状態が続くとタイムアウト切断するため、15〜30秒間隔で `: keepalive\n\n` 等のコメント行を送信することが推奨されます。
