# Google Sheets Reader - Secure CLI Tool

**セキュアなGoogle Sheets読み取りツール - 認証情報流出対策済み**

## 🔒 セキュリティ特徴

- **認証情報コミット禁止**: JSON キー、トークン、URL、ID をコード・GitHub に一切含めない
- **環境変数ベース認証**: サービスアカウント認証を環境変数で安全に管理
- **ログセキュリティ**: 機密データを自動的にマスクしてログ出力
- **最小権限**: 読み取り専用スコープのみ使用
- **最小範囲**: 必要な列・行・期間のみ取得

## 📋 前提条件

1. **Google Cloud Platform アカウント**
2. **サービスアカウント** (読み取り専用権限)
3. **Python 3.8+**
4. **対象スプレッドシート** へのサービスアカウント共有設定

## 🚀 セットアップ手順

### 1. リポジトリセットアップ

```bash
# 依存関係インストール
make install

# 環境変数ファイル作成
make setup-env
```

### 2. Google Cloud 設定

#### サービスアカウント作成
```bash
# GCP プロジェクトでサービスアカウント作成
gcloud iam service-accounts create sheets-reader \
    --display-name="Sheets Reader" \
    --description="Read-only access to Google Sheets"

# 認証情報 JSON ファイルダウンロード
gcloud iam service-accounts keys create /path/to/credentials.json \
    --iam-account=sheets-reader@your-project.iam.gserviceaccount.com
```

#### スプレッドシート共有設定
1. Google Sheets で対象スプレッドシートを開く
2. 「共有」をクリック
3. サービスアカウントのメールアドレスを追加
4. 権限を「閲覧者」に設定

### 3. 環境変数設定

`.env` ファイルを編集:

```bash
# 必須設定
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/credentials.json
SHEET_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms
WORKSHEET_NAME=Sheet1

# オプション設定（デフォルト値が使用される）
SHEETS_SCOPE=https://www.googleapis.com/auth/spreadsheets.readonly
REQUEST_TIMEOUT=10
RETRY_MAX_ATTEMPTS=3
RETRY_INITIAL_DELAY=5
WATCH_INTERVAL=300
JITTER_PERCENT=10
```

**重要**: `.env` ファイルは絶対にコミットしない！

### 4. 接続テスト

```bash
# 環境設定確認
make status

# 接続テスト
make health
```

## 📖 使用方法

### 基本的な実行

```bash
# 全データ取得
make run

# フィルタ付き取得
make run ARGS='--filter "status=PUBLISHED" --limit 100'

# 特定列のみ取得
make run ARGS='--columns "title,status,date" --out results.json'
```

### 常時監視モード

```bash
# 5分間隔で監視
make watch ARGS='--interval 300'

# フィルタ付き監視
make watch ARGS='--interval 300 --filter "status=ACTIVE" --out data.json'
```

### ヘルスチェック

```bash
# 接続状態確認
make health

# 詳細出力
make health ARGS='--out health_status.json'
```

## 🔍 フィルタリング機能

### 基本フィルタ

```bash
# ステータス一致
--filter 'status=PUBLISHED'

# 複数条件
--filter 'status=ACTIVE,priority=HIGH'
```

### 高度なフィルタ

```bash
# テキスト検索
--filter 'title_contains=重要'

# 日付範囲
--filter 'date_after_date=2023-01-01,date_before_date=2023-12-31'

# 複数値マッチ
--filter 'status_in=ACTIVE|PENDING|REVIEW'
```

## 📊 出力形式

### JSON出力例

```json
{
  "timestamp": "2023-12-07T10:30:00.000Z",
  "count": 2,
  "records": [
    {
      "title": "サンプル動画1",
      "status": "PUBLISHED",
      "date": "2023-12-01"
    },
    {
      "title": "サンプル動画2", 
      "status": "ACTIVE",
      "date": "2023-12-02"
    }
  ]
}
```

## ⚡ 開発・テスト

### テスト実行

```bash
# 全テスト実行
make test

# 特定テスト実行
make test ARGS='tests/test_config.py -v'

# カバレッジ付き
make test ARGS='--cov=. --cov-report=html'
```

### コード品質

```bash
# リンティング
make lint

# コードフォーマット
make format

# 開発環境セットアップ
make dev-setup
```

## 🚨 トラブルシューティング

### 認証エラー

**症状**: `Authentication failed: Permission denied`

**解決策**:
1. サービスアカウントがスプレッドシートに共有されているか確認
2. 認証情報ファイルのパスが正しいか確認
3. サービスアカウントが有効か確認

```bash
# 認証情報確認
gcloud auth activate-service-account --key-file=/path/to/credentials.json
```

### ネットワークエラー

**症状**: `Network error: API quota exceeded`

**解決策**:
1. リクエスト頻度を下げる (`WATCH_INTERVAL` を大きくする)
2. GCP プロジェクトの API クォータを確認
3. 取得データ量を制限 (`--limit` 使用)

### データエラー

**症状**: `Worksheet 'Sheet1' not found`

**解決策**:
1. ワークシート名が正しいか確認
2. シートが削除されていないか確認
3. 複数シートの場合、正確な名前を指定

```bash
# 利用可能なワークシート確認
python3 -c "
import gspread
from google.oauth2.service_account import Credentials

creds = Credentials.from_service_account_file('/path/to/credentials.json')
client = gspread.authorize(creds)
sheet = client.open_by_key('YOUR_SHEET_ID')
print([ws.title for ws in sheet.worksheets()])
"
```

## 🔐 運用時セキュリティ

### 認証情報管理

```bash
# 認証情報ローテーション（月1回推奨）
gcloud iam service-accounts keys create new-credentials.json \
    --iam-account=sheets-reader@your-project.iam.gserviceaccount.com

# 古い認証情報削除
gcloud iam service-accounts keys delete KEY_ID \
    --iam-account=sheets-reader@your-project.iam.gserviceaccount.com
```

### 定期ヘルスチェック

```bash
# cron での定期実行例 (毎時)
0 * * * * cd /path/to/project && make health >> /var/log/sheets-health.log 2>&1
```

### ログローテーション

```bash
# logrotate 設定例
/var/log/sheets-reader.log {
    daily
    rotate 30
    compress
    missingok
    notifempty
    create 0644 user group
}
```

## 📊 モニタリング

### 重要メトリクス

- 成功率 (health check)
- レスポンス時間
- API クォータ使用量
- エラー発生頻度

### アラート設定例

```bash
# 接続失敗時アラート
make health || echo "Sheets connection failed" | mail -s "Alert" admin@example.com

# 長時間実行アラート  
timeout 30s make run || echo "Sheets query timeout" | mail -s "Alert" admin@example.com
```

## 🚀 本番デプロイ

### 環境別設定

```bash
# 開発環境
cp .env.example .env.dev

# 本番環境 
cp .env.example .env.prod
```

### Docker化

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN chmod +x app.py

CMD ["python3", "app.py", "watch"]
```

### Kubernetes デプロイ

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: sheets-credentials
type: Opaque
data:
  credentials.json: <base64-encoded-credentials>
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sheets-reader
spec:
  replicas: 1
  selector:
    matchLabels:
      app: sheets-reader
  template:
    metadata:
      labels:
        app: sheets-reader
    spec:
      containers:
      - name: sheets-reader
        image: sheets-reader:latest
        env:
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: /secrets/credentials.json
        - name: SHEET_ID
          valueFrom:
            secretKeyRef:
              name: sheets-config
              key: sheet-id
        volumeMounts:
        - name: credentials
          mountPath: /secrets
          readOnly: true
      volumes:
      - name: credentials
        secret:
          secretName: sheets-credentials
```

## 📞 サポート

### ヘルプコマンド

```bash
# 使用方法表示
make help

# 利用可能なコマンド一覧
python3 app.py --help

# 各サブコマンドのヘルプ
python3 app.py run --help
python3 app.py watch --help
python3 app.py health --help
```

### 終了コード

- `0`: 正常終了
- `1`: 一般的なエラー
- `2`: 入力不正・設定エラー  
- `3`: 認証エラー
- `4`: 通信・データエラー

### ログレベル

```bash
# デバッグモード
python3 app.py run --log-level DEBUG

# エラーのみ
python3 app.py run --log-level ERROR

# ファイル出力
python3 app.py run --log-file app.log
```

## 🔄 アップデート手順

```bash
# 最新コード取得
git pull origin main

# 依存関係更新
make install

# テスト実行
make test

# 設定確認
make status
```

---

**⚠️ 重要**: このツールは認証情報の安全性を最優先に設計されています。`.env` ファイルや認証情報 JSON ファイルを絶対にリポジトリにコミットしないでください。