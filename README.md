# AI_stock_market_weekly

台股每週訊號追蹤專案。程式會在每週五台灣時間收盤後抓取資料，追蹤台灣加權指數與中大型權值股，產出「每週台股趨勢報告」HTML Email 與社群圖片。

正式工作目錄：

`C:\Users\zergv\Documents\GitHub\AI_stock_market_weekly`

GitHub repo：

https://github.com/ryanhsu1983/AI_stock_market_weekly

## 追蹤標的

- 台灣加權指數
- 台積電
- 聯發科
- 台達電
- 鴻海
- 廣達
- 緯創
- 緯穎

## 主要檔案

- `stock_market_tracking_system.py`：主程式，負責抓資料、計算每日模型與週報指標、產生 HTML 報告、圖片與寄信。
- `config.json`：追蹤標的、指標門檻、Email、重大事件、Google Drive 上傳設定。
- `.github/workflows/weekly_run.yml`：GitHub Actions 每週五自動執行設定。
- `email_preview.html`：本機執行後產生的預覽檔，不應提交到 Git。

## 週報模型

週報沿用每日版的趨勢、MACD、三大法人、KD、OBV、匯率、利率、量能、BIAS60 模型，並增加每週趨勢資訊：

- 本週收盤價變化與 5 日漲跌幅
- 本週高低點
- 週成交量、週日均量與 20 日均量比
- 本週三大法人買賣超合計
- 收盤價相對 10/20/60 日均線位置
- 本週趨勢總結
- 下週觀察重點
- 強勢續抱、過熱不追、轉弱觀察、修正等待、盤整區間等週報判讀

## Email 與圖片

Email 標題格式：

`【每週台股趨勢報告】YYYY-MM-DD 第N週`

社群圖片最多兩張：

- 第 1 張：本週市場總覽、重大事件、新聞、加權指數週趨勢
- 第 2 張：8 檔個股週變化與下週觀察重點

Google Drive 根資料夾：

https://drive.google.com/drive/u/0/folders/1E6w7XNwm3nn7XhjwQwdM62zVQCJgGK6M

上傳路徑：

`每週發布 / 年份 / 台股加權及中大型權值股訊號追蹤`

圖片檔名格式：

`YYYYMMDD_weekN__01.png`

`YYYYMMDD_weekN__02.png`

同一週防重複寄送會以第 1 張週報圖片檔名作為完成判斷；若 Google Drive 已存在同週檔案，備援排程或手動重跑會跳過寄送。

## GitHub Actions

排程：

- 台灣時間每週五 15:00
- 台灣時間每週五 15:30 備援
- 保留 `workflow_dispatch` 手動執行

GitHub Actions 使用 UTC，因此 workflow 內為：

- `0 7 * * 5`
- `30 7 * * 5`

## 本機測試

在正式工作目錄執行：

```powershell
python -m py_compile stock_market_tracking_system.py
python stock_market_tracking_system.py
```

如果本機沒有設定 `GMAIL_PASSWORD`，程式會跳過寄信，但仍會產生 HTML 預覽。若沒有 Google OAuth 或 service account 憑證，圖片會保留在本機但不會上傳。
