# CWA 氣象資料 ETL（兩支獨立流程）

這個資料夾裡有**兩支互相獨立、可以各自單獨執行**的 ETL 程式，把中央氣象署（CWA）開放資料平台的資料整理成好用的 JSON。兩支程式共用同一份 `requirements.txt`、`.env.example`，但主程式（`.py`）彼此不互相依賴，可以只跑其中一支。

| 程式 | 資料來源 | 做什麼 | 用 LLM？ |
|---|---|---|---|
| [`township_wind_rain_etl.py`](#1-township_wind_rain_etlpy---全台鄉鎮風速風向雨量-3-天展望) | `F-D0047-*`（鄉鎮天氣預報） | 把全台 22 縣市、約 368 個鄉鎮的風速/風向/降雨機率/天氣現象，濃縮成每個鄉鎮一則中文敘述 | ✅ 是（透過內部 Bedrock-Mantle 閘道呼叫 google.gemma-4-31b） |
| [`typhoon_warning_etl.py`](#2-typhoon_warning_etlpy---颱風警報) | `W-C0034-001`（正式颱風警報） | 篩選＋攤平官方颱風警報公告的欄位，內文逐字保留、不改寫 | ❌ 否 |

---

## 快速開始

```bash
# 1. 安裝套件（Python 3.9 以上）
pip install -r requirements.txt
# 若系統同時裝有 Python 2/3、pip 預設指向 Python 2，請改用：
pip3 install -r requirements.txt

# 2. 設定金鑰（.env.example 已內建可直接用的 CWA 示範金鑰；LLM_API_KEY 由講師另外提供，填入 .env）
# 若無.env.example檔案則需手動新增 .env
cp .env.example .env


# 3. 執行——兩支互相獨立，需要哪支就跑哪支
# 大多數 macOS/Linux 環境預設只有 python3（沒有 python 指令），Windows 則通常兩者皆有；
# 若不確定，先試 python3，指令找不到再改用 python。
python3 typhoon_warning_etl.py        # 颱風警報，1 次 API 呼叫，幾秒完成
python3 township_wind_rain_etl.py     # 全台鄉鎮風速/雨量，22 次 CWA + 22 次 LLM，首次約 3-5 分鐘

# 4. 結果自動存到 output/ 資料夾，檔名各自帶時間戳記

```

⚠️ **網路需求**：兩支都要能連到 `opendata.cwa.gov.tw`；`township_wind_rain_etl.py` 另外要能連到 `bedrock-mantle.us-east-1.api.aws`（LLM Gateway）。如果是企業內網、有出站防火牆或 Proxy，記得先確認這兩個網域有開放，不然程式會卡在連線逾時。

⚠️ **SSL 憑證驗證錯誤**：部分客戶端主機執行時可能出現 `CERTIFICATE_VERIFY_FAILED: Missing Subject Key Identifier`（有時混雜 `UNEXPECTED_EOF_WHILE_READING`）——這是 Python 內建 SSL 對 CWA 憑證鏈驗證較嚴格所致（`curl` 打同一支 API 通常不受影響）。兩支程式已內建兩層防護：
1. `truststore`（見 `requirements.txt`）：改用作業系統原生的憑證信任機制，多數情況能解決落差，正常裝好 `requirements.txt` 即可、不需額外設定；Python 版本低於 3.10 時不支援、會靜默退回原本行為。
2. **curl 備援**：如果 `truststore` 裝了還是遇到同樣的 SSL 錯誤（例如系統 OpenSSL 版本本身對這張憑證鏈判定過嚴，連信任庫都救不了），呼叫 CWA API 那步會自動改叫系統 `curl` 重新發送同一支請求（`curl` 走的驗證邏輯通常較寬鬆）。這需要主機上裝有 `curl` 指令（Linux/macOS 預設都有）；若還是失敗，錯誤訊息會明確標示是 curl 備援本身失敗（而不是原本的 SSL 錯誤），代表連 curl 都連不上，問題出在網路本身（防火牆/DNS）而非憑證。

---

## 金鑰說明

| 金鑰 | 用途 | 哪支程式需要 | `.env.example` 現況 | 正式環境怎麼換 |
|---|---|---|---|---|
| `CWA_API_KEY` | 呼叫中央氣象署開放資料平台 | 兩支都需要 | 已內建 CWA 官方文件公開的示範金鑰，本身無機密性，可直接使用 | 到 [opendata.cwa.gov.tw](https://opendata.cwa.gov.tw) 免費註冊會員 → 會員中心 → 「API 授權碼」→「取得授權碼」，換成自己的 |
| `LLM_API_KEY` | 呼叫內部 LLM Gateway（Bedrock-Mantle，OpenAI 相容格式）的 API Key，預設打 `google.gemma-4-31b` | 只有 `township_wind_rain_etl.py` 需要 | 留空——這是會實際扣費/計量的金鑰，不放進版本控制，教育訓練當天由講師另外提供 | 跟負責這個內部 LLM Gateway 的窗口申請自己的正式金鑰後替換 |

⚠️ `.env`（跟任何填了真實 `LLM_API_KEY` 的複本）**不要**上傳版本控制或跟訓練教材以外的對象分享。

---

## 1. `township_wind_rain_etl.py` - 全台鄉鎮風速/風向/雨量 3 天展望

把 CWA「鄉鎮天氣預報」（`F-D0047-*`，每縣市一個 resource_id）近 3 天、每 12 小時一筆的風速、風向、降雨機率、天氣現象，逐縣市丟給 LLM 濃縮成「每個鄉鎮一則」固定格式的中文敘述，供停電風險評估參考，例如：

> 8月20~22日，風向以偏東風為主，風速介於2~6m/s（蒲福2~4級），降雨機率介於20~60%，天氣現象以多雲短暫陣雨或雷雨為主。

呼叫次數：**22 次 CWA API + 22 次 LLM API**（每個縣市各一次，不是每個鄉鎮各打一次）。

### 執行

```bash
python township_wind_rain_etl.py
```

執行時會依序印出各縣市處理進度，完成後印出輸出檔案路徑，例如 `output/township_wind_rain_outlook_20260820_093000.json`。

### 輸出格式

```json
{
  "擷取時間": "2026-08-20T09:30:00+08:00",
  "涵蓋範圍": "全台 22 縣市、共 368 個鄉鎮市區，近3天，聚焦風速/風向/降雨機率/天氣現象",
  "counties": {
    "宜蘭縣": {
      "宜蘭市": "8月20～22日，風向以偏東風為主，風速介於2～6m/s（蒲福2～4級），降雨機率介於20～60%，天氣現象以多雲短暫陣雨或雷雨為主。",
      "羅東鎮": "..."
    }
  }
}
```

依縣市分組（`counties.<縣市>.<鄉鎮>`），避免不同縣市剛好有同名鄉鎮互相覆蓋。

### 快取（避免重跑浪費 API 費用）

- LLM 合併結果存在 `cache/<日期>_<縣市>.json`，**同一天**重跑會直接讀快取、不重打 LLM；**隔天**日期變了自動失效、重新產生。
- 想強制重跑某個縣市：刪掉 `cache/` 底下對應的檔案即可。
- 縣市對照表（`township_resource_map.json`）只探測一次、永久快取；如果哪天抓不到資料，把這個檔案刪掉重新探測。

### 常見問題

**Q: 報錯「缺少必要設定：CWA_API_KEY」？**
A: 確認 `.env` 跟 `township_wind_rain_etl.py` 放在同一個資料夾，且金鑰有正確填入。

**Q: 中途某個縣市失敗中斷了怎麼辦？**
A: 直接重新執行即可，已成功處理過的縣市會讀快取、不會重打 LLM，只會繼續處理沒完成的部分。

**Q: 這是官方颱風警報嗎？**
A: 不是，這支抓的是日常「鄉鎮天氣預報」。要正式颱風警報請用下面的 `typhoon_warning_etl.py`。

---

## 2. `typhoon_warning_etl.py` - 颱風警報

把 CWA 的**正式颱風警報公告**（`W-C0034-001`，CAP 格式）篩選＋攤平成精簡 JSON，只留判斷警報等級用的欄位（`event`/`urgency`/`severity`/`certainty`）、生效/開始/失效時間、發布資訊、颱風編號/名稱/報數、觀測與預測數據、警戒縣市，拿掉內部代號跟重複欄位；官方原文（`內文段落`）**逐字保留、不經過任何 AI 改寫**——欄位細節可直接參考下方輸出範例。

**這支完全不用 LLM**：《氣象法》第18、24條規定只有中央氣象署許可者才能發布颱風預報/警報，把官方警報內容拿去讓 LLM 改寫或摘要，改寫後的文字就不再是官方原文，有觸法風險，所以這支的角色只有篩選＋攤平，不生成或改寫任何文字。

呼叫次數：**1 次 CWA API**（不分縣市，一次拿到全部現有警報）。沒有快取機制，重跑成本很低，不需要。

⚠️ **時區小陷阱**：`effective`/`onset`/`expires` 是 `+08:00`（台灣時間），但 `觀測資訊`/`預測資訊` 裡的 `time` 是 `+00:00`（UTC）——這是 CWA 資料本身的狀況，要拿 `time` 跟其他時間比較或顯示，記得先轉換成台灣時間（+8 小時）。

### 執行

```bash
python typhoon_warning_etl.py
```

執行後印出取得的警報筆數與輸出檔案路徑，例如 `output/typhoon_warnings_20260820_100000.json`。

### 輸出格式

```json
{
  "擷取時間": "2026-08-20T10:00:00+08:00",
  "涵蓋範圍": "目前平台回傳的颱風警報公告，共 1 筆",
  "warnings": [
    {
      "颱風名稱": "白海豚（DOLPHIN）",
      "颱風編號": "13",
      "警報報數": "20",
      "警報類別": "END",
      "標題": "解除颱風警報",
      "緊急程度": "Past",
      "嚴重程度": "Minor",
      "確定性": "Observed",
      "發布單位": "中央氣象署",
      "生效時間": "2026-08-09T23:30:00+08:00",
      "開始時間": "2026-08-09T23:30:00+08:00",
      "失效時間": "2026-08-09T23:40:00+08:00",
      "官方連結": "https://www.cwa.gov.tw/V8/C/P/Warning/FIFOWS.html",
      "警戒縣市": ["基隆市", "臺北市", "新北市", "..."],
      "內文段落": [
        {"標題": "命名與位置", "內容": "輕度颱風 白海豚（國際命名 DOLPHIN）9日23時的中心位置..."}
      ],
      "觀測資訊": {
        "time": "2026-08-09T15:00:00+00:00",
        "position": "27.90,119.80",
        "max_winds": {"value": "25", "unit": "m/s"},
        "gust": {"value": "33", "unit": "m/s"},
        "pressure": {"value": "985", "unit": "hPa"}
      },
      "預測資訊": {
        "time": "2026-08-10T15:00:00+00:00",
        "position": "29.90,116.60",
        "max_winds": {"value": "15", "unit": "m/s"},
        "pressure": {"value": "995", "unit": "hPa"}
      }
    }
  ]
}
```

`warnings` 是陣列，目前有幾份公告就有幾筆；沒有任何颱風警報時可能是空陣列 `[]`，這是正常情況。

### 常見問題

**Q: `警報類別` 是 `"END"` 是什麼意思？**
A: `END` 代表「解除警報」，實際內容建議直接看 `標題` 欄位的中文說明更直覺。

**Q: 這份資料可以直接對外發布嗎？**
A: 內容是 CWA 官方公告，發布時要標示資料來源與 `發布單位`/`官方連結`，不要把 `內文段落` 之外的欄位另外改寫成「預報」文字對外發布（見上方「為什麼不用 LLM」）。

---

## 3. 檔案說明

```
township_wind_rain_etl.py    主程式 1，鄉鎮風速/風向/雨量 3天展望，可獨立執行
typhoon_warning_etl.py       主程式 2，颱風警報，可獨立執行
requirements.txt              兩支程式共用的 Python 套件需求（requests、jsonschema、truststore）
.env.example                  兩支程式共用的金鑰設定範本（複製成 .env）
township_resource_map.json    （首次執行 township_wind_rain_etl.py 後自動產生）22縣市對照表快取
cache/                        （首次執行 township_wind_rain_etl.py 後自動產生）LLM 合併結果快取，依日期+縣市命名
output/                       （執行後自動產生）兩支程式的最終輸出 JSON，檔名前綴不同不會互相覆蓋
```

兩支主程式互不依賴、可以只安裝需求跑其中一支；`output/`、`cache/` 資料夾執行時自動建立，不需要事先手動建立。
