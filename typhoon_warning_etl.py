"""颱風警報 ETL（單一檔案、可獨立執行）

打中央氣象署開放資料平台的 W-C0034-001（颱風警報，CAP 格式的正式警報公告），
從裡面一大包欄位中挑出真正有用的資訊，存成一份精簡的 JSON。

跟 township_wind_rain_etl.py（鄉鎮天氣預報+LLM合併）是兩支完全獨立的 ETL：
這支不需要 LLM，純粹是「篩選 + 攤平」CWA 原始欄位，沒有生成/改寫任何文字——
颱風警報是官方正式公告，本來就不該被改寫（詳見 README「為什麼不用 LLM」）。

用法：
    python typhoon_warning_etl.py

需要設定一把金鑰（環境變數，或跟這支程式同目錄放一份 .env，參考 .env.example）：
    CWA_API_KEY  中央氣象署開放資料平台授權碼（https://opendata.cwa.gov.tw 免費註冊取得）

完整說明請見同目錄的 README.md。
"""
from __future__ import annotations  # 讓下面 `dict | None` 這種型別寫法在 Python 3.9 也能執行

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

# 部分環境（尤其客戶端主機）的 Python 內建 SSL 憑證庫對 CWA 憑證鏈驗證異常嚴格
# （CERTIFICATE_VERIFY_FAILED: Missing Subject Key Identifier，換信任庫也不一定救得回來），
# 但系統層級的 curl 一直是通的。與其每次都先讓 requests 撞一次 SSL 錯誤才 fallback，
# 這支程式打 CWA API 直接走系統 curl（見 _fetch_json_via_curl），不經過 Python 的 SSL 驗證。
# 這裡仍 import requests，只是借用它的例外類別給 with_retry 統一判斷「要不要重試」。
import requests

# ============================================================
# 設定
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR / "output"

CWA_DATASTORE_BASE = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
RESOURCE_ID = "W-C0034-001"
TAIWAN_TZ = timezone(timedelta(hours=8))


def _load_dotenv(path: Path) -> None:
    """極簡 .env 讀取，不依賴 python-dotenv 套件——只認得 KEY=VALUE，已存在的環境變數不覆寫。
    只認整行以 # 開頭的註解，不支援行內註解；值頭尾如果包成對的單引號或雙引號會被拿掉（防呆用，
    不支援跳脫字元或變數展開這些完整 dotenv 語法）。"""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(key, value)


_load_dotenv(SCRIPT_DIR / ".env")

CWA_API_KEY = os.environ.get("CWA_API_KEY", "")


def check_config() -> None:
    if not CWA_API_KEY:
        raise SystemExit(
            "缺少必要設定：CWA_API_KEY\n"
            "請設定環境變數，或在本檔案同目錄建立 .env（參考 .env.example）。"
        )


# ============================================================
# 重試
# ============================================================

def with_retry(fn, *args, attempts: int = 3, base_delay_s: float = 2.0, **kwargs):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt == attempts:
                break
            delay = base_delay_s * attempt
            print(f"  [重試] 第 {attempt}/{attempts} 次呼叫失敗（{exc}），{delay}s 後重試...")
            time.sleep(delay)
    raise last_exc


# ============================================================
# Extract：呼叫 CWA
# ============================================================

def _fetch_json_via_curl(url: str, params: dict) -> dict:
    """直接呼叫系統 curl 發送 GET 請求並解析 JSON 回應，不經過 Python 內建 SSL 驗證。
    部分客戶端主機的 Python SSL 對 CWA 憑證鏈驗證過嚴（見 README「SSL 憑證驗證錯誤」），
    curl 走系統層級驗證，穩定能通，所以打 CWA API 一律直接用 curl。"""
    full_url = f"{url}?{urlencode(params)}"
    try:
        result = subprocess.run(
            ["curl", "-sS", "-X", "GET", full_url, "-H", "accept: application/json"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
    except FileNotFoundError as exc:
        raise requests.exceptions.ConnectionError("系統找不到 curl 指令") from exc
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise requests.exceptions.ConnectionError(f"curl 呼叫失敗：{exc}") from exc
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise requests.exceptions.ConnectionError(f"curl 回傳非 JSON：{result.stdout[:200]}") from exc


def fetch_typhoon_warnings() -> list:
    """回傳 records.info（CAP 格式的警報公告陣列，每個颱風目前追蹤中的最新幾份公告）。"""
    url = f"{CWA_DATASTORE_BASE}/{RESOURCE_ID}"
    params = {"Authorization": CWA_API_KEY, "format": "JSON"}
    data = _fetch_json_via_curl(url, params)
    if data.get("success") != "true":  # CWA 回傳的 success 是字串，不是布林
        raise RuntimeError(f"{RESOURCE_ID} 呼叫失敗：{data}")
    return data.get("records", {}).get("info", [])


# ============================================================
# Transform：從一大包 CAP 欄位裡篩出有用的資訊
# ============================================================
#
# 判斷依據（哪些留、哪些丟，理由見 README「欄位取捨說明」）：
#   留：event/urgency/severity/certainty（警報狀態）、effective/onset/expires（生效時間）、
#       senderName/headline/web（發布資訊）、description.section（官方原文段落，逐字保留不改寫）、
#       typhoon-info 裡的警報報數/警報類別/颱風編號/颱風名稱/觀測與預測數據、
#       area（警戒縣市清單，只留 areaDesc 名稱）
#   丟：language/category（每筆都是固定值 "zh-TW"/"Met"，沒有資訊量）、
#       eventCode（CAP 協定內部代號，非人類可讀）、
#       parameter（跟 headline 內容重複）、
#       area[].geocode（內部數字代碼，沒有對照表就沒有意義，只留 areaDesc 文字）

def _find_section(sections: list, title: str) -> dict | None:
    for s in sections:
        if s.get("title") == title:
            return s
    return None


def _find_section_value(sections: list, title: str) -> str | None:
    section = _find_section(sections, title)
    return section.get("value") if section else None


def extract_typhoon_warning(info_entry: dict) -> dict:
    """把一筆 CAP 格式的原始警報公告，篩選+攤平成精簡欄位。"""
    description = info_entry.get("description", {})
    general_sections = description.get("section", [])

    typhoon_info_blocks = description.get("typhoon-info", [])
    ty_sections = typhoon_info_blocks[0].get("section", []) if typhoon_info_blocks else []
    ty_info_item = _find_section(ty_sections, "颱風資訊") or {}

    typhoon_name_en = ty_info_item.get("typhoon_name")
    typhoon_name_zh = ty_info_item.get("cwa_typhoon_name")
    typhoon_name = f"{typhoon_name_zh}（{typhoon_name_en}）" if typhoon_name_zh else typhoon_name_en

    return {
        "颱風名稱": typhoon_name,
        "颱風編號": _find_section_value(ty_sections, "颱風編號"),
        "警報報數": _find_section_value(ty_sections, "警報報數"),
        "警報類別": _find_section_value(ty_sections, "警報類別"),
        "標題": info_entry.get("headline"),
        "緊急程度": info_entry.get("urgency"),
        "嚴重程度": info_entry.get("severity"),
        "確定性": info_entry.get("certainty"),
        "發布單位": info_entry.get("senderName"),
        "生效時間": info_entry.get("effective"),
        "開始時間": info_entry.get("onset"),
        "失效時間": info_entry.get("expires"),
        "官方連結": info_entry.get("web"),
        "警戒縣市": [a.get("areaDesc") for a in info_entry.get("area", []) if a.get("areaDesc")],
        "內文段落": [
            {"標題": s.get("title"), "內容": s.get("value")}
            for s in general_sections
        ],
        "觀測資訊": ty_info_item.get("analysis"),
        "預測資訊": ty_info_item.get("prediction"),
    }


# ============================================================
# Load：存檔
# ============================================================

def main() -> None:
    check_config()

    print(f"呼叫 {RESOURCE_ID}（颱風警報）...")
    raw_infos = with_retry(fetch_typhoon_warnings)
    print(f"  取得 {len(raw_infos)} 筆警報公告")

    warnings = [extract_typhoon_warning(info) for info in raw_infos]

    output = {
        "擷取時間": datetime.now(TAIWAN_TZ).isoformat(),
        "資料來源": "中央氣象署開放資料平台 W-C0034-001（颱風警報）",
        "涵蓋範圍": f"目前平台回傳的颱風警報公告，共 {len(warnings)} 筆",
        "warnings": warnings,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(TAIWAN_TZ).strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"typhoon_warnings_{timestamp}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n完成！共 {len(warnings)} 筆警報公告。")
    print(f"輸出檔案：{output_path}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"\n執行失敗：{exc}", file=sys.stderr)
        sys.exit(1)
