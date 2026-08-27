"""全台鄉鎮風速/風向/雨量 3 天展望 ETL（單一檔案、可獨立執行）

用法：
    python township_wind_rain_etl.py

需要先設定兩把金鑰（環境變數，或跟這支程式同目錄放一份 .env，參考 .env.example）：
    CWA_API_KEY  中央氣象署開放資料平台授權碼（https://opendata.cwa.gov.tw 免費註冊取得）
    LLM_API_KEY  內部 LLM Gateway（Bedrock-Mantle，OpenAI 相容格式）的 API Key

完整流程說明、輸出格式、常見問題請見同目錄的 README.md，這裡只放程式邏輯。
"""
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import jsonschema

# 打 CWA API 那段已經改走系統 curl（見 _fetch_json_via_curl），不經過 Python 內建 SSL，
# 所以不受下面這個問題影響。但打 LLM Gateway（bedrock-mantle）那段還是用 requests 直連，
# 部分環境的 Python 內建 SSL 憑證庫可能驗證過嚴而失敗，改用作業系統原生信任庫可解決；
# 若環境沒裝 truststore（例如 Python < 3.10）則靜默略過，退回原本行為。
try:
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass

import requests

# ============================================================
# 設定
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR / "output"
CACHE_DIR = SCRIPT_DIR / "cache"
RESOURCE_MAP_PATH = SCRIPT_DIR / "township_resource_map.json"

CWA_DATASTORE_BASE = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://bedrock-mantle.us-east-1.api.aws/openai/v1/chat/completions")
LLM_MODEL = os.environ.get("LLM_MODEL", "google.gemma-4-31b")

TAIWAN_TZ = timezone(timedelta(hours=8))
NEAR_TERM_ENTRIES = 6  # 1週逐12小時資料共14筆(7天x2)，取前6筆(=近3天)
EXPECTED_COUNTIES = 22
MAX_PROBE = 95  # Step 0 探測 resource_id 的上限，正常 44 次內就會找齊 22 縣市

NARRATIVE_TEMPLATE = (
    "{M}月{D1}~{D2}日，風向以{風向}為主，風速介於{風速下限}~{風速上限}m/s"
    "（蒲福{蒲福下限}~{蒲福上限}級），降雨機率介於{降雨下限}~{降雨上限}%，天氣現象以{天氣現象}為主。"
)

MERGE_SYSTEM_PROMPT = """你是台電營業區處防災準備的氣象分析助手。任務：把「__COUNTY__」底下每個鄉鎮
近3天、每12小時一筆的風速/風向/降雨機率/天氣現象資料，濃縮成「每個鄉鎮一則」的固定格式敘述，
供停電風險評估參考。

輸出格式（每個鄉鎮都套用同一個模板，把底線部分換成你從資料算出來的值，其餘文字照抄不要更動）：
""" + NARRATIVE_TEMPLATE + """

規則：
1. {M}{D1}{D2} 是這 3 天資料涵蓋的月份與起訖日期（例如 8、19、22）。
2. {風速下限}~{風速上限}、{蒲福下限}~{蒲福上限}、{降雨下限}~{降雨上限} 都是這 3 天資料裡的最小值~最大值，
   不是平均值，且必須是原始資料中實際出現過的數字，不可推算或杜撰。
3. {風向} 填這 3 天最常出現的風向；如果有兩種差不多常見，用頓號連接最多兩個（例如「東北風、偏東風」）。
4. {天氣現象} 填這 3 天最常出現的天氣現象；同樣如果有兩種差不多常見，用頓號連接最多兩個。
5. 只能用風速/風向/降雨機率/天氣現象這幾項資訊，不要提及溫度、濕度、舒適度。
6. 輸出必須是 JSON 物件，key 為鄉鎮名稱，value 為套用模板後的敘述字串，
   鄉鎮清單必須完整對應輸入資料，不可遺漏、不可新增。
"""


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
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")


def check_config() -> None:
    missing = []
    if not CWA_API_KEY:
        missing.append("CWA_API_KEY")
    if not LLM_API_KEY:
        missing.append("LLM_API_KEY")
    if missing:
        raise SystemExit(
            "缺少必要設定：" + "、".join(missing) + "\n"
            "請設定環境變數，或在本檔案同目錄建立 .env（參考 .env.example）。"
        )


# ============================================================
# 重試（CWA / LLM 呼叫共用）
# ============================================================

def with_retry(fn, *args, attempts: int = 3, base_delay_s: float = 2.0, **kwargs):
    """對 fn 的呼叫做線性退避重試，只重試網路層級的錯誤（逾時、連線中斷、5xx/429 等）。"""
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
# Step 0 + Step 1：CWA 開放資料平台
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


def fetch_cwa_resource(resource_id: str) -> dict:
    url = f"{CWA_DATASTORE_BASE}/{resource_id}"
    params = {"Authorization": CWA_API_KEY, "format": "JSON"}
    data = _fetch_json_via_curl(url, params)
    if data.get("success") != "true":  # CWA 回傳的 success 是字串，不是布林
        raise RuntimeError(f"{resource_id} 呼叫失敗：{data}")
    return data


def discover_township_forecast_resources() -> dict:
    """{縣市名: resource_id}，鄉鎮天氣預報「1週逐12小時版」。第一次執行會探測並存檔到
    township_resource_map.json，之後執行直接讀檔，不會重複打 API。"""
    if RESOURCE_MAP_PATH.exists():
        with open(RESOURCE_MAP_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    print("首次執行，探索縣市對照表（約 44 次 API 呼叫，之後會存檔快取，不會重複跑）...")
    found: dict = {}
    for n in range(1, MAX_PROBE + 1, 2):
        resource_id = f"F-D0047-{n:03d}"
        try:
            data = with_retry(fetch_cwa_resource, resource_id)
        except (requests.exceptions.RequestException, RuntimeError) as exc:
            print(f"  [探測] {resource_id} 失敗，跳過：{exc}")
            continue

        locations = data.get("records", {}).get("Locations", [])
        for loc in locations:
            if "1週" not in (loc.get("DatasetDescription") or ""):
                continue
            county = loc["LocationsName"]
            if county not in found:
                found[county] = resource_id

        if len(locations) > 1:
            print(f"  [探測] {resource_id} 一次回傳 {len(locations)} 個縣市，視為全台彙整資源，停止探測。")
            break
        if len(found) >= EXPECTED_COUNTIES:
            break

    if len(found) < EXPECTED_COUNTIES:
        raise RuntimeError(
            f"只探測到 {len(found)}/{EXPECTED_COUNTIES} 個縣市：{list(found.keys())}，"
            "可能是 CWA 編號規則變了，需要人工檢查。"
        )

    with open(RESOURCE_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(found, f, ensure_ascii=False, indent=2)
    return found


def fetch_township_forecast(resource_id: str) -> dict:
    """一個縣市的完整回應：{LocationsName, DatasetDescription, Location: [各鄉鎮...]}"""
    data = with_retry(fetch_cwa_resource, resource_id)
    locations = data.get("records", {}).get("Locations", [])
    if not locations:
        raise RuntimeError(f"{resource_id} 沒有回傳任何 Locations")
    return locations[0]


# ============================================================
# Step 1（續）：篩選只留風速/風向/降雨機率/天氣現象
# ============================================================

def _time_series(weather_element: dict, value_keys: list) -> list:
    """攤平一個 WeatherElement 的 Time[]，統一成 [{start, end, **數值}]（CWA 有些要素用單一
    DataTime，有些用 StartTime/EndTime 區間，這裡統一處理掉這個差異）。"""
    series = []
    for t in weather_element.get("Time", []):
        start = t.get("StartTime") or t.get("DataTime")
        end = t.get("EndTime") or t.get("DataTime")
        values = t.get("ElementValue", [{}])[0]
        series.append({"start": start, "end": end, **{k: values.get(k) for k in value_keys}})
    return series


def extract_wind_rain_context(location: dict, max_entries: int = NEAR_TERM_ENTRIES) -> dict:
    """只留風速/風向/12小時降雨機率/天氣現象，捨棄溫度/濕度/舒適度跟冗長的『天氣預報綜合描述』
    全文（那段話本身就是把風速風向雨量又用中文講一次，兩份一起給 LLM 只會浪費 token）。
    只取前 max_entries 筆（近3天）。回傳 {township, wind_rain_timeline: [逐段文字]}。"""
    elements = {e["ElementName"]: e for e in location.get("WeatherElement", [])}
    wind_speed = _time_series(elements.get("風速", {}), ["WindSpeed", "BeaufortScale"])[:max_entries]
    wind_direction = _time_series(elements.get("風向", {}), ["WindDirection"])[:max_entries]
    rain_probability = _time_series(elements.get("12小時降雨機率", {}), ["ProbabilityOfPrecipitation"])[:max_entries]
    weather = _time_series(elements.get("天氣現象", {}), ["Weather"])[:max_entries]

    timeline = []
    for ws, wd, rp, wx in zip(wind_speed, wind_direction, rain_probability, weather):
        start_label = (ws["start"] or "")[5:16].replace("T", " ")  # "08-19 18:00"
        end_label = (ws["end"] or "")[11:16]  # "06:00"
        timeline.append(
            f"{start_label}~{end_label} 風向{wd.get('WindDirection', '?')} "
            f"風速{ws.get('WindSpeed', '?')}m/s(蒲福{ws.get('BeaufortScale', '?')}級) "
            f"降雨機率{rp.get('ProbabilityOfPrecipitation', '?')}% "
            f"天氣現象{wx.get('Weather', '?')}"
        )
    return {"township": location.get("LocationName"), "wind_rain_timeline": timeline}


# ============================================================
# Step 2：LLM 合併成每鄉鎮一則敘述（每縣市一次呼叫）
# ============================================================

def llm_chat_json(system: str, user: str, max_tokens: int = 6000, temperature: float = 0.3, timeout: int = 120) -> dict:
    body = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        # response_format: json_object 是 OpenAI 的 JSON 模式語法，要求訊息內容包含「json」字樣才會生效；
        # 目前打的 Bedrock-Mantle 閘道聲稱 OpenAI 相容，但沒有拿到正式金鑰實測過這個欄位是否真的有效，
        # 如果之後發現輸出常常不是合法 JSON（觸發下面的修正重試也修不好），先來這裡確認這個欄位有沒有被閘道正確處理。
        "response_format": {"type": "json_object"},
    }
    resp = requests.post(
        LLM_BASE_URL,
        headers={"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"},
        json=body,
        timeout=timeout,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    return json.loads(content)


def _township_schema(township_names: list) -> dict:
    return {
        "type": "object",
        "required": township_names,
        "properties": {name: {"type": "string"} for name in township_names},
        "additionalProperties": False,
    }


def _cache_path(cache_date: str, county_name: str) -> Path:
    safe_name = county_name.replace("/", "_")
    return CACHE_DIR / f"{cache_date}_{safe_name}.json"


def merge_county_narratives(county_name: str, township_contexts: list, cache_date: str) -> dict:
    """一次 LLM 呼叫，把一個縣市底下所有鄉鎮的風速/風向/雨量資料合併成 {鄉鎮: 敘述}。
    快取檔名是「日期_縣市.json」——同一天重跑會直接讀快取、不重打 LLM；隔天日期變了
    自動失效重新產生。若當天想強制重新產生某縣市，手動刪除對應快取檔即可（見 README）。"""
    township_names = [t["township"] for t in township_contexts]
    cache_file = _cache_path(cache_date, county_name)

    if cache_file.exists():
        print(f"  [快取命中] {county_name}（{cache_file.name}）")
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    system_prompt = MERGE_SYSTEM_PROMPT.replace("__COUNTY__", county_name)
    user_content = json.dumps(township_contexts, ensure_ascii=False)
    schema = _township_schema(township_names)

    try:
        result = with_retry(llm_chat_json, system_prompt, user_content)
        jsonschema.validate(result, schema)
    except (jsonschema.ValidationError, json.JSONDecodeError) as exc:
        print(f"  [修正重試] {county_name} 輸出格式錯誤或未通過驗證：{exc}")
        repair_user = (
            f"{user_content}\n\n(前次輸出格式錯誤或未通過驗證：{exc}，"
            f"請修正後重新輸出合法 JSON 物件，且必須完整包含以下鄉鎮：{township_names})"
        )
        result = with_retry(llm_chat_json, system_prompt, repair_user)
        jsonschema.validate(result, schema)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


# ============================================================
# Step 3：彙整存檔
# ============================================================

def main() -> None:
    check_config()
    cache_date = datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d")

    print("Step 0：取得縣市對照表...")
    county_map = discover_township_forecast_resources()
    print(f"  共 {len(county_map)} 個縣市\n")

    print("Step 1+2：逐縣市抓取、篩選、LLM 合併...")
    all_results = {}
    total_townships = 0
    for county_name, resource_id in county_map.items():
        print(f"處理 {county_name}（{resource_id}）...")
        location_data = fetch_township_forecast(resource_id)
        townships = location_data.get("Location", [])

        contexts = [extract_wind_rain_context(t) for t in townships]
        narratives = merge_county_narratives(county_name, contexts, cache_date)

        all_results[county_name] = narratives
        total_townships += len(narratives)
        print(f"  -> {len(narratives)} 個鄉鎮完成")

    print(f"\nStep 3：彙整存檔...")
    output = {
        "擷取時間": datetime.now(TAIWAN_TZ).isoformat(),
        "資料來源": "中央氣象署開放資料平台 F-D0047-*（鄉鎮1週逐12小時天氣預報，取近3天）",
        "涵蓋範圍": f"全台 {len(all_results)} 縣市、共 {total_townships} 個鄉鎮市區，近3天，聚焦風速/風向/降雨機率/天氣現象",
        "counties": all_results,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(TAIWAN_TZ).strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"township_wind_rain_outlook_{timestamp}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n完成！共 {len(all_results)} 縣市、{total_townships} 個鄉鎮。")
    print(f"輸出檔案：{output_path}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 - 頂層錯誤要讓使用者看到訊息再退出，不要印一堆 traceback
        print(f"\n執行失敗：{exc}", file=sys.stderr)
        sys.exit(1)
