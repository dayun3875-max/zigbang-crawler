import requests
import csv
import time
import random
from datetime import datetime
from pathlib import Path

LOCAL_CODE = "11710"
LOCAL_NAME = "송파구"
LIST_URL   = f"https://apis.zigbang.com/apt/locals/{LOCAL_CODE}/item-catalogs"
PAGE_LIMIT = 20

BASE_DIR = Path("data")
BASE_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_CSV   = BASE_DIR / f"zigbang_{LOCAL_NAME}_{timestamp}.csv"
LOG_FILE  = BASE_DIR / f"zigbang_{LOCAL_NAME}_{timestamp}_log.txt"

TRAN_TYPE_KR = {"trade": "매매", "charter": "전세", "rental": "월세"}
DIRECTION_KR = {
    "e": "동향", "w": "서향", "s": "남향", "n": "북향",
    "se": "남동향", "sw": "남서향", "ne": "북동향", "nw": "북서향",
}

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.zigbang.com",
    "referer": "https://www.zigbang.com/",
    "sdk-version": "0.112.0",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "x-zigbang-platform": "www",
}

def log(msg):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line  = f"[{stamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def sleep():
    time.sleep(1 + random.uniform(0, 0.3))

def api_get(offset):
    params = [
        ("tranTypeIn[]", "trade"),
        ("tranTypeIn[]", "charter"),
        ("tranTypeIn[]", "rental"),
        ("includeOfferItem", "true"),
        ("offset", offset),
        ("limit", PAGE_LIMIT),
    ]
    for attempt in range(3):
        try:
            res = requests.get(LIST_URL, headers=HEADERS, params=params, timeout=15)
            if res.status_code == 200:
                return res.json()
            log(f"  ⚠️ HTTP {res.status_code} (시도 {attempt+1}/3)")
        except Exception as e:
            log(f"  ❌ 오류 ({attempt+1}/3): {e}")
        time.sleep(2)
    return None

def fetch_all_items():
    all_items = []
    offset    = 0
    total     = None

    while True:
        data = api_get(offset)
        if not data:
            log("  ❌ API 응답 없음. 중단.")
            break

        items = data.get("list") or []

        if offset == 0:
            total = data.get("count", 0)
            pages = -(-total // PAGE_LIMIT)
            log(f"  {LOCAL_NAME} 전체 매물: {total}개 | 예상 페이지: {pages}p")

        if not items:
            break

        all_items.extend(items)
        log(f"  offset={offset:>4} → {len(items):>2}개 수집 | 누적: {len(all_items)}/{total}")

        if len(all_items) >= total or len(items) < PAGE_LIMIT:
            break

        offset += PAGE_LIMIT
        sleep()

    return all_items

def parse_row(item):
    now       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    size_m2   = item.get("sizeM2") or 0
    deposit   = item.get("depositMin", 0) or 0
    rent      = item.get("rentMin", 0) or 0
    rt        = item.get("roomTypeTitle") or {}
    tran_type = item.get("tranType", "")

    item_id_list = item.get("itemIdList") or [{}]
    id_obj  = item_id_list[0] if item_id_list else {}
    item_id = id_obj.get("itemId", "") if isinstance(id_obj, dict) else ""

    return {
        "수집시간":        now,
        "단지ID":          item.get("areaDanjiId", ""),
        "단지명":          item.get("areaDanjiName", ""),
        "매물ID":          item_id,
        "매물출처":        id_obj.get("itemSource", "") if isinstance(id_obj, dict) else "",
        "호ID":            item.get("areaHoId", ""),
        "단지타입ID":      item.get("danjiRoomTypeId", ""),
        "거래유형_영문":   tran_type,
        "거래유형":        TRAN_TYPE_KR.get(tran_type, ""),
        "보증금(만원)":    deposit,
        "월세(만원)":      rent,
        "가격(억)":        round(deposit / 10000, 2) if deposit else 0,
        "가격범위여부":    item.get("isPriceRange", ""),
        "구":              item.get("local2", ""),
        "동네":            item.get("local3", ""),
        "동번호":          item.get("dong", ""),
        "층":              item.get("floor", ""),
        "면적_전용m2":     size_m2,
        "면적_전용평":     round(size_m2 / 3.3058, 2) if size_m2 else "",
        "면적_계약m2":     item.get("sizeContractM2", ""),
        "타입_m2":         rt.get("m2", "") if isinstance(rt, dict) else "",
        "타입_평":         rt.get("p", "") if isinstance(rt, dict) else "",
        "방향":            DIRECTION_KR.get(item.get("direction", ""), item.get("direction", "")),
        "제목":            item.get("itemTitle", ""),
        "동일호_매물수":   item.get("itemCount", 1),
        "동일호_여부":     "동일호중복" if (item.get("itemCount") or 1) > 1 else "단독매물",
        "매물유형":        item.get("itemType", ""),
        "실거래확인":      item.get("isActualItemChecked", ""),
        "직접등록여부":    item.get("isHugLessor", ""),
        "매물URL":         f"https://www.zigbang.com/home/apt/items/{item_id}",
    }

# ✅ 여기만 수정: 저장 전 정렬 추가
def save_csv(rows):
    TRAN_ORDER = {"매매": 0, "전세": 1, "월세": 2}

    sorted_rows = sorted(
        rows,
        key=lambda r: (
            r["동네"],                          # 1순위: 동네
            r["단지명"],                         # 2순위: 단지명
            str(r["동번호"]),                    # 3순위: 동번호
            TRAN_ORDER.get(r["거래유형"], 9),    # 4순위: 거래유형 (매매→전세→월세)
            r["가격(억)"],                       # 5순위: 가격 오름차순
        )
    )

    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=sorted_rows[0].keys())
        writer.writeheader()
        writer.writerows(sorted_rows)
    log(f"  💾 저장완료: {OUT_CSV}")

def main():
    log("=" * 60)
    log(f"직방 {LOCAL_NAME} 전체 매물 수집 시작")
    log(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)

    items = fetch_all_items()
    if not items:
        log("❌ 수집된 매물 없음")
        return

    seen_uid = set()
    deduped  = []
    for item in items:
        row = parse_row(item)
        uid = str(row["매물ID"]) or f"{row['단지ID']}_{row['층']}_{row['면적_전용m2']}"
        if uid not in seen_uid:
            seen_uid.add(uid)
            deduped.append(row)

    log(f"\n총 수집: {len(items)}개 | 중복제거 후: {len(deduped)}개")
    save_csv(deduped)

    log("\n[거래유형별 통계]")
    for tran_kr in TRAN_TYPE_KR.values():
        cnt = sum(1 for r in deduped if r["거래유형"] == tran_kr)
        log(f"  {tran_kr}: {cnt}개")

    log(f"✅ 완료! CSV: {OUT_CSV} | LOG: {LOG_FILE}")

if __name__ == "__main__":
    main()

