import requests
import csv
import time
from datetime import datetime

url = "https://apis.zigbang.com/apt/danjis/38487/item-catalogs"

headers = {
    "accept": "application/json, text/plain, */*",
    "origin": "https://www.zigbang.com",
    "referer": "https://www.zigbang.com/",
    "user-agent": "Mozilla/5.0",
    "x-zigbang-platform": "www",
    "sdk-version": "0.112.0"
}

params = {
    "userNo": "27271436",
    "offset": 0,
    "limit": 100
}

all_rows = []

while True:

    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    items = data.get("list", [])

    if not items:
        break

    for i in items:

        item_id = i.get("itemIdList", [{}])[0].get("itemId")

        deposit = i.get("depositMin", 0) or 0
        rent = i.get("rentMin", 0) or 0

        price_eok = round(deposit / 10000, 2) if deposit else 0

        size_m2 = i.get("sizeM2", 0) or 0
        size_py = round(size_m2 / 3.3058, 2) if size_m2 else 0

        room = i.get("roomTypeTitle", {})

        item_url = f"https://www.zigbang.com/home/apt/items/{item_id}"

        row = {
            "수집시간": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "단지": i.get("areaDanjiName"),
            "구": i.get("local2"),
            "동": i.get("local3"),
            "거래유형": i.get("tranType"),
            "동번호": i.get("dong"),
            "층": i.get("floor"),
            "면적_계약": i.get("sizeContractM2"),
            "면적_전용": size_m2,
            "전용평": size_py,
            "타입_m2": room.get("m2"),
            "타입_평": room.get("p"),
            "보증금": deposit,
            "월세": rent,
            "가격(억)": price_eok,
            "제목": i.get("itemTitle"),
            "매물ID": item_id,
            "매물URL": item_url
        }

        all_rows.append(row)

    params["offset"] += params["limit"]

    if params["offset"] >= data.get("totalCount", 0):
        break

    time.sleep(0.5)


file_name = f"helio_zigbang_{datetime.now().strftime('%Y%m%d')}.csv"

with open(file_name, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
    writer.writeheader()
    writer.writerows(all_rows)

print("저장 완료:", file_name)