import requests
import csv
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor
import os
from tqdm import tqdm

# API 地址
url = "https://bff-market.591.com.tw/v1/search/list?"

# Headers
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://bff-market.591.com.tw/",
    "Accept": "application/json",
}

# 地區參數
region_ids = [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 17, 19, 21, 22, 23, 24, 25, 26]


# 查詢參數基礎
base_params = {
    "from": 3,
    "sort": 1,
    "page": 1,
}

# 共享數據結構與鎖
data_lock = threading.Lock()
csv_utf8_filename = "all_region_housing_data_utf8.csv"
csv_utf8_sig_filename = "all_region_housing_data_utf8_sig.csv"
fieldnames = ["ID", "名稱", "地區", "區域", "地址", "類型", "價格", "用途", "標籤"]

# 保存數據到 CSV
def save_to_csv(batch_data, encoding, filename):
    file_exists = os.path.exists(filename)
    with open(filename, "a", newline="", encoding=encoding) as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()  # 文件不存在時寫入表頭
        writer.writerows(batch_data)
    print(f"{len(batch_data)} 筆數據已保存到 {filename}（編碼：{encoding}）")

# 抓取函數
def fetch_region(region_id):
    page = 1
    empty_count = 0
    local_data = []
    while empty_count < 2:
        params = base_params.copy()
        params["regionid"] = region_id
        params["page"] = page

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                items = data.get("data", {}).get("items", [])
                if not items:
                    empty_count += 1
                else:
                    empty_count = 0
                    local_data.extend([
                        {
                            "ID": item["id"],
                            "名稱": item["name"],
                            "地區": item["region"],
                            "區域": item["section"],
                            "地址": item["simple_address"],
                            "類型": item["housing_type_str"],
                            "價格": f"{item['price']['price']} {item['price']['unit']}",
                            "用途": item["build_purpose_simple"],
                            "標籤": item["housing_text"],
                        }
                        for item in items
                    ])
                print(f"地區 {region_id} 第 {page} 頁抓取成功，獲取 {len(items)} 筆數據")

                # 每 1000 筆數據保存一次
                if len(local_data) >= 1000:
                    with data_lock:
                        save_to_csv(local_data, "utf-8", csv_utf8_filename)
                        save_to_csv(local_data, "utf-8-sig", csv_utf8_sig_filename)
                    local_data = []  # 清空當前批次數據
            else:
                print(f"地區 {region_id} 第 {page} 頁請求失敗，狀態碼: {response.status_code}")
                break
        except Exception as e:
            print(f"地區 {region_id} 第 {page} 頁請求出錯: {e}")
            break

        # 遞增頁碼並添加隨機延遲
        page += 1


    # 保存剩餘數據
    if local_data:
        with data_lock:
            save_to_csv(local_data, "utf-8", csv_utf8_filename)
            save_to_csv(local_data, "utf-8-sig", csv_utf8_sig_filename)

# 按批次執行抓取
region_groups = [region_ids[i:i+4] for i in range(0, len(region_ids), 4)]
for group in tqdm(region_groups, desc="抓取進度"):
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(fetch_region, group)
    print(f"地區組 {group} 抓取完成，等待 60 秒繼續")
    time.sleep(60)

print("所有數據抓取完成")
