from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
import csv
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests

#url = "https://bff-market.591.com.tw/v1/search/list?page=3&regionid=6&sort=1&from=3"  # 替換為實際 API 地址

# 正確的 API 地址（從瀏覽器中獲取）
url = "https://bff-market.591.com.tw/v1/search/list?"

# Headers（模擬瀏覽器）
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://bff-market.591.com.tw/",  # 替換為來源頁面
    "Accept": "application/json",
}

# 查詢參數（從開發者工具中獲取完整參數）
params = {
    "regionid":6,  # 地區桃園其他地區看文件說明
    "from":3,      # 來源
    "sort":1,      # 排序單價低到高排序
    "page":1       # 頁碼
}

# 存放所有數據
all_data = []

# 模擬多頁請求，動態判斷
page = 1
while True:
    params["page"] = page
    response = requests.get(url, headers=headers, params=params)

    print(f"請求 URL: {response.url}")  # 調試：打印請求 URL

    if response.status_code == 200:
        data = response.json()
        try:
            items = data["data"]["items"]
            if not items:  # 如果數據為空，跳出迴圈
                print("已抓取所有數據")
                break

            # 提取數據
            for item in items:
                parsed_item = {
                    "ID": item["id"],
                    "名稱": item["name"],
                    "地區": item["region"],
                    "區域": item["section"],
                    "地址": item["simple_address"],
                    "類型": item["housing_type_str"],
                    "價格": f"{item['price']['price']} {item['price']['unit']}",
                    "用途": item["build_purpose_simple"],
                    "標籤": item["housing_text"]
                }
                all_data.append(parsed_item)
        except KeyError as e:
            print(f"數據解析錯誤: {e}")
            break
    else:
        print(f"第 {page} 頁請求失敗，狀態碼: {response.status_code}")
        break

    page += 1  # 進入下一頁

print(f"共抓取到 {len(all_data)} 條數據")

# 保存數據到 CSV
csv_filename = "housing_data.csv"
with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["ID", "名稱", "地區", "區域", "地址", "類型", "價格", "用途", "標籤"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()  # 寫入表頭
    writer.writerows(all_data)  # 寫入數據

print(f"數據已保存到 {csv_filename}")
