import pandas as pd
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定檔案名稱
unique_housing_file = "updated_unique_housing_data.csv"
second_layer_output = "allhousing_data.csv"
webdriver_path = 'C:/Users/林翰緯/chromedriver-win64/chromedriver.exe'  # 替換為您的 ChromeDriver 路徑
service = Service(webdriver_path)

# 初始化 WebDriver
def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # 啟用無頭模式
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-images")  # 禁用圖片
    options.add_argument("--disable-extensions")  # 禁用擴展
    options.add_argument("--disable-javascript")  # 禁用 JavaScript
    options.add_argument("--disable-plugins-discovery")
    options.add_argument("--disable-infobars")
    return webdriver.Chrome(service=service, options=options)

# 安全提取元素
def safe_find_elements(driver, selector):
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector)))
        return driver.find_elements(By.CSS_SELECTOR, selector)
    except Exception:
        return []

# 抓取第二層數據的函數
def fetch_details(driver, item_id):
    url = f"https://market.591.com.tw/{item_id}/price"
    driver.get(url)
    time.sleep(random.uniform(1, 2))  # 動態等待

    transactions = []
    try:
        area_elements = safe_find_elements(driver, "div.row-ctn.area")
        total_elements = safe_find_elements(driver, "div.none.total span")
        parking_elements_price = safe_find_elements(driver, "div.park .row-ctn")
        parking_elements_area = safe_find_elements(driver, "div.park .area")
        address_elements = safe_find_elements(driver, "address")
        date_elements = safe_find_elements(driver, "div.year-month span")
        floor_elements = safe_find_elements(driver, "div.floor span")
        price_elements = safe_find_elements(driver, "div.price .price-detail")

        for i in range(len(area_elements)):
            房數, 坪數 = ("N/A", "N/A")
            if "/" in area_elements[i].text:
                房數, 坪數 = area_elements[i].text.split("/")
                房數 = str(房數).strip()
                坪數 = str(坪數).replace("坪", "").strip()

            transaction = {
                "ID": item_id,
                "成交年月": str(date_elements[i].text).strip() if i < len(date_elements) else "N/A",
                "樓層": str(floor_elements[i].text).strip() if i < len(floor_elements) else "N/A",
                "幾房": 房數,
                "坪數": 坪數,
                "單價": str(price_elements[i].text).replace("萬/坪", "").strip() if i < len(price_elements) else "N/A",
                "總價": str(total_elements[i].text).replace("萬", "").strip() if i < len(total_elements) else "N/A",
                "車位價格": str(parking_elements_price[i].text).replace("萬", "").strip() if i < len(parking_elements_price) else "N/A",
                "車位坪數": str(parking_elements_area[i].text).strip() if i < len(parking_elements_area) else "無車位",
                "地址": str(address_elements[i].text).strip() if i < len(address_elements) else "N/A",
            }

            # 過濾空值超過 3 個的資料行
            empty_count = sum(1 for value in transaction.values() if value == "N/A" or not str(value).strip())
            if empty_count <= 3:
                transactions.append(transaction)

        print(f"成功抓取 ID: {item_id} - 共 {len(transactions)} 筆有效數據")
    except Exception as e:
        print(f"抓取 ID: {item_id} 時發生錯誤: {e}")

    return transactions

# 主抓取函數
def fetch_all(data):
    all_data = []
    count = 0

    # 使用多線程抓取
    with ThreadPoolExecutor(max_workers=10) as executor:  # 增加並行數量
        drivers = [init_driver() for _ in range(10)]
        futures = [
            executor.submit(fetch_details, drivers[idx % 10], row["ID"])
            for idx, row in data.iterrows()
        ]

        for future in as_completed(futures):
            try:
                details = future.result()
                all_data.extend(details)
                count += len(details)

                # 每 1000 筆數據保存一次
                if count >= 1000:
                    pd.DataFrame(all_data).to_csv(second_layer_output, index=False, mode="a", header=not bool(pd.read_csv(second_layer_output, nrows=1)), encoding="utf-8")
                    print(f"已保存 {count} 筆數據到 {second_layer_output}")
                    all_data = []  # 清空已保存的數據
                    count = 0
            except Exception as e:
                print(f"抓取過程中發生錯誤: {e}")

    # 保存剩餘數據
    if all_data:
        pd.DataFrame(all_data).to_csv(second_layer_output, index=False, mode="a", header=not bool(pd.read_csv(second_layer_output, nrows=1)), encoding="utf-8")
        print(f"保存剩餘數據，共 {len(all_data)} 筆")

# 主函數
def main():
    unique_housing_data = pd.read_csv(unique_housing_file)

    # 過濾目標地區資料（範例：基隆市、新北市，且 layer_two = 0）
    target_data = unique_housing_data[
        (unique_housing_data["地區"].isin(["基隆市", "新北市"])) & (unique_housing_data["layer_two"] == 0)
    ]

    print(f"共找到 {len(target_data)} 筆待抓取資料")
    if target_data.empty:
        print("沒有符合條件的資料需要抓取")
        return

    # 抓取資料
    fetch_all(target_data)

if __name__ == "__main__":
    main()