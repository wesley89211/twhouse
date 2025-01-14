import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

# 設置 WebDriver 路徑
webdriver_path = 'C:/Users/林翰緯/chromedriver-win64/chromedriver.exe'  # 替換為您的 ChromeDriver 路徑
service = Service(webdriver_path)

# 第一層 CSV 文件名
first_layer_csv = "unique_housing_data.csv"

# 第二層保存的 CSV 文件名
second_layer_csv = "allhousing_data.csv"

# 初始化 WebDriver
def init_driver():
    return webdriver.Chrome(service=service)

# 讀取第一層數據
def load_first_layer_data(csv_file):
    data = {}
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            region = row["地區"]  # 假設第一層 CSV 包含 "地區" 和 "ID" 欄位
            item_id = row["ID"]
            if region not in data:
                data[region] = []
            data[region].append(item_id)
    return data

# 保存數據到 CSV
def save_to_csv(data, filename):
    file_exists = False
    try:
        with open(filename, "r", encoding="utf-8") as f:
            file_exists = True
    except FileNotFoundError:
        pass

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ID", "成交年月", "樓層", "幾房", "坪數", "單價", "總價", "車位價格", "車位坪數", "地址"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)

# 過濾空值超過三個的行
def filter_valid_rows(rows):
    valid_rows = []
    for row in rows:
        empty_count = sum(1 for key in row if row[key] == "N/A" or not row[key].strip())
        if empty_count <= 3:  # 如果空值數量小於等於 3，保留該行
            valid_rows.append(row)
    return valid_rows

# 抓取第二層數據
def fetch_details(driver, item_id):
    url = f"https://market.591.com.tw/{item_id}/price"
    driver.get(url)
    time.sleep(3)  # 等待頁面加載

    transactions = []
    try:
        # 提取數據
        area_elements = driver.find_elements(By.CSS_SELECTOR, "div.row-ctn.area")
        total_elements = driver.find_elements(By.CSS_SELECTOR, "div.none.total span")
        parking_elements_price = driver.find_elements(By.CSS_SELECTOR, "div.park .row-ctn")
        parking_elements_area = driver.find_elements(By.CSS_SELECTOR, "div.park .area")
        address_elements = driver.find_elements(By.CSS_SELECTOR, "address")
        date_elements = driver.find_elements(By.CSS_SELECTOR, "div.year-month span")
        floor_elements = driver.find_elements(By.CSS_SELECTOR, "div.floor span")
        price_elements = driver.find_elements(By.CSS_SELECTOR, "div.price .price-detail")

        for i in range(len(area_elements)):
            房數, 坪數 = ("N/A", "N/A")
            if "/" in area_elements[i].text:
                房數, 坪數 = area_elements[i].text.split("/")
                房數 = 房數.strip()
                坪數 = 坪數.replace("坪", "").strip()

            transaction = {
                "ID": item_id,  # 增加 ID 欄位
                "成交年月": date_elements[i].text if i < len(date_elements) else "N/A",
                "樓層": floor_elements[i].text if i < len(floor_elements) else "N/A",
                "幾房": 房數,
                "坪數": 坪數,
                "單價": price_elements[i].text.replace("萬/坪", "").strip() if i < len(price_elements) else "N/A",
                "總價": total_elements[i].text.replace("萬", "").strip() if i < len(total_elements) else "N/A",
                "車位價格": parking_elements_price[i].text.replace("萬", "").strip() if i < len(parking_elements_price) else "N/A",
                "車位坪數": parking_elements_area[i].text if i < len(parking_elements_area) else "無車位",
                "地址": address_elements[i].text if i < len(address_elements) else "N/A",
            }
            transactions.append(transaction)

        print(f"成功抓取 ID: {item_id} - 共 {len(transactions)} 筆數據")
    except Exception as e:
        print(f"抓取 ID: {item_id} 時發生錯誤: {e}")

    return transactions

# 多線程抓取
def fetch_all(region_data):
    all_data = []
    count = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        drivers = [init_driver() for _ in range(5)]  # 初始化 5 個 WebDriver
        for region, region_ids in region_data.items():
            for idx, item_id in enumerate(region_ids):
                futures.append(executor.submit(fetch_details, drivers[idx % 5], item_id))

        for future in as_completed(futures):
            try:
                details = future.result()
                valid_details = filter_valid_rows(details)  # 過濾空值超過 3 個的行
                all_data.extend(valid_details)
                count += len(valid_details)

                # 每 1000 筆數據保存一次
                if count >= 1000:
                    save_to_csv(all_data, second_layer_csv)
                    print(f"已保存 {count} 筆數據到 {second_layer_csv}")
                    all_data = []  # 清空已保存的數據
                    count = 0

            except Exception as e:
                print(f"抓取數據時發生錯誤: {e}")

    # 保存剩餘數據
    if all_data:
        save_to_csv(all_data, second_layer_csv)
        print(f"保存剩餘數據，共 {len(all_data)} 筆")

# 主函數
def main():
    # 載入第一層數據
    region_data = load_first_layer_data(first_layer_csv)
    print(f"開始抓取所有地區的數據...")

    # 多線程抓取
    fetch_all(region_data)
    print("所有地區數據抓取完成")

if __name__ == "__main__":
    main()
