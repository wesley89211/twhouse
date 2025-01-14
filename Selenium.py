from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
import csv
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests

# 初始化瀏覽器
driver = webdriver.Chrome()  # 確保已安裝 ChromeDriver
url = "https://market.591.com.tw/list?regionId=6&sort=1"  # 替換為你的目標網站 URL
driver.get(url)
time.sleep(2)  # 等待初始頁面加載
try:
    # 1. 點擊提示框的「了解」按鈕
    guide_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//button[@class="guide-button"]'))
    )
    guide_button.click()
    print("已點擊『了解』按鈕")
    time.sleep(2)

    # 2. 點擊「更多」按鈕展開篩選框
    more_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//div[@class="t5-dropdown-toggle"]//span[text()="更多(2)"]'))
    )
    more_button.click()
    print("已點擊『更多』按鈕")
    time.sleep(2)

    # 勾選「不限」checkbox
    unlimited_option = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//li[@id="moreFilterItemEle10"]//p[text()="不限"]'))
    )
    unlimited_option.click()
    print("已成功勾選『不限』checkbox")
    time.sleep(2)

    confirm_button = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, '(//section[@class="grid-filter-btn"]//button)[2]'))
    )
    confirm_button.click()
    print("已點擊『確定』按鈕")


except Exception as e:
    print(f"操作失敗: {e}")


# 滾動到頁面底部
def scroll_to_bottom(driver, pause_time=2, max_scrolls=10):
    last_height = driver.execute_script("return document.body.scrollHeight")
    scrolls = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause_time)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        scrolls += 1
        if scrolls >= max_scrolls:
            break

scroll_to_bottom(driver)

# 保存滾動後的 HTML
html = driver.page_source
with open("page_after_scroll.html", "w", encoding="utf-8") as file:
    file.write(html)
print("滾動後的 HTML 已保存")

# 提取數據（假設通過 API 提取更多數據）
url = "https://bff-market.591.com.tw/v1/search/list?page=3&regionid=6&sort=1&from=3"  # 替換為 API 地址
headers = {"User-Agent": "Mozilla/5.0"}
all_data = []
for page in range(1, 5):  # 假設最多 4 頁
    params = {"regionId": 6, "sort": 1, "page": page}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        all_data.extend(data["items"])  # 替換為 API 返回的數據鍵
    else:
        print(f"第 {page} 頁請求失敗")

print(f"共抓取到 {len(all_data)} 條數據")
driver.quit()
'''
# 保存到 CSV 文件
with open('locations.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=["路段", "詳細地址", "地段標籤"])
    writer.writeheader()
    writer.writerows(locations)

print("地段數據已保存到 locations.csv")
'''
