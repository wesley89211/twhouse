import pandas as pd

# 文件名稱
input_file = "unique_housing_data.csv"
output_file = "processed_housing_data.csv"

# 讀取 CSV 文件
df = pd.read_csv(input_file, encoding="utf-8")

# 清理價格欄位
df["價格"] = (
    df["價格"]
    .str.replace("萬/坪", "", regex=False)  # 移除 "萬/坪"
    .str.strip()                           # 移除前後空格
    .replace("", "0")                      # 將空字串替換為 "0"
    .astype(float)                         # 轉換為浮點數
)

# 檢查結果
print("處理後的價格欄位（前 10 筆）：")
print(df[["價格"]].head(10))

# 保存處理後的數據到新文件
df.to_csv(output_file, index=False, encoding="utf-8")
print(f"處理後的數據已保存到 {output_file}")
