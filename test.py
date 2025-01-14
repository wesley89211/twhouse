import pandas as pd

# 文件名稱
input_file = "all_region_housing_data_utf8.csv"
output_file = "unique_housing_data.csv"

# 讀取 CSV 文件
df = pd.read_csv(input_file, encoding="utf-8")

# 檢查初始總筆數
initial_count = len(df)
print(f"初始資料共有 {initial_count} 筆")

# 刪除重複資料（以 ID 欄位為基準）
df_unique = df.drop_duplicates(subset="ID", keep="first")

# 檢查刪除後的總筆數
final_count = len(df_unique)
print(f"刪除重複資料後，共剩下 {final_count} 筆土地資訊")

# 保存去重後的資料到新文件
df_unique.to_csv(output_file, index=False, encoding="utf-8")
print(f"去重後的資料已保存到 {output_file}")
