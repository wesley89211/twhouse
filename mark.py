import pandas as pd

# 檔案名稱
unique_housing_file = "unique_housing_data.csv"
all_housing_file = "allhousing_data.csv"
output_file = "updated_unique_housing_data.csv"

# 讀取 CSV 文件
unique_housing_data = pd.read_csv(unique_housing_file)
all_housing_data = pd.read_csv(all_housing_file)

# 確保 ID 欄位名稱一致
unique_housing_data['ID'] = unique_housing_data['ID'].astype(str)
all_housing_data['ID'] = all_housing_data['ID'].astype(str)

# 計算在 allhousing_data.csv 中每個 ID 出現的次數
id_counts = all_housing_data['ID'].value_counts()

# 新增 layer_two 欄位
unique_housing_data['layer_two'] = unique_housing_data['ID'].apply(lambda x: 1 if id_counts.get(x, 0) > 0 else 0)

# 保存結果
unique_housing_data.to_csv(output_file, index=False, encoding='utf-8')

print(f"更新完成，結果已保存到 {output_file}")
