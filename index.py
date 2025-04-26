import pandas as pd
import numpy as np  # 用於處理可能的 NaN 值

# --- 設定參數 ---
# 輸入的 CSV 檔案路徑 (爬蟲輸出的檔案)
input_csv_path = "nccu_library_scrape_胡_dynamic.csv"  # 請確保這是你爬蟲輸出的正確檔名

# 輸出的 CSV 檔案路徑 (合併後的結果)
output_csv_path = "nccu_library_merged_resumes_final.csv"

# --- 根據你提供的表頭設定欄位名稱 ---
name_column = "姓名Name"

# 用於組合單個履歷條目的欄位 (按希望的順序排列)
# 我們將把這些欄位的內容格式化成一個字串代表單次經歷
entry_component_columns = [
    "命令日期Release date",
    "機關 | 官/職等 | 職務Organization | Rank/Grade | Capacity",
    "原因Reason",
    "類別Category",  # 也可以加入類別和狀態
    "狀態Situation",
]

# 合併後代表完整履歷的欄位名稱
merged_resume_column_name = "合併經歷Entries"

# 合併履歷條目時使用的分隔符號
separator = "\n------------------------------\n"  # 使用換行和較長的分隔線

# --- 資料處理主程式 ---

print(f"正在讀取輸入文件: {input_csv_path}")

try:
    # 讀取 CSV 文件，嘗試使用 utf-8-sig 編碼
    df = pd.read_csv(input_csv_path, encoding="utf-8-sig")
    print("文件讀取成功。")
    # print("\n原始 DataFrame 的前幾行:")
    # print(df.head())
    print("\n原始 DataFrame 的欄位名稱:")
    print(df.columns.tolist())

    # *** 檢查必要的欄位是否存在 ***
    required_columns = [name_column] + entry_component_columns
    actual_columns = df.columns.tolist()
    missing_cols = [col for col in required_columns if col not in actual_columns]

    if name_column not in actual_columns:
        print(f"\n錯誤：找不到姓名欄位 '{name_column}'。請檢查設定。")
        exit()
    if not all(col in actual_columns for col in entry_component_columns):
        print(
            f"\n警告：以下用於組合經歷的欄位部分或全部缺失，將僅使用存在的欄位: {missing_cols}"
        )
        # 更新實際存在的欄位列表
        entry_component_columns = [
            col for col in entry_component_columns if col in actual_columns
        ]
        if not entry_component_columns:
            print(
                f"\n錯誤：完全找不到任何可以用於組合經歷的欄位 ({entry_component_columns})。"
            )
            exit()

    # --- 預處理 ---
    # 1. 清理姓名欄位的前後空白，並移除姓名為空的行
    df[name_column] = df[name_column].str.strip()
    df = df.dropna(subset=[name_column])
    df = df[df[name_column] != ""]

    # 2. 將用於組合的欄位轉為字串，填充 NaN 為空字串
    for col in entry_component_columns:
        df[col] = df[col].fillna("").astype(str)

    # --- 組合單條經歷描述 ---
    # 定義一個函數來格式化每一行的經歷
    def format_entry(row):
        entry_parts = []
        # 根據 entry_component_columns 的順序添加非空內容
        date_val = row.get("命令日期Release date", "").strip()
        org_val = row.get(
            "機關 | 官/職等 | 職務Organization | Rank/Grade | Capacity", ""
        ).strip()
        reason_val = row.get("原因Reason", "").strip()
        category_val = row.get("類別Category", "").strip()
        situation_val = row.get("狀態Situation", "").strip()

        if date_val:
            entry_parts.append(f"日期: {date_val}")
        if category_val:
            entry_parts.append(f"類別: {category_val}")
        if situation_val:
            entry_parts.append(f"狀態: {situation_val}")
        if org_val:
            # 這個欄位名太長，用簡稱
            entry_parts.append(f"機關/職務: {org_val}")
        if reason_val:
            entry_parts.append(f"原因: {reason_val}")

        # 使用換行連接每個部分，如果只有一個部分則不加換行
        return "\n".join(entry_parts) if len(entry_parts) > 1 else "".join(entry_parts)

    print("\n正在為每一行創建組合的經歷描述...")
    # 應用函數到每一行，創建一個新的臨時欄位 '_CombinedEntry'
    df["_CombinedEntry"] = df.apply(format_entry, axis=1)

    # 顯示一些組合後的條目範例
    # print("\n組合後的單條經歷範例:")
    # print(df[['姓名Name', '_CombinedEntry']].head())

    # --- 分組與合併 ---
    print(f"\n將根據 '{name_column}' 欄位進行分組...")
    print(f"將合併組合後的經歷條目，使用分隔符...")

    # 定義合併函數：將同組內的履歷條目用指定分隔符連接起來
    # 過濾掉完全空的條目
    def merge_formatted_resumes(series):
        valid_entries = [entry for entry in series if entry and not entry.isspace()]
        return separator.join(valid_entries)

    # 執行分組和聚合
    # 只聚合我們新創建的 _CombinedEntry 欄位
    merged_df = df.groupby(name_column, as_index=False).agg(
        **{merged_resume_column_name: ("_CombinedEntry", merge_formatted_resumes)}
        # 使用 Python 3.5+ 的 kwargs 語法來動態命名聚合後的欄位
        # 等同於 {'合併經歷Entries': ('_CombinedEntry', merge_formatted_resumes)}
    )

    print("\n--- 合併後的結果預覽 (前 5 筆) ---")
    # 為了更好的預覽，可以調整 Pandas 的顯示選項 (可選)
    # pd.set_option('display.max_colwidth', 200) # 顯示更多欄位內容
    print(merged_df.head())
    print(f"\n合併完成，共得到 {len(merged_df)} 筆獨立的姓名記錄。")

    # --- 保存結果 ---
    try:
        merged_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
        print(f"\n合併後的數據已成功保存到文件: {output_csv_path}")
    except Exception as e:
        print(f"\n保存合併後的 CSV 文件時出錯: {e}")

except FileNotFoundError:
    print(
        f"錯誤：找不到輸入的 CSV 文件 '{input_csv_path}'。請確保文件存在於正確的路徑。"
    )
except pd.errors.EmptyDataError:
    print(f"錯誤：輸入的 CSV 文件 '{input_csv_path}' 是空的。")
except KeyError as e:
    print(
        f"\n錯誤：程式碼中使用的欄位名稱 '{e}' 在您的 CSV 文件中不存在。請仔細檢查 'name_column' 和 'entry_component_columns' 的設定。"
    )
except Exception as e:
    print(f"處理過程中發生未預期的錯誤: {e}")
