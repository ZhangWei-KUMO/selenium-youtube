import time
import random
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service  # For specifying driver path
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


def scrape_nccu_lib_dynamic(max_pages=75, driver_path=None):
    """
    使用 Selenium 從動態加載的國立政治大學圖書館搜尋結果頁面爬取表格數據。

    Args:
        max_pages (int): 要爬取的最大頁數。
        driver_path (str, optional): WebDriver 的路徑。
                                     如果為 None，Selenium 會嘗試從系統 PATH 找。
                                     建議明確提供路徑，例如 'path/to/chromedriver.exe'。

    Returns:
        pandas.DataFrame: 包含所有爬取數據的 DataFrame。
                         如果爬取失敗或找不到數據，則返回空的 DataFrame。
    """
    base_url = "https://gpost.lib.nccu.edu.tw/display.php?&q=%E8%83%A1&pagenumber=100&order=default&orderype=asc&tpl=rough&page="
    all_data = []
    headers_list = []
    target_table_id = "searchresult_tb"  # 目標表格 ID

    # --- 設定 Selenium WebDriver ---
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')  # (可選) 無頭模式，不在螢幕上顯示瀏覽器視窗
    options.add_argument("--disable-gpu")  # 在某些系統上 headless 模式需要
    options.add_argument("--log-level=3")  # 減少 Selenium 的控制台輸出
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )  # 設定 User Agent

    if driver_path:
        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    else:
        # 嘗試讓 Selenium 自動管理或從 PATH 查找
        print("未指定 driver_path，將嘗試自動查找 WebDriver...")
        driver = webdriver.Chrome(options=options)

    print(f"開始使用 Selenium 爬取，總共 {max_pages} 頁...")

    try:
        for page_num in range(1, max_pages + 1):
            url = f"{base_url}{page_num}"
            print(f"正在加載第 {page_num}/{max_pages} 頁: {url}")

            try:
                driver.get(url)

                # --- 等待目標表格加載完成 ---
                # 這是關鍵步驟！等待最多 20 秒，直到 id 為 target_table_id 的元素出現
                wait = WebDriverWait(driver, 20)
                wait.until(EC.presence_of_element_located((By.ID, target_table_id)))
                print(f"第 {page_num} 頁表格 '{target_table_id}' 已加載。")

                # 獲取頁面原始碼 (現在應該包含動態加載的表格了)
                page_source = driver.page_source

                # 使用 BeautifulSoup 解析渲染後的 HTML
                soup = BeautifulSoup(page_source, "lxml")

                # 找到目標表格
                table = soup.find("table", id=target_table_id)

                if not table:
                    # 理論上 WebDriverWait 會確保表格存在，但加個保險
                    print(
                        f"警告：在第 {page_num} 頁雖然等到元素，但 BeautifulSoup 未解析到表格。"
                    )
                    continue

                # --- 提取表頭 (僅在第一頁且表頭列表為空時執行) ---
                if page_num == 1 and not headers_list:
                    header_row = table.find("thead")
                    if not header_row:
                        header_row = table.find("tr")
                    if header_row:
                        headers_list = [
                            th.get_text(strip=True)
                            for th in header_row.find_all(["th", "td"])
                        ]
                        print(f"提取到的表頭: {headers_list}")
                    else:
                        print("警告：無法在第一頁找到表頭。")

                # --- 提取表格數據行 ---
                tbody = table.find("tbody")
                rows = tbody.find_all("tr") if tbody else table.find_all("tr")
                # 如果表頭在第一個 tr 且 headers_list 已被提取，需要跳過
                if headers_list and not tbody and rows:
                    rows = rows[1:]

                if not rows:
                    print(f"警告：在第 {page_num} 頁的表格中找不到數據行 (tr)。")
                    continue

                for row in rows:
                    cols = [td.get_text(strip=True) for td in row.find_all("td")]
                    if cols:
                        all_data.append(cols)

                # --- 添加隨機延遲 ---
                sleep_time = random.uniform(
                    1.0, 2.5
                )  # Selenium 操作通常慢些，延遲可以稍長
                print(f"完成第 {page_num} 頁，休息 {sleep_time:.2f} 秒...")
                time.sleep(sleep_time)

            except TimeoutException:
                print(
                    f"錯誤：在第 {page_num} 頁等待表格 '{target_table_id}' 超時。可能該頁無數據或網頁結構改變。"
                )
                # 可以選擇跳過 (continue) 或停止 (break)
                continue
            except NoSuchElementException:
                print(
                    f"錯誤：在第 {page_num} 頁找不到元素。 WebDriverWait 後續可能出錯。"
                )
                continue
            except Exception as e:
                print(f"處理第 {page_num} 頁時發生未知錯誤: {e}")
                continue

    finally:
        # --- 確保瀏覽器關閉 ---
        print("爬取結束或遇到錯誤，關閉瀏覽器...")
        driver.quit()

    print(f"爬取完成！總共提取了 {len(all_data)} 筆數據。")

    # --- 將數據轉換為 Pandas DataFrame ---
    if all_data:
        if headers_list and len(all_data[0]) == len(headers_list):
            df = pd.DataFrame(all_data, columns=headers_list)
        else:
            print(
                "警告：提取到的表頭數量與數據列數不匹配，或未找到表頭。將使用預設數字索引作為列名。"
            )
            df = pd.DataFrame(all_data)
    else:
        print("未提取到任何數據。")
        df = pd.DataFrame()

    return df


# --- 主程式執行部分 ---
if __name__ == "__main__":
    # 設定要爬取的總頁數
    total_pages_to_scrape = 75

    # --- 設定你的 WebDriver 路徑 ---
    # Windows 範例: "C:/path/to/chromedriver.exe" (注意使用 / 或 \\)
    # Linux/Mac 範例: "/path/to/chromedriver"
    # 如果 WebDriver 在腳本同目錄或 PATH 中，可以設為 None，但建議明確指定
    webdriver_executable_path = (
        None  # <-- 在這裡填寫你的 chromedriver 路徑! 例如: 'chromedriver.exe'
    )

    if webdriver_executable_path is None:
        print("警告：未在程式碼中指定 WebDriver 路徑。")
        print(
            "請確保 chromedriver(.exe) 在您的系統 PATH 中，或者在程式碼中設置 'webdriver_executable_path' 變數。"
        )
        # 你也可以在這裡直接提示用戶輸入路徑
        # webdriver_executable_path = input("請輸入 ChromeDriver 的完整路徑: ")

    # 執行爬蟲函式
    scraped_df = scrape_nccu_lib_dynamic(
        max_pages=total_pages_to_scrape, driver_path=webdriver_executable_path
    )

    # 顯示 DataFrame 的前幾行和基本資訊
    if not scraped_df.empty:
        print("\n--- 爬取結果預覽 (前 5 筆) ---")
        print(scraped_df.head())

        print("\n--- DataFrame 資訊 ---")
        scraped_df.info()

        # --- (可選) 將 DataFrame 存儲為 CSV 文件 ---
        try:
            output_filename = "nccu_library_scrape_胡_dynamic.csv"
            scraped_df.to_csv(output_filename, index=False, encoding="utf-8-sig")
            print(f"\n數據已成功保存到文件: {output_filename}")
        except Exception as e:
            print(f"\n保存 CSV 文件時出錯: {e}")
    else:
        print("\n爬蟲未獲取到數據，無法生成預覽或保存文件。")
