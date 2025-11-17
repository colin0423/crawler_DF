###############################
# combined crawler
# 誘卵桶 + 天氣資料
###############################

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import urllib.parse
import requests
import time
import os
import pandas as pd
import numpy as np
from datetime import datetime,timedelta

###############################
# 0. 初始化 driver（共用）
###############################
def init_driver(path):
    download_dir = os.path.abspath(path)
    os.makedirs(download_dir, exist_ok=True)
    print("📂 下載路徑：", download_dir)
    chrome_prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
    }

    opts = Options()
    opts.add_argument("--headless=new")           # 背景靜默
    opts.add_argument("--window-size=1920,1080")  # 視窗開大，版面比較正常
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("prefs", chrome_prefs)
    driver = webdriver.Chrome(options=opts)
    wait = WebDriverWait(driver, 10)
    return driver, wait , download_dir


###########################################
# 1️⃣ 誘卵桶資料爬蟲
###########################################
def crawl_bucket(driver, wait,download_dir, year_title="114年臺南市登革熱誘卵桶監測資訊", file_name="bucket_114.csv"):
    print("\n====== 🪣 誘卵桶資料爬取 ======")

    driver.get("https://data.tainan.gov.tw/DataSet/Detail/33a5bbc9-6898-4851-9147-4410f0b2f47e")

    # 找該年度
    link_elem = wait.until(
        EC.presence_of_element_located((
            By.XPATH,
            f'//a[@title="{year_title}"]'
        ))
    )
    link_elem.click()
    print(f"✅ 已成功點進 {year_title}")

    # 抓 CSV
    csv_elem = wait.until(
        EC.presence_of_element_located((
            By.XPATH,
            '//a[contains(text(), "CSV")]'
        ))
    )

    href = csv_elem.get_attribute("href")
    full_url = urllib.parse.urljoin("https://data.tainan.gov.tw", href)

    print("🔗 CSV 下載連結：", full_url)

    save_path = os.path.join(download_dir, file_name)

    res = requests.get(full_url, headers={"User-Agent": "Mozilla/5.0"})
    res.raise_for_status()

    with open(save_path, "wb") as f:
        f.write(res.content)

    print("📁 已下載：", save_path)
    print("🪣 誘卵桶下載完畢\n")


###########################################
# 2️⃣ 天氣資料爬蟲
###########################################
def normalize_weather_filename(download_dir, target_filename):
    """
    將自動加 (1)、(2)… 的檔案統一改成 target_filename
    """
    files = os.listdir(download_dir)

    # 找出所有像 "467410-2025-11" 開頭的檔案
    candidates = [f for f in files if f.startswith(target_filename.split(".")[0])]

    if not candidates:
        print("⚠ 找不到任何天氣資料檔案")
        return None

    # 按照修改時間排序，最新的排最後
    candidates = sorted(
        candidates,
        key=lambda f: os.path.getmtime(os.path.join(download_dir, f))
    )

    newest_file = candidates[-1]  # 最新的

    src = os.path.join(download_dir, newest_file)
    dst = os.path.join(download_dir, target_filename)

    # 若 dst 已存在 → 刪掉
    if os.path.exists(dst):
        os.remove(dst)

    os.rename(src, dst)
    print(f"📁 已將最新下載檔案：{newest_file} → {target_filename}")

    return dst

def crawl_weather(driver, wait, download_dir,station="467410", month="2024-10"):



    print("\n====== 🌦️ 天氣資料爬取 ======")

    driver.get("https://codis.cwa.gov.tw/StationData")

    # 切到測站清單
    btn_list = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[contains(text(),"測站清單")]')))
    btn_list.click()

    # 找表格
    tbody = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody")))
    scroll_container = tbody

    # 找 chart icon
    icon = None
    for step in range(40):
        matches = tbody.find_elements(
            By.XPATH,
            (
                f'.//tr[.//td[contains(normalize-space(.), "{station}")]]'
                f'//i[contains(@class,"fa-chart-line")]'
            )
        )
        if matches:
            icon = matches[0]
            driver.execute_script("arguments[0].scrollIntoView(false);", icon)
            driver.execute_script("window.scrollBy(0,200);")
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", icon)
            print(f"✅ 點擊測站 {station}")
            break

        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollTop + 400;",
            scroll_container
        )
        time.sleep(0.5)

    if icon is None:
        raise RuntimeError("❌ 找不到該站碼，可能不存在")

    # 切月報表
    month_tab = wait.until(
        EC.element_to_be_clickable((
            By.XPATH,
            '//div[@class="lightbox-tool-menu"]/div[.//div[contains(text(),"月報表(逐日資料)")]]'
        ))
    )
    driver.execute_script("arguments[0].click();", month_tab)
    print("✅ 已切到『月報表(逐日資料)』")

    time.sleep(0.5)

    # 找 lightbox
    lightbox = wait.until(
        EC.presence_of_element_located((
            By.XPATH,
            '//section[contains(@class,"lightbox-tool") and .//label[contains(text(),"測站時序圖報表")]]'
        ))
    )
    print("✅ 找到 lightbox")

    # 補切測站
    try:
        sel = lightbox.find_element(By.TAG_NAME, "select")
        Select(sel).select_by_value(station)
        print("✅ 測站已切換")
        time.sleep(0.4)
    except:
        print("⚠ 測站下拉可能已正確")

    # 下載 CSV
    csv_btn = lightbox.find_element(
        By.XPATH,
        './/div[contains(@class,"lightbox-tool-type-ctrl-btn") and contains(text(),"CSV")]'
    )
    driver.execute_script("arguments[0].click();", csv_btn)
    print("🌦️ 正在下載 CSV...")
    now = datetime.now()
    year = now.year
    month = now.month
    time.sleep(3)
    print("🌦️ 天氣爬蟲完成\n")
    target_filename = f"{station}-{year}-{month}.csv"
    normalize_weather_filename(download_dir, target_filename)
    print("📂 下載完成，目前資料夾內容：", os.listdir(download_dir))



def sort(path):
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    weather_path = f'{path}/467410-{year}-{month}.csv'
    df_weather = pd.read_csv(weather_path)  
    df_weather.columns = df_weather.iloc[0]
    df_weather = df_weather.drop(0).reset_index(drop=True)
    invalid_rows = df_weather[df_weather["StnPres"] == "--"].index.tolist()
    print("StnPres 為 '--' 的列索引：", invalid_rows)
    df_weather = df_weather.drop(columns=['ObsTime', 'StnPresMaxTime', 'StnPresMinTime', 'T Max Time','T Min Time', 'RHMinTime','WGustTime', 
                                        'PrecpMax10Time', 'PrecpMax60Time','UVI Max Time'])
    df_weather.columns
    #該月份天氣不足7天
    if invalid_rows and invalid_rows[0] < 7:
        print("⚠ 偵測到前 7 日出現 '--'，自動改讀上一個月")
        # 取上個月月份
        last_month_day = now.replace(day=1) - timedelta(days=1)
        last_month_year = last_month_day.year
        last_month = last_month_day.month
        # 自動產生上一個月的路徑（補零）
        last_weather_path = f'crawler_DF/467410-{last_month_year}-{last_month:02d}.csv'
        df_last_weather = pd.read_csv(last_weather_path)
        df_last_weather.columns = df_weather.iloc[0]
        df_last_weather = df_weather.drop(0).reset_index(drop=True)
        df_last_weather = df_weather.drop(columns=['ObsTime', 'StnPresMaxTime', 'StnPresMinTime', 'T Max Time','T Min Time', 'RHMinTime','WGustTime', 
                                            'PrecpMax10Time', 'PrecpMax60Time','UVI Max Time'])
        
        
        df_cur_valid = df_weather[df_weather["StnPres"] != "--"]
        # 先嘗試拿本月最後幾天（最多 7 筆）
        cur_part = df_cur_valid.tail(7)
        n_cur = len(cur_part)
        df_last_valid = df_last_weather[df_last_weather["StnPres"] != "--"]
        need = 7 - n_cur
        last_part = df_last_valid.tail(need)

        # 注意順序：先舊月、再新月，時間上比較合理
        week_block = pd.concat([last_part, cur_part], ignore_index=True)
    else: 
        print("✔ 資料完整，使用本月最後 7 筆有效資料")
        df_cur_valid = df_weather[df_weather["StnPres"] != "--"]
        week_block = df_cur_valid.tail(7)
    week_block = week_block.apply(pd.to_numeric, errors="coerce")
    df_weak_weather = week_block.mean(numeric_only=True)
    df_weak_weather
    data = {
        '行政區': [
            '新營區','鹽水區','白河區','柳營區','後壁區','東山區','麻豆區','下營區','六甲區','官田區',
            '大內區','佳里區','學甲區','西港區','七股區','將軍區','北門區','新化區','善化區','新市區',
            '安定區','山上區','玉井區','楠西區','南化區','左鎮區','仁德區','歸仁區','關廟區','龍崎區',
            '永康區','東區','南區','北區','中西區','安南區','安平區'
        ],
        '區別': [
            '67000010','67000020','67000030','67000040','67000050','67000060','67000070','67000080','67000090','67000100',
            '67000110','67000120','67000130','67000140','67000150','67000160','67000170','67000180','67000190','67000200',
            '67000210','67000220','67000230','67000240','67000250','67000260','67000270','67000280','67000290','67000300',
            '67000310','67000320','67000330','67000340','67000350','67000360','67000370'
        ]
    }
    TW_year = year - 1911
    bucket_path = f'{path}/bucket_{TW_year}.csv'
    df_bucket = pd.read_csv(bucket_path)
    df_map = pd.DataFrame(data)
    # 用 merge 依照「區別」來對應行政區
    df_bucket["區別"] = df_bucket["區別"].astype(str).str.strip()
    df_bucket = df_bucket.merge(df_map, on="區別", how="left")

    # 若你想把行政區放前面、或把區別丟掉：
    df_bucket = df_bucket.drop(columns=["Seq","縣市","區別","監測週期"])
    df_bucket = df_bucket.tail(10).reset_index(drop=True)
    df_bucket = df_bucket[["行政區","陽性率","總卵粒數"]]
    df_bucket 
    if isinstance(df_weak_weather, pd.Series):
        df_weak_weather_df = df_weak_weather.to_frame().T
    else:
        df_weak_weather_df = df_weak_weather
    # 加一個假鍵 key=1，做 cross join
    df_bucket["key"] = 1
    df_weak_weather_df["key"] = 1

    df_merged = df_bucket.merge(df_weak_weather_df, on="key").drop(columns=["key"])

    # 輸出 CSV
    output_path = f"{path}/week_data.csv"
    df_merged.to_csv(output_path, index=False, encoding="utf-8-sig")
###########################################
# Main: 選擇要跑哪一段
###########################################
if __name__ == "__main__":

    # 一次建立 driver
    driver, wait, download_dir = init_driver("crawler_DF")
    # 你可以選擇要跑哪些
    crawl_bucket(driver, wait, download_dir)                     # 🪣 誘卵桶
    crawl_weather(driver, wait,download_dir, "467410", "2024-10")  # 🌦️ 天氣資料
    print(download_dir)
    sort(download_dir)
    # driver.quit()
