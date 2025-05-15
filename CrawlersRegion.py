import csv
import json
import time
import random
import hashlib
from curl_cffi import requests
import os
from tqdm import tqdm
import logging
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta  # 新增导入

# --- 用户配置 START ---
CITY_LIST_URL = "https://www.zhipin.com/wapi/zpCommon/data/city.json"
OUTPUT_DIR = "./zhipin_crawler_output/"
REQUEST_INTERVAL = random.uniform(1.5, 3.5)
MAX_WORKERS = 3

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'ai',
    'charset': 'utf8mb4'
}
BASE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "priority": "u=1, i",
    "sec-ch-ua": "\"Chromium\";v=\"124\", \"Google Chrome\";v=\"124\", \"Not-A.Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "token": "RPf0flkFghOVYmL8",
    "traceid": "F-0f8a65u3FEkaMhzJ",
}
COOKIES = {
    "ab_guid": "7263b56e-ef12-4081-a3a7-0b2d76908ddb",
    "Hm_lvt_194df3105ad7148dcf2b98a91b5e727a": "1743075833,1743425693,1744336265,1744696103",
    "wt2": "D5b5pmwvin0FY-cVJJzKQ1V7jZxk0auMbFrhtts_Yo8krqLxNPlLTGPwW-N_HwhFulmG7anyobJAJGpfF0BiBdw~~",
    "wbg": "0",
    "zp_at": "RBnRtDPJkcSoyKYRnQohC4gh63_DKU6NlO1dq9KDQec~",
    "lastCity": "101281600",
    "bst": "V2R9IiEOf631loVtRuyxUZKCiy7DrQwik~|R9IiEOf631loVtRuyxUZKCiy7DrWzSg~",
    "__c": "1747146727",
    "__g": "-",
    "__a": "25811706.1732931558.1747137355.1747146727.95.9.1.95",
    "__zp_stoken__": "5bc4fw4%2FDgkcSQg5dYRIQdVfChMOEc35XW3hzwoDCr2LCunfCtsOHwqpgwqBaUsOHwqzCs1bCrMKbcMKRWMKXS8KkwrTEh8KawpLCm8KcVcKnw4XCmcSkw7dyw7nEm8Kqw4XCnTk3EhENDA4TFBAJD8KHwogXEhgTFBAJDxYVCRAKQSrCpsKQP0RAOS5RTU4LUmJbVWhODGFWTEA%2BEgwSFj4wRTlAOsOARcK%2BesOERsOCf8OGOsOBZTlIOkbDgyQrRsODew1zDlQRwoAMwrrClRHDiWgdbcOew4DDpjZEQ8K5xL9DQydDOT89REBIP0M3QGrDil4icsOnwr%2FDiTQ5GUdDREc7P0NERTlBN0RBRClDRSlFEwsTDhcwQMOBwprDgMOoQ0Q%3D"
}
if "zp_token" not in BASE_HEADERS and "bst" in COOKIES:
    BASE_HEADERS["zp_token"] = COOKIES["bst"]
elif "zp_token" not in BASE_HEADERS and "zp_token" in COOKIES:
    BASE_HEADERS["zp_token"] = COOKIES["zp_token"]

# 新增配置项
TEMP_DATA_FILE = os.path.join(OUTPUT_DIR, 'temp_crawled_data.json')
MAX_DATA_AGE_HOURS = 6  # 临时数据文件最长保留时间（小时），超过此时间将重新爬取
# --- 用户配置 END ---

#日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'crawler.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
os.makedirs(OUTPUT_DIR, exist_ok=True)


class BossZhipinCrawler:
    # (所有类方法基本保持不变，除了batch_fetch_district_data不再直接调用_save_json_data_to_file_and_db)
    def __init__(self):
        self.base_headers = BASE_HEADERS.copy()
        self.cookies_dict = COOKIES.copy()
        self.session = requests.Session()
        self.session.headers.update(self.base_headers)
        self._update_cookies_in_session()

        if not self.cookies_dict.get("__zp_stoken__") and not self.session.headers.get("zp_token"):
            logging.warning(
                "CRITICAL: Key authentication tokens ('__zp_stoken__' cookie or 'zp_token' header) might be missing. Expect failures.")

    def _update_cookies_in_session(self):
        for name, value in self.cookies_dict.items():
            self.session.cookies.set(name, value)

    def _get_dynamic_headers(self, city_code, query="python"):
        headers = self.base_headers.copy()
        headers["referer"] = f"https://www.zhipin.com/web/geek/jobs?city={city_code}&query={query}"
        return headers

    def fetch_city_list(self):
        logging.info("Attempting to fetch city list...")
        try:
            response = self.session.get(
                CITY_LIST_URL,
                timeout=20,
                impersonate="chrome110"
            )
            response.raise_for_status()
            data = response.json()

            if data.get("message") == "Success" and "zpData" in data:
                zp_data_content = data["zpData"]
                if "cityList" in zp_data_content:
                    return self._extract_city_info(zp_data_content)
                else:
                    logging.error(f"获取城市列表失败: 'cityList' not found in 'zpData'. Response: {data}")
                    raise Exception("获取城市列表失败: 'cityList' not found in 'zpData'")
            else:
                logging.error(f"获取城市列表失败，API响应格式不符合预期或未成功: {data}")
                raise Exception(f"获取城市列表失败，API响应格式不符合预期或未成功. Message: {data.get('message')}")

        except requests.RequestsError as e:
            logging.error(f"获取城市列表时请求出错 (curl_cffi): {e}. URL: {CITY_LIST_URL}")
            raise
        except json.JSONDecodeError as e:
            logging.error(
                f"获取城市列表失败，响应非JSON格式. Status: {response.status_code if 'response' in locals() else 'N/A'}, Text: {response.text[:500] if 'response' in locals() else 'N/A'}. Error: {e}")
            raise Exception("获取城市列表失败，响应非JSON格式")
        except Exception as e:
            logging.error(f"获取城市列表时发生未知错误: {e}")
            raise

    def _extract_city_info(self, zp_data_content):
        cities = []
        if not zp_data_content.get("cityList") or not isinstance(zp_data_content["cityList"], list):
            logging.warning("`cityList` not found or not a list in zp_data_content for `_extract_city_info`.")
            return []

        for group in zp_data_content["cityList"]:
            if group and isinstance(group.get("subLevelModelList"), list):
                for city_item in group["subLevelModelList"]:
                    if city_item and city_item.get("name") and city_item.get("code"):
                        if city_item["name"] in ["不限", "全国"]:
                            continue
                        cities.append({
                            "name": str(city_item["name"]),
                            "code": str(city_item["code"])
                        })
            elif group and group.get("name") and group.get("code") and not group.get("subLevelModelList"):
                if group["name"] not in ["不限", "全国"]:
                    cities.append({
                        "name": str(group["name"]),
                        "code": str(group["code"])
                    })
        if not cities:
            logging.warning("未能从city.json中提取任何城市信息。请检查API响应结构和_extract_city_info逻辑。")
        else:
            csv_path = os.path.join(OUTPUT_DIR, 'city_codes.csv')
            try:
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["name", "code"])
                    writer.writeheader()
                    writer.writerows(cities)
                logging.info(f"共提取 {len(cities)} 个城市信息，已保存到 {csv_path}")
            except IOError as e:
                logging.error(f"保存城市列表到CSV文件失败: {e}")
        return cities

    def fetch_district_data(self, city_name, city_code):
        url = "https://www.zhipin.com/wapi/zpgeek/businessDistrict.json"
        params = {
            "cityCode": city_code,
            "_": int(time.time() * 1000)
        }
        dynamic_headers = self._get_dynamic_headers(city_code)
        try:
            response = self.session.get(
                url,
                params=params,
                headers=dynamic_headers,
                timeout=20,
                impersonate="chrome110"
            )
            response.raise_for_status()
            data = response.json()
            if data.get("message") == "Success" and "zpData" in data and \
                    isinstance(data["zpData"], dict) and "businessDistrict" in data["zpData"] and \
                    data["zpData"]["businessDistrict"] is not None:
                district_data = data["zpData"]["businessDistrict"]
                if isinstance(district_data, dict) and district_data.get("code"):
                    return {
                        "city_name": city_name, "city_code": city_code,
                        "data": district_data, "status": "success"
                    }
                else:
                    logging.info(f"{city_name}({city_code}) - 商圈数据存在但为空或格式无效: {district_data}")
                    return {
                        "city_name": city_name, "city_code": city_code, "data": None,
                        "status": "empty_data", "message": "商圈数据为空或格式无效"
                    }
            elif data.get("message") == "Success":
                logging.info(f"{city_name}({city_code}) - API响应成功但无商圈数据. Response: {data}")
                return {
                    "city_name": city_name, "city_code": city_code, "data": None,
                    "status": "empty_data", "message": "无商圈数据 (API成功响应但zpData为空)"
                }
            else:
                error_msg = data.get('message', '未知API错误')
                logging.warning(f"{city_name}({city_code}) - 获取商圈数据API返回错误: {error_msg}. Response: {data}")
                return {
                    "city_name": city_name, "city_code": city_code, "data": None,
                    "status": "failed", "message": f"API Error: {error_msg}"
                }
        except requests.HTTPError as e:
            logging.error(f"{city_name}({city_code}) - HTTP错误 {e.response.status_code}: {e.response.text[:500]}")
            return {"city_name": city_name, "city_code": city_code, "data": None, "status": "failed",
                    "message": f"HTTP {e.response.status_code}"}
        except json.JSONDecodeError:
            logging.error(
                f"{city_name}({city_code}) - 响应非JSON格式. Text: {response.text[:500] if 'response' in locals() else 'N/A'}")
            return {"city_name": city_name, "city_code": city_code, "data": None, "status": "failed",
                    "message": "JSONDecodeError"}
        except requests.RequestsError as e:
            logging.error(f"{city_name}({city_code}) - 请求商圈数据时出错 (curl_cffi): {str(e)}")
            return {"city_name": city_name, "city_code": city_code, "data": None, "status": "error",
                    "message": f"RequestError: {str(e)}"}
        except Exception as e:
            logging.error(f"{city_name}({city_code}) - 获取商圈数据时发生未知异常: {str(e)}")
            return {"city_name": city_name, "city_code": city_code, "data": None, "status": "error",
                    "message": f"Unknown Exception: {str(e)}"}

    def batch_fetch_district_data(self, city_list):
        """批量获取所有指定城市的商圈数据，但不直接保存到数据库的json_storage表"""
        results = []
        failed_cities_details = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="DistrictFetcher") as executor:
            future_to_city = {
                executor.submit(self.fetch_district_data, city['name'], city['code']): city
                for city in city_list
            }

            for future in tqdm(as_completed(future_to_city), total=len(city_list), desc="抓取商圈数据进度"):
                city_meta = future_to_city[future]
                try:
                    result = future.result()
                    results.append(result)
                    if result['status'] not in ('success', 'empty_data'):
                        failed_cities_details.append({
                            'name': city_meta['name'],
                            'code': city_meta['code'],
                            'status': result['status'],
                            'reason': result.get('message', '未知原因')
                        })
                except Exception as e:
                    logging.error(f"处理城市 {city_meta['name']} ({city_meta['code']}) 的future时发生意外错误: {e}")
                    failed_info = {'name': city_meta['name'], 'code': city_meta['code'],
                                   'status': 'exception_in_future', 'reason': str(e)}
                    results.append({
                        "city_name": city_meta['name'], "city_code": city_meta['code'],
                        "data": None, "status": "error", "message": f"Future Exception: {str(e)}"
                    })
                    failed_cities_details.append(failed_info)
                time.sleep(REQUEST_INTERVAL)

        if failed_cities_details:
            logging.error(f"发现 {len(failed_cities_details)} 个城市未能成功获取商圈数据:")
            for fc in failed_cities_details:
                logging.error(f"  城市: {fc['name']}({fc['code']}), 状态: {fc['status']}, 原因: {fc['reason']}")
            raise Exception("部分城市商圈数据获取失败，程序终止。请检查日志。")

        # 不再在这里调用 _save_json_data_to_file_and_db
        # 而是将结果返回，由 main 函数决定如何处理
        return results

    def _save_raw_data_to_temp_file(self, data_to_save, filepath):
        """将原始爬取数据保存到临时JSON文件"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
            logging.info(f"原始爬取数据已成功保存到临时文件: {filepath}")
        except IOError as e:
            logging.error(f"保存原始数据到临时文件 {filepath} 失败: {e}")
            raise  # 保存失败是严重问题，需要上报

    def _load_raw_data_from_temp_file(self, filepath):
        """从临时JSON文件加载原始爬取数据"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logging.info(f"从临时文件 {filepath} 加载原始数据成功。")
            return data
        except FileNotFoundError:
            logging.info(f"临时数据文件 {filepath} 未找到。")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"解析临时数据文件 {filepath} 失败: {e}。文件可能已损坏。")
            return None  # 文件损坏，视为无效
        except IOError as e:
            logging.error(f"读取临时数据文件 {filepath} 失败: {e}")
            return None

    def _save_json_data_to_file_and_db(self, data_to_save):
        """
        保存JSON数据到永久文件 (business_districts_all.json) 和数据库的city_json_storage表.
        这个方法现在接收处理好的数据。
        """
        permanent_output_file = os.path.join(OUTPUT_DIR, 'business_districts_all.json')
        try:
            with open(permanent_output_file, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
            logging.info(f"所有城市的原始商圈数据已保存到永久文件: {permanent_output_file}")
        except IOError as e:
            logging.error(f"保存商圈数据到永久JSON文件失败: {e}")

        try:
            json_str = json.dumps(data_to_save, ensure_ascii=False, sort_keys=True)
            data_hash = hashlib.sha256(json_str.encode('utf-8')).hexdigest()
        except Exception as e:
            logging.error(f"计算数据哈希值时出错: {e}")
            return

        conn = None
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                cursor.execute("SELECT data_hash FROM city_json_storage WHERE data_key = %s", ('all_cities',))
                existing_record = cursor.fetchone()

                if existing_record and existing_record[0] == data_hash:
                    logging.info("数据库中的商圈原始JSON数据未发生变化，无需更新。")
                else:
                    sql = """
                    INSERT INTO city_json_storage (data_key, json_data, data_hash, update_time)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                        json_data = VALUES(json_data),
                        data_hash = VALUES(data_hash),
                        update_time = CURRENT_TIMESTAMP
                    """
                    cursor.execute(sql, ('all_cities', json_str, data_hash))
                    conn.commit()
                    logging.info("商圈原始JSON数据已成功保存/更新到数据库 city_json_storage 表。")
        except pymysql.Error as e:
            logging.error(f"保存JSON数据到数据库 city_json_storage 时出错: {e}")
            raise  # 重大错误，需要上报
        except Exception as e:  # 捕获其他可能的错误，例如 'cryptography' --MySQL5.x和 MySQL8.x的密码策略不同
            logging.error(f"保存JSON数据到数据库时发生一般错误: {e}")
            raise  # 重大错误，需要上报
        finally:
            if conn:
                conn.close()

    def process_and_insert_data_into_region(self, cities_district_data):
        level1_inserts = []
        level2_inserts = []
        level3_inserts = []

        for city_entry in cities_district_data:
            city_q_name = city_entry['city_name']
            city_q_code = str(city_entry['city_code'])

            if city_entry['status'] == 'success' and city_entry.get('data'):
                actual_city_data = city_entry['data']
                city_api_code = str(actual_city_data.get('code'))
                city_api_name = str(actual_city_data.get('name', city_q_name)).replace("'", "''")
                if city_api_code != city_q_code:
                    logging.warning(
                        f"城市 {city_q_name} ({city_q_code}) 的商圈数据API返回代码 ({city_api_code}) 与查询代码不符。将使用API返回代码。")
                level1_inserts.append((city_api_code, city_api_name, None, 1))
                districts = actual_city_data.get('subLevelModelList') or []
                for district in districts:
                    if not (district and district.get('code') and district.get('name')):
                        logging.warning(f"城市 {city_api_name} ({city_api_code}) 的区级数据不完整，跳过: {district}")
                        continue
                    district_code = str(district['code'])
                    district_name = str(district['name']).replace("'", "''")
                    level2_inserts.append((district_code, district_name, city_api_code, 2))
                    sub_districts_or_streets = district.get('subLevelModelList') or []
                    for sub_item in sub_districts_or_streets:
                        if not (sub_item and sub_item.get('code') and sub_item.get('name')):
                            logging.warning(
                                f"区 {district_name} ({district_code}) 的街道/商圈数据不完整，跳过: {sub_item}")
                            continue
                        sub_item_code = str(sub_item['code'])
                        sub_item_name = str(sub_item['name']).replace("'", "''")
                        level3_inserts.append((sub_item_code, sub_item_name, district_code, 3))
            elif city_entry['status'] == 'empty_data':
                logging.info(f"城市 {city_q_name} ({city_q_code}) 无商圈数据，仅作为市级数据插入。")
                level1_inserts.append((city_q_code, city_q_name.replace("'", "''"), None, 1))
        conn = None
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                logging.info("准备清空并重新插入数据到 region 表...")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute("TRUNCATE TABLE region")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
                logging.info("region 表已清空。")
                insert_sql = "INSERT INTO region (code, name, parent_code, level) VALUES (%s, %s, %s, %s)"
                if level1_inserts:
                    cursor.executemany(insert_sql, level1_inserts)
                    logging.info(f"已插入 {len(level1_inserts)} 条市级数据 (level 1)。")
                if level2_inserts:
                    cursor.executemany(insert_sql, level2_inserts)
                    logging.info(f"已插入 {len(level2_inserts)} 条区级数据 (level 2)。")
                if level3_inserts:
                    cursor.executemany(insert_sql, level3_inserts)
                    logging.info(f"已插入 {len(level3_inserts)} 条街道/商圈级数据 (level 3)。")
                conn.commit()
                total_inserted = len(level1_inserts) + len(level2_inserts) + len(level3_inserts)
                logging.info(f"所有数据已成功插入到 region 表。共插入 {total_inserted} 条记录。")
        except pymysql.Error as e:
            logging.error(f"数据库操作 (region表) 出错: {e}")
            if conn: conn.rollback()
            raise
        except Exception as e:  # 捕获其他可能的错误，例如上面你遇到的 'cryptography'
            logging.error(f"处理并插入数据到 region 表时发生一般错误: {e}")
            if conn: conn.rollback()
            raise
        finally:
            if conn:
                conn.close()


def main():
    logging.info("=== Boss Zhipin区域数据抓取脚本启动 ===")
    logging.warning("重要提示: 请确保配置文件中的 COOKIES 和 BASE_HEADERS 是最新的，否则请求很可能会失败。")
    logging.warning(f"临时数据文件将尝试从 '{TEMP_DATA_FILE}' 加载/保存。")
    logging.warning(f"如果临时数据文件超过 {MAX_DATA_AGE_HOURS} 小时，将强制重新爬取。")

    crawler = BossZhipinCrawler()
    all_cities_district_data = None
    force_recrawl = False  # 可以通过命令行参数控制这个

    # 检查点逻辑
    if os.path.exists(TEMP_DATA_FILE) and not force_recrawl:
        try:
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(TEMP_DATA_FILE))
            if datetime.now() - file_mod_time < timedelta(hours=MAX_DATA_AGE_HOURS):
                logging.info(f"发现有效的临时数据文件 (修改于 {file_mod_time}), 尝试从中加载数据...")
                all_cities_district_data = crawler._load_raw_data_from_temp_file(TEMP_DATA_FILE)
                if all_cities_district_data:
                    logging.info("成功从临时文件加载数据，将跳过爬虫步骤。")
                else:
                    logging.warning("临时数据文件无效或解析失败，将执行爬虫。")
            else:
                logging.info(f"临时数据文件已超过 {MAX_DATA_AGE_HOURS} 小时，将执行爬虫。")
        except Exception as e:
            logging.warning(f"检查临时数据文件时发生错误: {e}。将执行爬虫。")
            all_cities_district_data = None  # 确保如果加载失败，会重新爬取

    if all_cities_district_data is None:  # 如果没有从文件加载成功，则执行爬虫
        try:
            logging.info("--- 步骤 1: 获取城市列表 ---")
            city_list = crawler.fetch_city_list()
            if not city_list:
                logging.error("未能获取到城市列表，程序终止。")
                return  # 或者 exit(1)
            logging.info(f"成功获取 {len(city_list)} 个城市的基础信息。")

            logging.info(f"--- 步骤 2: 批量获取 {len(city_list)} 个城市的商圈数据 ---")
            all_cities_district_data = crawler.batch_fetch_district_data(city_list)

            # 爬取成功后，立即保存到临时文件
            crawler._save_raw_data_to_temp_file(all_cities_district_data, TEMP_DATA_FILE)

        except Exception as e:
            logging.error(f"爬虫步骤执行失败: {e}", exc_info=True)
            logging.error("请检查日志，修复问题后重试。")
            # exit(1) # 根据需要决定是否在这里退出
            return  # 确保如果爬虫失败，后续步骤不会执行

    # ---- 后续处理步骤 ----
    # 无论数据是新爬取的还是从文件加载的，都会执行以下步骤
    if all_cities_district_data:
        try:
            logging.info("--- 步骤 3: 将原始商圈数据保存到永久文件和数据库的json_storage表 ---")
            # 这个方法现在只负责保存，不负责获取
            crawler._save_json_data_to_file_and_db(all_cities_district_data)

            successful_fetches = [d for d in all_cities_district_data if d['status'] == 'success']
            empty_data_fetches = [d for d in all_cities_district_data if d['status'] == 'empty_data']
            logging.info(
                f"数据统计：成功获取商圈数据城市数: {len(successful_fetches)}。无数据城市数: {len(empty_data_fetches)}。")

            logging.info("--- 步骤 4: 处理数据并插入到 region 表 ---")
            crawler.process_and_insert_data_into_region(all_cities_district_data)

            logging.info("=== 数据处理全部完成！脚本执行成功。 ===")

        except Exception as e:
            logging.error(f"数据保存或插入数据库步骤执行失败: {e}", exc_info=True)
            logging.error("请检查日志，修复问题后重试。上次爬取的数据已保存在临时文件中，若有效可用于下次运行。")
            # exit(1)
    else:
        logging.error("未能获取或加载任何商圈数据，无法进行后续处理。程序终止。")
        # exit(1)


if __name__ == "__main__":
    # 为了解决你之前遇到的 `cryptography` 问题，确保它已安装
    try:
        import cryptography
    except ImportError:
        logging.error("关键依赖 'cryptography' 未安装。请运行 'pip install cryptography' 后重试。")
        # exit(1) # 可以选择在这里直接退出
        # 为了演示，我们继续，但如果DB需要它，后续会失败
    main()