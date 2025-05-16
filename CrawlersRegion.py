import csv
import json
import time
import random
import hashlib
from curl_cffi import requests  # 确保这是 curl_cffi.requests
import os
from tqdm import tqdm
import logging
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# --- 用户配置 START ---
CITY_LIST_URL = "https://www.zhipin.com/wapi/zpCommon/data/city.json"
OUTPUT_DIR = "./zhipin_crawler_output/"
REQUEST_INTERVAL = random.uniform(1.5, 3.5)
MAX_WORKERS = 3

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',  # 请修改为你的密码
    'database': 'ai',  # 请修改为你的数据库名
    'port': 3306,
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
if "bst" in COOKIES:
    BASE_HEADERS["zp_token"] = COOKIES["bst"]

TEMP_DATA_FILE = os.path.join(OUTPUT_DIR, 'temp_crawled_data.json')
MAX_DATA_AGE_HOURS = 6
DEBUG_JSON_OUTPUT = True
# --- 用户配置 END ---

os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'crawler.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class BossZhipinCrawler:
    def __init__(self):
        self.base_headers = BASE_HEADERS.copy()
        self.cookies_dict = COOKIES.copy()
        self.session = requests.Session()
        self.session.headers.update(self.base_headers)
        self._update_cookies_in_session()
        if not self.cookies_dict.get("__zp_stoken__") and not self.session.headers.get("zp_token"):
            logging.warning("CRITICAL: Key auth tokens might be missing. Expect failures.")

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
            response = self.session.get(CITY_LIST_URL, timeout=20, impersonate="chrome110")
            response.raise_for_status()
            data = response.json()
            if data.get("message") == "Success" and "zpData" in data and "cityList" in data["zpData"]:
                return self._extract_city_info(data["zpData"])
            else:
                msg = f"获取城市列表失败: API响应格式不符合预期或未成功. Response: {data.get('message', data)}"
                logging.error(msg)
                raise Exception(msg)
        except requests.RequestsError as e:
            logging.error(f"获取城市列表请求出错: {e}"); raise
        except json.JSONDecodeError as e:
            logging.error(f"获取城市列表响应非JSON: {e}"); raise
        except Exception as e:
            logging.error(f"获取城市列表未知错误: {e}"); raise

    def _extract_city_info(self, zp_data_content):
        cities = []
        if not zp_data_content.get("cityList") or not isinstance(zp_data_content["cityList"], list):
            logging.warning("`cityList` not found or not a list for extraction.");
            return []
        for group in zp_data_content["cityList"]:
            for city_item in group.get("subLevelModelList", []):
                if city_item and city_item.get("name") and city_item.get("code") and city_item["name"] not in ["不限",
                                                                                                               "全国"]:
                    cities.append({"name": str(city_item["name"]), "code": str(city_item["code"])})
            if group and group.get("name") and group.get("code") and not group.get("subLevelModelList") and group[
                "name"] not in ["不限", "全国"]:
                cities.append({"name": str(group["name"]), "code": str(group["code"])})
        if cities:
            csv_path = os.path.join(OUTPUT_DIR, 'city_codes.csv')
            try:
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["name", "code"]);
                    writer.writeheader();
                    writer.writerows(cities)
                logging.info(f"共提取 {len(cities)} 个城市信息，已保存到 {csv_path}")
            except IOError as e:
                logging.error(f"保存城市列表到CSV失败: {e}")
        else:
            logging.warning("未能提取任何城市信息。")
        return cities

    def fetch_district_data(self, city_name, city_code):
        url = "https://www.zhipin.com/wapi/zpgeek/businessDistrict.json"
        params = {"cityCode": city_code, "_": int(time.time() * 1000)}
        status_args = {"city_name": city_name, "city_code": city_code, "data": None}
        try:
            response = self.session.get(url, params=params, headers=self._get_dynamic_headers(city_code), timeout=20,
                                        impersonate="chrome110")
            response.raise_for_status()
            data = response.json()
            if data.get("message") == "Success" and isinstance(data.get("zpData"), dict) and data["zpData"].get(
                    "businessDistrict"):
                district_data = data["zpData"]["businessDistrict"]
                if isinstance(district_data, dict) and district_data.get("code"):
                    return {**status_args, "data": district_data, "status": "success"}
                else:
                    logging.info(f"{city_name}({city_code}) - 商圈数据存在但为空或格式无效: {district_data}")
                    return {**status_args, "status": "empty_data", "message": "商圈数据为空或格式无效"}
            elif data.get("message") == "Success":
                logging.info(f"{city_name}({city_code}) - API响应成功但无商圈数据. Response: {data}")
                return {**status_args, "status": "empty_data",
                        "message": "无商圈数据 (API成功但zpData/businessDistrict为空或无效)"}
            else:
                error_msg = data.get('message', '未知API错误')
                logging.warning(f"{city_name}({city_code}) - 获取商圈数据API返回错误: {error_msg}. Response: {data}")
                return {**status_args, "status": "failed", "message": f"API Error: {error_msg}"}
        except requests.HTTPError as e:
            return {"city_name": city_name, "city_code": city_code, "data": None, "status": "failed",
                    "message": f"HTTP {e.response.status_code}"}
        except json.JSONDecodeError:
            return {"city_name": city_name, "city_code": city_code, "data": None, "status": "failed",
                    "message": "JSONDecodeError"}
        except requests.RequestsError as e:
            return {"city_name": city_name, "city_code": city_code, "data": None, "status": "error",
                    "message": f"RequestError: {e}"}
        except Exception as e:
            return {"city_name": city_name, "city_code": city_code, "data": None, "status": "error",
                    "message": f"Unknown Exception: {e}"}

    def batch_fetch_district_data(self, city_list):
        results, failed_cities_details = [], []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="DistrictFetcher") as executor:
            future_to_city = {executor.submit(self.fetch_district_data, c['name'], c['code']): c for c in city_list}
            for future in tqdm(as_completed(future_to_city), total=len(city_list), desc="抓取商圈数据进度"):
                city_meta = future_to_city[future]
                try:
                    result = future.result()
                    results.append(result)
                    if result['status'] not in ('success', 'empty_data'):
                        failed_cities_details.append(
                            {'name': city_meta['name'], 'code': city_meta['code'], 'status': result['status'],
                             'reason': result.get('message', '未知原因')})
                except Exception as e:
                    logging.error(f"处理城市 {city_meta['name']} 的future时意外错误: {e}")
                    failed_info = {'name': city_meta['name'], 'code': city_meta['code'],
                                   'status': 'exception_in_future', 'reason': str(e)}
                    results.append({"city_name": city_meta['name'], "city_code": city_meta['code'], "data": None,
                                    "status": "error", "message": f"Future Exception: {e}"})
                    failed_cities_details.append(failed_info)
                time.sleep(REQUEST_INTERVAL)
        if failed_cities_details:
            for fc in failed_cities_details: logging.error(
                f"失败城市: {fc['name']}({fc['code']}), 状态: {fc['status']}, 原因: {fc['reason']}")
            raise Exception(f"发现 {len(failed_cities_details)} 个城市未能成功获取商圈数据，程序终止。")
        return results

    def _save_raw_data_to_temp_file(self, data_to_save, filepath):
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
            logging.info(f"原始爬取数据已保存到临时文件: {filepath}")
        except IOError as e:
            logging.error(f"保存原始数据到临时文件 {filepath} 失败: {e}"); raise

    def _load_raw_data_from_temp_file(self, filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logging.info(f"从临时文件 {filepath} 加载原始数据成功。");
            return data
        except FileNotFoundError:
            logging.info(f"临时数据文件 {filepath} 未找到。"); return None
        except json.JSONDecodeError as e:
            logging.error(f"解析临时数据文件 {filepath} 失败: {e}."); return None
        except IOError as e:
            logging.error(f"读取临时数据文件 {filepath} 失败: {e}"); return None

    def _save_json_data_to_file_and_db(self, data_to_save):
        # 将整体数据保存到永久JSON文件 (作为一种备份)
        permanent_output_file = os.path.join(OUTPUT_DIR, 'business_districts_all.json')
        try:
            with open(permanent_output_file, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
            logging.info(f"原始商圈数据已保存到永久文件: {permanent_output_file}")
        except IOError as e:
            logging.error(f"保存商圈数据到永久JSON文件失败: {e}")

        data_updated_in_db = False  # 标记数据库中的数据是否被更新
        canonical_data_for_hashing = None  # 用于存储排序后的、用于哈希计算的数据
        try:
            # 准备数据以计算哈希值，确保一致性
            if isinstance(data_to_save, list):  # 如果data_to_save是列表 (我们期望的情况)
                # 定义排序键函数：优先使用'city_code'，如果不存在或元素不是字典，则用元素本身转字符串
                def get_sort_key(item):
                    if isinstance(item, dict):
                        return str(item.get('city_code', ''))  # 使用.get避免KeyError，并转为字符串
                    return str(item)  # 其他类型直接转字符串以供排序

                try:
                    # 对列表进行排序，确保每次生成的JSON字符串顺序一致，从而保证哈希值的一致性
                    canonical_data_for_hashing = sorted(data_to_save, key=get_sort_key)
                except Exception as sort_e:  # 捕获排序中可能发生的任何错误
                    logging.error(f"为哈希目的排序数据时发生错误: {sort_e}. 将使用原始顺序（可能导致哈希不一致）。")
                    canonical_data_for_hashing = data_to_save  # 出错则使用原始顺序作为后备
            else:  # 如果data_to_save不是列表 (例如单个字典，虽然在此场景不太可能)
                canonical_data_for_hashing = data_to_save

            # 序列化为JSON字符串，键按字母顺序排序 (sort_keys=True) 以保证一致性
            json_str = json.dumps(canonical_data_for_hashing, ensure_ascii=False, sort_keys=True)
            # 计算JSON字符串的SHA256哈希值
            data_hash = hashlib.sha256(json_str.encode('utf-8')).hexdigest()

        except Exception as e:  # 如果哈希计算过程出错
            logging.error(f"计算数据哈希值时出错: {e}")
            raise  # 重新抛出异常，这是一个关键步骤的失败

        # 连接数据库，比较哈希值并更新/插入数据
        conn = None
        try:
            conn = pymysql.connect(**DB_CONFIG)  # 建立数据库连接
            with conn.cursor() as cursor:  # 创建数据库游标
                data_key_for_db = 'all_cities'  # 定义在数据库中存储这条聚合数据的键名
                # 查询数据库中是否已存在该键的数据及其哈希值
                cursor.execute("SELECT data_hash FROM city_json_storage WHERE data_key = %s", (data_key_for_db,))
                existing_record = cursor.fetchone()  # 获取查询结果 (一行或None)
                existing_hash_from_db = existing_record[0] if existing_record else None  # 如果有记录，则获取哈希值

                # 比较新计算的哈希值与数据库中存储的哈希值
                if existing_hash_from_db and existing_hash_from_db == data_hash:
                    # 哈希值相同，表示数据内容未发生变化
                    logging.info(f"数据库中键 '{data_key_for_db}' 的数据未发生变化，无需更新。")
                    data_updated_in_db = False  # 标记数据库数据未更新
                else:
                    # 哈希值不同，或数据库中尚无此记录，需要执行更新或插入操作
                    log_msg_prefix = f"键 '{data_key_for_db}'"
                    if existing_hash_from_db:  # 如果是哈希不匹配 (说明是更新操作)
                        logging.warning(f"{log_msg_prefix} 的哈希不匹配，将更新数据库。")
                        logging.info(f"  DB Hash  : {existing_hash_from_db}")  # 打印旧哈希
                        logging.info(f"  New Hash : {data_hash}")  # 打印新哈希
                    else:  # 如果数据库中没有记录 (说明是新插入操作)
                        logging.info(f"数据库中未找到 {log_msg_prefix} 的记录，将插入新数据。 New Hash: {data_hash}")

                    # 如果配置了调试输出，则将用于生成新哈希的JSON字符串保存到文件
                    if DEBUG_JSON_OUTPUT:
                        debug_json_path = os.path.join(OUTPUT_DIR, f"debug_json_for_hash_{data_key_for_db}.json")
                        try:
                            with open(debug_json_path, "w", encoding="utf-8") as f_debug:
                                f_debug.write(json_str)  # 将JSON字符串写入调试文件
                            logging.info(f"用于生成新哈希的JSON已保存到 {debug_json_path}")
                        except IOError as io_err:  # 文件写入错误
                            logging.error(f"保存调试JSON文件失败: {io_err}")

                    # 构建SQL语句：插入或更新记录 (使用ON DUPLICATE KEY UPDATE实现存在则更新，不存在则插入)
                    sql = """
                    INSERT INTO city_json_storage (data_key, json_data, data_hash, update_time)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                        json_data = VALUES(json_data),
                        data_hash = VALUES(data_hash),
                        update_time = CURRENT_TIMESTAMP
                    """
                    cursor.execute(sql, (data_key_for_db, json_str, data_hash))  # 执行SQL
                    conn.commit()  # 提交数据库事务
                    logging.info(f"{log_msg_prefix} 的JSON数据已成功保存/更新到数据库 city_json_storage 表。")
                    data_updated_in_db = True  # 标记数据库数据已更新
        except pymysql.Error as e:  # 捕获PyMySQL相关的数据库操作错误
            logging.error(f"保存JSON数据到数据库 city_json_storage 时出错: {e}")
            if conn: conn.rollback()  # 如果发生错误且连接存在，则回滚事务
            raise  # 重新抛出异常
        except Exception as e:  # 捕获其他一般性错误
            logging.error(f"保存JSON数据到数据库时发生一般错误: {e}")
            if conn: conn.rollback()
            raise
        finally:  # 无论操作成功与否，确保数据库连接在最后被关闭
            if conn:
                conn.close()
        return data_updated_in_db  # 返回数据库数据是否被更新的标志

    def process_and_insert_data_into_region(self, cities_district_data):
        level1_inserts, level2_inserts, level3_inserts = [], [], []  # 初始化用于存储各级别区域数据的列表

        # 遍历从API获取或从临时文件加载的每个城市的数据条目
        for city_entry in cities_district_data:
            city_q_name = city_entry['city_name']  # 获取查询时使用的城市名称
            city_q_code = str(city_entry['city_code'])  # 获取查询时使用的城市代码

            # 只处理状态为 'success' 且包含有效 'data' 字段的条目
            if city_entry['status'] == 'success' and city_entry.get('data'):
                actual_city_data = city_entry['data']  # 这是API返回的该城市的具体商圈数据结构
                city_api_code = str(actual_city_data.get('code'))  # API返回的城市代码
                # API返回的城市名，如果API未返回名称，则使用查询时的名称。替换SQL中的单引号。
                city_api_name = str(actual_city_data.get('name', city_q_name)).replace("'", "''")

                # 记录一个警告如果API返回的城市代码与查询时不一致 (数据源可能的问题)
                if city_api_code != city_q_code:
                    logging.warning(
                        f"城市 {city_q_name} ({city_q_code}) 的商圈数据API返回代码 ({city_api_code}) 与查询代码不符。将使用API返回代码。")
                # 准备市级数据 (level 1)，父代码为None
                level1_inserts.append((city_api_code, city_api_name, None, 1))

                # 获取该城市下的区/县列表 (subLevelModelList)
                districts_data = actual_city_data.get('subLevelModelList')
                # 如果 districts_data 是 None (JSON中的null) 或者不是一个列表，则视为空列表，避免迭代错误
                districts = districts_data if isinstance(districts_data, list) else []

                # 遍历区/县
                for district in districts:
                    # 检查区/县数据是否包含必要的 code 和 name 字段
                    if not (district and district.get('code') and district.get('name')):
                        logging.warning(f"城市 {city_api_name} ({city_api_code}) 的区级数据不完整，跳过: {district}")
                        continue  # 跳过这个不完整的区数据
                    district_code = str(district['code'])
                    district_name = str(district['name']).replace("'", "''")
                    # 准备区级数据 (level 2)，父代码为市级代码
                    level2_inserts.append((district_code, district_name, city_api_code, 2))

                    # 获取该区/县下的街道/商圈列表
                    sub_districts_data = district.get('subLevelModelList')
                    # 同样处理可能为 None 或非列表的情况
                    sub_districts_or_streets = sub_districts_data if isinstance(sub_districts_data, list) else []

                    # 遍历街道/商圈
                    for sub_item in sub_districts_or_streets:
                        # 检查街道/商圈数据是否包含必要的 code 和 name 字段
                        if not (sub_item and sub_item.get('code') and sub_item.get('name')):
                            logging.warning(
                                f"区 {district_name} ({district_code}) 的街道/商圈数据不完整，跳过: {sub_item}")
                            continue  # 跳过这个不完整的街道数据
                        sub_item_code = str(sub_item['code'])
                        sub_item_name = str(sub_item['name']).replace("'", "''")
                        # 准备街道级数据 (level 3)，父代码为区级代码
                        level3_inserts.append((sub_item_code, sub_item_name, district_code, 3))
            elif city_entry['status'] == 'empty_data':  # 如果城市API调用成功但返回无商圈数据
                logging.info(f"城市 {city_q_name} ({city_q_code}) 无商圈数据，仅作为市级数据插入。")
                level1_inserts.append((city_q_code, city_q_name.replace("'", "''"), None, 1))

        # --- 数据库操作：清空并批量插入region表 ---
        conn = None
        try:
            conn = pymysql.connect(**DB_CONFIG)  # 连接数据库
            conn.autocommit(False)  # 关闭自动提交，以便进行事务控制
            with conn.cursor() as cursor:  # 创建游标
                logging.info("准备清空并重新插入数据到 region 表...")
                # 在 TRUNCATE 操作前后禁用/启用外键检查，防止因外键约束导致TRUNCATE失败
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute("TRUNCATE TABLE region")  # 清空 region 表
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
                logging.info("region 表已清空。")

                # 构建批量插入SQL语句
                insert_sql = "INSERT INTO region (code, name, parent_code, level) VALUES (%s, %s, %s, %s)"
                # 批量插入市级数据
                if level1_inserts:
                    cursor.executemany(insert_sql, level1_inserts)
                    logging.info(f"已插入 {len(level1_inserts)} 条市级数据 (level 1)。")
                # 批量插入区级数据
                if level2_inserts:
                    cursor.executemany(insert_sql, level2_inserts)
                    logging.info(f"已插入 {len(level2_inserts)} 条区级数据 (level 2)。")
                # 批量插入街道级数据
                if level3_inserts:
                    cursor.executemany(insert_sql, level3_inserts)
                    logging.info(f"已插入 {len(level3_inserts)} 条街道/商圈级数据 (level 3)。")

                conn.commit()  # 如果所有插入操作都成功，则提交事务
                total_inserted = len(level1_inserts) + len(level2_inserts) + len(level3_inserts)
                logging.info(f"所有数据已成功插入到 region 表。共插入 {total_inserted} 条记录。")
        except pymysql.Error as e:  # 捕获数据库操作相关的错误
            logging.error(f"数据库操作 (region表) 出错: {e}")
            if conn: conn.rollback()  # 如果出错且连接存在，则回滚事务
            raise  # 重新抛出异常
        except Exception as e:  # 捕获其他一般性错误
            logging.error(f"处理并插入数据到 region 表时发生一般错误: {e}")
            if conn: conn.rollback()
            raise
        finally:  # 确保数据库连接在最后被关闭
            if conn:
                conn.close()


def main():
    logging.info("=== Boss Zhipin区域数据抓取脚本启动 ===")
    logging.warning("重要提示: 请确保配置文件中的 COOKIES 和 BASE_HEADERS 是最新的，否则请求很可能会失败。")
    logging.warning(f"临时数据文件将尝试从 '{TEMP_DATA_FILE}' 加载/保存。")
    logging.warning(f"如果临时数据文件超过 {MAX_DATA_AGE_HOURS} 小时，将强制重新爬取。")

    crawler = BossZhipinCrawler()
    all_cities_district_data = None
    force_recrawl = False
    source_data_is_newly_fetched = False

    if os.path.exists(TEMP_DATA_FILE) and not force_recrawl:
        try:
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(TEMP_DATA_FILE))
            if datetime.now() - file_mod_time < timedelta(hours=MAX_DATA_AGE_HOURS):
                logging.info(f"发现有效的临时数据文件 (修改于 {file_mod_time}), 尝试加载...")
                all_cities_district_data = crawler._load_raw_data_from_temp_file(TEMP_DATA_FILE)
                if all_cities_district_data:
                    logging.info("成功从临时文件加载数据，跳过爬虫。")
                else:
                    logging.warning("临时数据文件无效或解析失败，将执行爬虫。")
                    all_cities_district_data = None
            else:
                logging.info(f"临时数据文件已超 {MAX_DATA_AGE_HOURS} 小时，将执行爬虫。")
                all_cities_district_data = None
        except Exception as e:
            logging.warning(f"检查临时数据文件时发生错误: {e}。将执行爬虫。")
            all_cities_district_data = None

    if all_cities_district_data is None:
        source_data_is_newly_fetched = True
        try:
            logging.info("--- 步骤 1: 获取城市列表 ---")
            city_list = crawler.fetch_city_list()
            if not city_list: logging.error("未能获取城市列表，终止。"); return
            logging.info(f"成功获取 {len(city_list)} 个城市基础信息。")
            logging.info(f"--- 步骤 2: 批量获取 {len(city_list)} 个城市商圈数据 ---")
            all_cities_district_data = crawler.batch_fetch_district_data(city_list)
            crawler._save_raw_data_to_temp_file(all_cities_district_data, TEMP_DATA_FILE)
        except Exception as e:
            logging.error(f"爬虫步骤执行失败: {e}", exc_info=True);
            logging.error("请检查日志后重试。");
            return

    if all_cities_district_data:
        data_source_changed_in_db = False
        try:
            logging.info("--- 步骤 3: 保存原始商圈数据到永久文件和数据库的json_storage表 ---")
            data_source_changed_in_db = crawler._save_json_data_to_file_and_db(all_cities_district_data)

            success_count = sum(1 for d in all_cities_district_data if d['status'] == 'success')
            empty_count = sum(1 for d in all_cities_district_data if d['status'] == 'empty_data')
            logging.info(f"数据统计：成功获取商圈数据城市数: {success_count}。无数据城市数: {empty_count}。")

            if source_data_is_newly_fetched or data_source_changed_in_db:
                if source_data_is_newly_fetched and not data_source_changed_in_db:
                    logging.info("数据为新爬取 (但与DB中json_storage内容一致)，仍将处理 region 表以确保同步。")
                elif data_source_changed_in_db:
                    logging.info("检测到原始数据在数据库中已更新或为首次存储，将处理 region 表。")

                logging.info("--- 步骤 4: 处理数据并插入到 region 表 ---")
                crawler.process_and_insert_data_into_region(all_cities_district_data)
            else:
                logging.info(
                    "原始数据未发生变化 (从临时文件加载且与DB中json_storage内容一致)，跳过 region 表的处理和插入。")

            logging.info("=== 数据处理全部完成！脚本执行成功。 ===")
        except Exception as e:
            logging.error(f"数据保存或插入DB步骤失败: {e}", exc_info=True)
            logging.error("请检查日志后重试。上次爬取数据已存临时文件，若有效可用于下次运行。")
    else:
        logging.error("未能获取或加载任何商圈数据，无法后续处理。终止。")


if __name__ == "__main__":
    try:
        import cryptography
    except ImportError:
        logging.warning(
            "可选依赖 'cryptography' 未安装。如果连接 MySQL 8.x 或使用特定认证方式，可能需 'pip install cryptography'。")
    main()