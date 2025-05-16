import csv
import json
import time
import random
import hashlib
from curl_cffi import requests  # 确保这是 curl_cffi.requests
import os
from tqdm import tqdm  # 用于显示进度条
import logging  # 用于记录日志
import pymysql  # 用于连接MySQL数据库
from concurrent.futures import ThreadPoolExecutor, as_completed  # 用于多线程并发
from datetime import datetime, timedelta  # 用于处理日期时间

# --- 用户配置 START ---
CITY_LIST_URL = "https://www.zhipin.com/wapi/zpCommon/data/city.json"
OUTPUT_DIR = "./zhipin_crawler_output/"
REQUEST_INTERVAL = random.uniform(1.5, 3.5)
MAX_WORKERS = 3

DB_CONFIG = {
    'host': '43.153.41.128',
    'user': 'root',
    'password': 'blueCat666',  # !!! 请修改为你的数据库密码 !!!
    'database': 'hr-ai',  # !!! 请修改为你的数据库名 !!!
    'charset': 'utf8mb4',
    'port': 7777
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

# TEMP_DATA_FILE 现在存储转换后的 RegionVo 结构列表
TEMP_DATA_FILE = os.path.join(OUTPUT_DIR, 'temp_regionvo_data.json')  # 修改文件名以反映内容变化
MAX_DATA_AGE_HOURS = 6
DEBUG_JSON_OUTPUT = True
PERMANENT_REGIONVO_JSON_FILE = os.path.join(OUTPUT_DIR, 'all_cities_regionvo_structure.json')  # 唯一的永久JSON文件
DB_STORAGE_KEY_REGIONVO = 'all_cities_regionvo'  # 数据库中存储RegionVo结构JSON的键
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
        logging.info("尝试获取城市列表...")
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
            logging.warning("`cityList` 在城市数据中未找到或格式不正确，无法提取。");
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
            return {**status_args, "status": "failed", "message": f"HTTP {e.response.status_code}"}
        except json.JSONDecodeError:
            return {**status_args, "status": "failed", "message": "JSONDecodeError"}
        except requests.RequestsError as e:
            return {**status_args, "status": "error", "message": f"RequestError: {e}"}
        except Exception as e:
            return {**status_args, "status": "error", "message": f"Unknown Exception: {e}"}

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
        return results  # 返回的是原始API数据列表

    def _save_converted_data_to_temp_file(self, data_to_save_region_vo_list, filepath):  # 修改了方法名和参数名
        """将转换后的RegionVo结构数据保存到临时JSON文件。"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data_to_save_region_vo_list, f, ensure_ascii=False, indent=2)
            logging.info(f"转换后的RegionVo结构数据已保存到临时文件: {filepath}")
        except IOError as e:
            logging.error(f"保存转换后的RegionVo结构数据到临时文件 {filepath} 失败: {e}")
            raise

    def _load_converted_data_from_temp_file(self, filepath):  # 修改了方法名
        """从临时JSON文件加载转换后的RegionVo结构数据。"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logging.info(f"从临时文件 {filepath} 加载转换后的RegionVo结构数据成功。")
            return data
        except FileNotFoundError:
            logging.info(f"临时数据文件 {filepath} (RegionVo结构) 未找到。")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"解析临时数据文件 {filepath} (RegionVo结构) 失败: {e}。")
            return None
        except IOError as e:
            logging.error(f"读取临时数据文件 {filepath} (RegionVo结构) 失败: {e}")
            return None

    def _convert_to_region_vo_structure(self, boss_data_node, level, parent_code=None):
        """递归将Boss API节点转换为RegionVo兼容字典。"""
        if not boss_data_node or not boss_data_node.get('code') or not boss_data_node.get('name'):
            return None
        region_vo_node = {
            "code": str(boss_data_node.get('code')),
            "name": str(boss_data_node.get('name')).replace("'", "''"),
            "level": level,
            "parentCode": parent_code,
            "subLevelModelList": []
        }
        boss_sub_level_list = boss_data_node.get('subLevelModelList')
        if isinstance(boss_sub_level_list, list):
            for sub_node_data in boss_sub_level_list:
                converted_sub_node = self._convert_to_region_vo_structure(sub_node_data, level + 1,
                                                                          region_vo_node["code"])
                if converted_sub_node:
                    region_vo_node["subLevelModelList"].append(converted_sub_node)
        return region_vo_node

    def _save_regionvo_json_to_permanent_storage(self, data_to_save_region_vo_list):  # 修改了方法名
        """
        将转换后的RegionVo结构数据保存到永久JSON文件，并存入数据库的 `city_json_storage` 表。
        返回: 布尔值，指示数据库中的数据是否被更新。
        """
        # 1. 保存到永久JSON文件
        try:
            with open(PERMANENT_REGIONVO_JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(data_to_save_region_vo_list, f, ensure_ascii=False, indent=2)
            logging.info(f"转换后的RegionVo结构数据已保存到永久文件: {PERMANENT_REGIONVO_JSON_FILE}")
        except IOError as e:
            logging.error(f"保存转换后数据到永久JSON文件失败: {e}")

        data_updated_in_db = False
        canonical_data_for_hashing = None
        try:
            # 2. 准备数据以计算哈希值
            if isinstance(data_to_save_region_vo_list, list):
                def get_sort_key(item):
                    if isinstance(item, dict): return str(item.get('code', ''))
                    return str(item)

                try:
                    canonical_data_for_hashing = sorted(data_to_save_region_vo_list, key=get_sort_key)
                except Exception as sort_e:
                    logging.error(f"为哈希目的排序转换后数据时发生错误: {sort_e}. 将使用原始顺序。")
                    canonical_data_for_hashing = data_to_save_region_vo_list
            else:
                canonical_data_for_hashing = data_to_save_region_vo_list

            json_str = json.dumps(canonical_data_for_hashing, ensure_ascii=False, sort_keys=True)
            data_hash = hashlib.sha256(json_str.encode('utf-8')).hexdigest()
        except Exception as e:
            logging.error(f"计算转换后数据哈希值时出错: {e}"); raise

        # 3. 连接数据库，比较哈希值并更新/插入数据
        conn = None
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                cursor.execute("SELECT data_hash FROM city_json_storage WHERE data_key = %s",
                               (DB_STORAGE_KEY_REGIONVO,))
                existing_record = cursor.fetchone()
                existing_hash_from_db = existing_record[0] if existing_record else None

                if existing_hash_from_db and existing_hash_from_db == data_hash:
                    logging.info(f"数据库中键 '{DB_STORAGE_KEY_REGIONVO}' 的数据未变，无需更新。")
                    data_updated_in_db = False
                else:
                    log_msg_prefix = f"键 '{DB_STORAGE_KEY_REGIONVO}'"
                    if existing_hash_from_db:
                        logging.warning(
                            f"{log_msg_prefix} 的哈希不匹配，将更新。 DB Hash: {existing_hash_from_db}, New Hash: {data_hash}")
                    else:
                        logging.info(f"数据库中未找到 {log_msg_prefix} 的记录，将插入。 New Hash: {data_hash}")
                    if DEBUG_JSON_OUTPUT:
                        debug_json_path = os.path.join(OUTPUT_DIR,
                                                       f"debug_json_for_hash_{DB_STORAGE_KEY_REGIONVO}.json")
                        try:
                            with open(debug_json_path, "w", encoding="utf-8") as f_debug:
                                f_debug.write(json_str)
                            logging.info(f"用于生成新哈希的RegionVo结构JSON已保存到 {debug_json_path}")
                        except IOError as io_err:
                            logging.error(f"保存调试JSON失败: {io_err}")

                    sql = """INSERT INTO city_json_storage (data_key, json_data, data_hash, update_time)
                             VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                             ON DUPLICATE KEY UPDATE json_data = VALUES(json_data), data_hash = VALUES(data_hash), update_time = CURRENT_TIMESTAMP"""
                    cursor.execute(sql, (DB_STORAGE_KEY_REGIONVO, json_str, data_hash))
                    conn.commit()
                    logging.info(f"{log_msg_prefix} 的JSON数据已成功保存/更新到数据库。")
                    data_updated_in_db = True
        except pymysql.Error as e:
            logging.error(f"保存RegionVo结构JSON到DB时出错: {e}")
            if conn:
                conn.rollback()
                raise
        except Exception as e:
            logging.error(f"保存RegionVo结构JSON到DB时一般错误: {e}")
            if conn:
                conn.rollback()
                raise
        finally:
            if conn:
                conn.close()
                return (data_updated_in_db)

    def _flatten_region_vo_for_db_insert(self, region_vo_node_list):
        inserts_by_level = {1: [], 2: [], 3: []}

        def recurse_flatten(nodes, current_parent_code, current_level):  # 参数名保持一致
            if current_level > 3:
                return
            for node in nodes:
                if node and node.get('code') and node.get('name') and node.get('level') == current_level:
                    inserts_by_level[current_level].append((
                        node['code'],
                        node['name'],
                        current_parent_code,
                        current_level
                    ))
                    sub_nodes = node.get("subLevelModelList")
                    if isinstance(sub_nodes, list):
                        recurse_flatten(sub_nodes, node['code'], current_level + 1)

        # 使用位置参数调用，或者与定义匹配的关键字参数
        recurse_flatten(region_vo_node_list, None, 1)

        return inserts_by_level[1], inserts_by_level[2], inserts_by_level[3]


    def process_and_insert_data_into_region(self, cities_region_vo_data_list):
        all_level1_inserts, all_level2_inserts, all_level3_inserts = self._flatten_region_vo_for_db_insert(
            cities_region_vo_data_list)
        conn = None
        try:
            conn = pymysql.connect(**DB_CONFIG)
            conn.autocommit(False)
            with conn.cursor() as cursor:
                logging.info("准备清空并重新插入数据到 region 表 (基于RegionVo结构)...")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute("TRUNCATE TABLE region")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                logging.info("region 表已清空。")

                insert_sql = "INSERT INTO region (code, name, parent_code, level) VALUES (%s, %s, %s, %s)"
                if all_level1_inserts:
                    cursor.executemany(insert_sql, all_level1_inserts)
                    logging.info(f"已插入 {len(all_level1_inserts)} 条市级数据 (level 1)。")
                if all_level2_inserts:
                    cursor.executemany(insert_sql, all_level2_inserts)
                    logging.info(f"已插入 {len(all_level2_inserts)} 条区级数据 (level 2)。")
                if all_level3_inserts:
                    cursor.executemany(insert_sql, all_level3_inserts)
                    logging.info(f"已插入 {len(all_level3_inserts)} 条街道/商圈级数据 (level 3)。")

                conn.commit()
                total_inserted = len(all_level1_inserts) + len(all_level2_inserts) + len(all_level3_inserts)
                logging.info(f"所有数据已成功插入到 region 表。共插入 {total_inserted} 条记录。")
        except pymysql.Error as e:
            logging.error(f"数据库操作 (region表) 出错: {e}")
            if conn: conn.rollback()
            raise
        except Exception as e:
            logging.error(f"处理并插入数据到 region 表时发生一般错误: {e}")
            if conn: conn.rollback()
            raise
        finally:
            if conn:
                conn.close()


def main():
    logging.info("=== Boss Zhipin区域数据抓取脚本启动 ===")
    logging.warning("重要提示: 请确保配置文件中的 COOKIES 和 BASE_HEADERS 是最新的，否则请求很可能会失败。")
    logging.warning(f"临时数据文件将尝试从 '{TEMP_DATA_FILE}' (RegionVo结构) 加载/保存。")  # 更新日志
    logging.warning(f"如果临时数据文件超过 {MAX_DATA_AGE_HOURS} 小时，将强制重新爬取。")

    crawler = BossZhipinCrawler()
    all_cities_region_vo_list = []  # 直接使用转换后的列表
    force_recrawl = False
    source_data_is_newly_fetched = False

    # --- 检查点逻辑：尝试从临时文件加载转换后的RegionVo数据 ---
    if os.path.exists(TEMP_DATA_FILE) and not force_recrawl:
        try:
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(TEMP_DATA_FILE))
            if datetime.now() - file_mod_time < timedelta(hours=MAX_DATA_AGE_HOURS):
                logging.info(f"发现有效的临时RegionVo数据文件 (修改于 {file_mod_time}), 尝试加载...")
                all_cities_region_vo_list = crawler._load_converted_data_from_temp_file(TEMP_DATA_FILE)  # 加载转换后数据
                if all_cities_region_vo_list:
                    logging.info("成功从临时文件加载转换后的RegionVo数据，将跳过爬虫和转换步骤。")
                else:
                    logging.warning("临时RegionVo数据文件无效或解析失败，将执行爬虫和转换。")
                    all_cities_region_vo_list = []  # 确保如果加载失败，列表是空的
            else:
                logging.info(f"临时RegionVo数据文件已超 {MAX_DATA_AGE_HOURS} 小时，将执行爬虫和转换。")
                all_cities_region_vo_list = []
        except Exception as e:
            logging.warning(f"检查临时RegionVo数据文件时发生错误: {e}。将执行爬虫和转换。")
            all_cities_region_vo_list = []

    # --- 爬虫和转换逻辑：如果未能从临时文件加载转换后数据，则执行 ---
    if not all_cities_region_vo_list:  # 注意这里条件变化了
        source_data_is_newly_fetched = True
        all_cities_district_data_raw = None  # 存储原始API数据
        try:
            # 步骤1: 获取城市列表
            logging.info("--- 步骤 1: 获取城市列表 ---")
            city_list = crawler.fetch_city_list()
            if not city_list: logging.error("未能获取城市列表，终止。"); return
            logging.info(f"成功获取 {len(city_list)} 个城市基础信息。")

            # 步骤2: 批量获取各城市的商圈原始数据
            logging.info(f"--- 步骤 2: 批量获取 {len(city_list)} 个城市商圈原始数据 ---")
            all_cities_district_data_raw = crawler.batch_fetch_district_data(city_list)

            # 步骤2.5: 将原始数据转换为RegionVo结构列表
            logging.info("--- 步骤 2.5: 将原始数据转换为RegionVo结构列表 ---")
            if all_cities_district_data_raw:  # 确保有原始数据才转换
                for city_entry_raw in all_cities_district_data_raw:
                    if city_entry_raw['status'] == 'success' and city_entry_raw.get('data'):
                        region_vo_city = crawler._convert_to_region_vo_structure(city_entry_raw['data'], level=1,
                                                                                 parent_code=None)
                        if region_vo_city:
                            all_cities_region_vo_list.append(region_vo_city)
                    elif city_entry_raw['status'] == 'empty_data':
                        empty_city_vo = {
                            "code": str(city_entry_raw['city_code']),
                            "name": str(city_entry_raw['city_name']).replace("'", "''"),
                            "level": 1, "parentCode": None, "subLevelModelList": []
                        }
                        all_cities_region_vo_list.append(empty_city_vo)
                logging.info(f"数据已转换为 {len(all_cities_region_vo_list)} 个顶层RegionVo结构。")
            else:
                logging.warning("没有原始数据可供转换为RegionVo结构。")

            # 将转换后的RegionVo数据保存到临时文件
            if all_cities_region_vo_list:
                crawler._save_converted_data_to_temp_file(all_cities_region_vo_list, TEMP_DATA_FILE)
            else:
                logging.warning("转换后RegionVo列表为空，不保存临时文件。")

        except Exception as e:
            logging.error(f"爬虫或数据转换步骤执行失败: {e}", exc_info=True)
            logging.error("请检查日志后重试。");
            return

    # --- 后续数据处理步骤：仅当有转换后的RegionVo数据时执行 ---
    if all_cities_region_vo_list:
        data_source_changed_in_db = False
        try:
            # 步骤3: 将转换后的RegionVo结构数据保存到永久文件和数据库的city_json_storage表
            logging.info("--- 步骤 3: 保存RegionVo结构数据到永久文件和数据库的json_storage表 ---")
            data_source_changed_in_db = crawler._save_regionvo_json_to_permanent_storage(all_cities_region_vo_list)

            # 统计数据可以基于转换后的 all_cities_region_vo_list 的长度，或保持基于原始数据统计
            # 这里我们假设原始数据的统计更有意义，因为它反映了API的直接响应
            if 'all_cities_district_data_raw' in locals() and all_cities_district_data_raw is not None:
                successful_fetches = sum(1 for d in all_cities_district_data_raw if d['status'] == 'success')
                empty_data_fetches = sum(1 for d in all_cities_district_data_raw if d['status'] == 'empty_data')
                logging.info(
                    f"原始数据统计：成功获取商圈数据城市数: {successful_fetches}。无数据城市数: {empty_data_fetches}。")
            else:  # 如果是从临时文件加载的RegionVo，原始数据可能不在内存中
                logging.info(f"处理 {len(all_cities_region_vo_list)} 条RegionVo数据。")

            # 决定是否需要更新 region 表
            if source_data_is_newly_fetched or data_source_changed_in_db:
                if source_data_is_newly_fetched and not data_source_changed_in_db:
                    logging.info(
                        "数据为新爬取并转换 (但与DB中RegionVo结构json_storage内容一致)，仍将处理 region 表以确保同步。")
                elif data_source_changed_in_db:
                    logging.info("检测到RegionVo结构数据在数据库中已更新或为首次存储，将处理 region 表。")

                logging.info("--- 步骤 4: 处理数据并插入到 region 表 ---")
                crawler.process_and_insert_data_into_region(all_cities_region_vo_list)
            else:
                logging.info(
                    "RegionVo结构数据未发生变化 (从临时文件加载且与DB中json_storage内容一致)，跳过 region 表的处理和插入。")

            logging.info("=== 数据处理全部完成！脚本执行成功。 ===")
        except Exception as e:
            logging.error(f"数据保存或插入DB步骤失败: {e}", exc_info=True)
            logging.error("请检查日志后重试。上次转换的数据已存临时文件，若有效可用于下次运行。")
    else:
        logging.error("未能获取、加载或转换任何有效的RegionVo数据，无法后续处理。终止。")


if __name__ == "__main__":
    try:
        import cryptography
    except ImportError:
        logging.warning(
            "可选依赖 'cryptography' 未安装。如果连接 MySQL 8.x 或使用特定认证方式，可能需 'pip install cryptography'。")
    main()