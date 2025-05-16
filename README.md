# Boss直聘区域数据抓取脚本

本项目包含一个Python脚本，用于从Boss直聘网站获取中国的城市、行政区及商圈（街道）层级数据，并将这些数据处理后存入MySQL数据库。

## 功能特性

-   **城市列表获取**：自动从API获取最新的城市列表及其代码。
-   **商圈数据抓取**：并发抓取每个城市的详细行政区划和商圈数据。
-   **数据持久化与缓存**：
    -   将原始抓取的商圈数据聚合后，计算哈希值并存入数据库的 `city_json_storage` 表，用于检测数据变化。
    -   数据还会保存到本地的临时JSON文件 (`temp_crawled_data.json`)，在设定的有效期内（默认6小时）可用于跳过重复爬取。
    -   原始数据也会备份到永久性的JSON文件 (`business_districts_all.json`)。
-   **层级数据入库**：
    -   解析抓取的原始数据，提取市、区、街道三级行政单位。
    -   清空并重新批量插入到 `region` 数据库表，包含代码、名称、父级代码和层级信息。
    -   **智能更新**：只有当原始JSON数据是新爬取的，或者与 `city_json_storage` 中存储的内容相比发生了变化时，才会更新 `region` 表。
-   **健壮性设计**：
    -   使用 `curl_cffi` 模拟浏览器请求，配置请求头和Cookie。
    -   多线程并发提高效率，同时设置请求间隔避免IP封锁。
    -   详细的日志记录（包括文件和控制台输出）。
    -   完善的错误处理和数据库事务管理。
-   **可配置性**：大部分关键参数（如API地址、数据库连接、请求头、Cookie、并发数、请求间隔、临时文件有效期等）均可在脚本开头的配置区进行修改。

## 环境准备与依赖

在运行脚本之前，请确保你的环境满足以下要求：

1.  **Python**: Python 3.7 或更高版本。
2.  **MySQL数据库**: 一个可用的MySQL数据库实例。
3.  **Python依赖包**:
    -   `curl_cffi`
    -   `tqdm`
    -   `pymysql`
    -   `cryptography` (连接某些MySQL版本时可能需要，如MySQL8.x与MySQL5.x的密码策略不一样)

    可以通过以下命令安装所有必要的Python依赖：
    ```bash
    pip install curl_cffi tqdm pymysql cryptography
    ```

## 数据库表结构

脚本需要以下两张MySQL数据表。请在运行脚本前创建它们：

1.  **`city_json_storage`** (用于存储原始JSON数据及其哈希值)
    ```sql
    CREATE TABLE IF NOT EXISTS city_json_storage (
        data_key VARCHAR(255) PRIMARY KEY,
        json_data LONGTEXT, -- 对于非常大的JSON，使用 LONGTEXT
        data_hash VARCHAR(64) UNIQUE,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
    ```

2.  **`region`** (用于存储层级化的区域数据)
    ```sql
    CREATE TABLE IF NOT EXISTS region (
        code VARCHAR(50) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        parent_code VARCHAR(50) DEFAULT NULL,
        level INT NOT NULL,
        INDEX idx_parent_code (parent_code),
        INDEX idx_level (level),
    );
    ```

## 配置脚本

在运行脚本前，你需要修改脚本文件 (`your_script_name.py`) 开头的 **用户配置区域**：

1.  **`DB_CONFIG`**:
    -   `host`: 数据库服务器地址。
    -   `user`: 数据库用户名。
    -   `password`: 数据库密码。
    -   `database`: 要使用的数据库名称。
    -   `port`: 数据库端口

2.  **`BASE_HEADERS` 和 `COOKIES`**:
    -   **至关重要**: 这些值需要从你的浏览器开发者工具中获取。请登录Boss直聘网站，然后检查网络请求（例如对 `city.json` 或 `businessDistrict.json` 的请求），复制相应的请求头和Cookie到脚本中。
    -   确保包含所有必要的认证令牌，如 `zp_token` (可能在 `bst` cookie中或直接作为header), `__zp_stoken__` 等。
    -   **注意**：这些Headers和Cookies具有时效性，如果脚本运行失败提示认证错误或返回空数据，很可能是这些值已过期，需要重新获取。

3.  **其他可选配置**:
    -   `OUTPUT_DIR`: 输出文件的目录。
    -   `REQUEST_INTERVAL`: 请求间隔。
    -   `MAX_WORKERS`: 并发线程数。
    -   `MAX_DATA_AGE_HOURS`: 临时数据文件有效期。
    -   `DEBUG_JSON_OUTPUT`: 是否在哈希不匹配时输出用于调试的JSON文件。

## 运行脚本

配置完成后，在你的终端中导航到脚本所在的目录，然后运行：

```bash
python your_script_name.py
