#import pymysql
import requests
import pandas as pd
import math
from database.db_config import db_config_rpa
import json
from sqlalchemy import create_engine,text
import urllib
from datetime import datetime, timedelta
import time
import random
#import numpy as np
from SeleniumDriver.driver2 import TianMao
import os
import urllib.parse
from loguru import logger
import io
import re

# 日志配置模块
# --- 1. 配置日志文件路径和名称 ---
LOG_DIR = "logs"
FILES_DIR = "files"
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
logger.info(DIR_PATH)
LOG_DIR = os.path.join(DIR_PATH, LOG_DIR)
FILES_DIR = os.path.join(DIR_PATH, FILES_DIR)
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
if not os.path.exists(FILES_DIR):
    os.makedirs(FILES_DIR)
# 设置日志文件的基础名称
log_file_name = os.path.join(LOG_DIR, "Beie_Tmall_Channel_Main_{time:YYYY-MM-DD}.log")

# --- 2. 移除默认配置并添加新的配置 ---
# logger.remove()
# --- 3: 输出到文件

logger.add(
    sink=log_file_name,
    level="INFO",
    encoding="utf-8",
    rotation="00:00",  # 每天 0 点创建一个新文件（实现按日分割）
    retention="7 days",  # 只保留最近 7 天的日志文件
    enqueue=True,  # 启用多进程/多线程安全队列（推荐）
    compression="zip",  # 对旧日志文件进行压缩 (可选)
)

def get15day(rq):
    """获取指定日期前15天的日期"""
    start_date = (datetime.strptime(rq, "%Y-%m-%d") - timedelta(days=16)).strftime("%Y-%m-%d")
    return start_date

def pct_to_float(x):
    """将百分比字符串清洗函数"""
    if isinstance(x, str) and x.endswith('%'):
        try:
            return float(x.replace('%', '')) / 100
        except:
            return 0
    return x

#日期设置
# rq = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # 当前日期的前一天
rq = '2026-03-20'
riqi = [get15day(rq), rq]

# 单天处理的日期（用于基础数据和商品数据）
single_date = rq
logger.info(f"数据获取日期: {single_date}")
logger.info(f"推广15天日期范围: {riqi[0]} 至 {riqi[1]}")

#时间戳函数
r_time = int(time.time() * 1000)

def tjqd(x):
    """推广渠道名称映射函数"""
    if x == '精准人群推广':
        x = '引力魔方'
    elif x == '人群推广':
        x = '引力魔方'
    elif x == '拉新快':
        x = '引力魔方'
    elif x == '店铺直达':
        x = '品销宝'
    elif x == '关键词推广':
        x = '直通车'
    elif x == '超级短视频':
        x = '超级短视频_万相台'
    elif x == '超级直播':
        x = '超级直播_万相台'
    elif x=='货品全站推':
        x ='全站推广'
    else:
        x = '万相台'
    return x

def create_db_engine():
    """仅创建并返回 SQLAlchemy Engine 对象，不执行查询"""
    config = db_config_rpa
    # 确保 pymysql 已安装并用于连接
    connection_string = f"mysql+pymysql://{config['user']}:{urllib.parse.quote_plus(config['password'])}@{config['host']}:{config['port']}/{config['database']}?charset=utf8"
    # 创建引擎
    db = create_engine(connection_string) 
    return db

def database(sql):
    """数据库连接函数"""
    config = db_config_rpa
    connection_string = f"mysql+pymysql://{config['user']}:{urllib.parse.quote_plus(config['password'])}@{config['host']}:{config['port']}/{config['database']}?charset=utf8"
    db = create_engine(connection_string) 
    try:
        with db.connect() as conn:
            a = pd.read_sql(sql, conn)
    except Exception as e:
        logger.error(f"[ERROR] 执行 SQL 查询失败: {e}")
        a = pd.DataFrame()
    return a, db


def delete_15days(engine, table_name, shopname=None, end_date=None):
    """从 table_name 中删除 end_date 向前 15 天的数据"""
    end_date = end_date or riqi[1]
    start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=16)).strftime("%Y-%m-%d")

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        if shopname:
            sql = "DELETE FROM `{}` WHERE `店铺名称`=%s AND `推广日期` BETWEEN %s AND %s".format(table_name)
            cursor.execute(sql, (shopname, start_date, end_date))
        else:
            sql = "DELETE FROM `{}` WHERE `推广日期` BETWEEN %s AND %s".format(table_name)
            cursor.execute(sql, (start_date, end_date))
        conn.commit()
        logger.info(f" 已删除 {table_name} 中 {start_date} 至 {end_date} 的旧数据（店铺: {shopname}）")
    except Exception as e:
        logger.error(f"[ERROR] 删除数据失败: {e}")
    finally:
        try:
            cursor.close()
        except:
            pass
        conn.close()

def delete_single_day(engine, table_name, shopname=None, target_date=None):
    """从 table_name 中删除指定单天的数据"""
    target_date = target_date or single_date

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        if shopname:
            sql = "DELETE FROM `{}` WHERE `店铺名称`=%s AND `统计日期`=%s".format(table_name)
            cursor.execute(sql, (shopname, target_date))
        else:
            sql = "DELETE FROM `{}` WHERE `统计日期`=%s".format(table_name)
            cursor.execute(sql, (target_date,))
        conn.commit()
        logger.info(f" 已删除 {table_name} 中 {target_date} 的旧数据（店铺: {shopname}）")
    except Exception as e:
        logger.error(f"[ERROR] 删除单天数据失败: {e}")
    finally:
        try:
            cursor.close()
        except:
            pass
        conn.close()
        
def get_target_item_ids(engine):
    """从数据库获取需要抓取的商品ID列表"""
    try:
        with engine.connect() as connection:
            sql = text("SELECT DISTINCT `商品ID` FROM `贝易_重点品品牌词ID表`")
            df = pd.read_sql(sql, con=connection)
        if not df.empty:
            id_list = df['商品ID'].astype(str).tolist()
            return [i for i in id_list if i and i.lower() != 'nan' and i.strip() != '']
        return []
    except Exception as e:
        logger.error(f"获取商品ID列表失败: {e}")
        return []
    
def convert_search_data_format(df):
    """搜索词数据清洗与格式转换"""
    col_int = ['访客数', '支付金额', '客单价', '下单金额', '下单买家数', '支付买家数', 'UV价值', '关注店铺人数', 
               '收藏商品买家数', '加购人数', '新访客', '直接支付买家数', '收藏商品-支付买家数', '粉丝支付买家数', 
               '加购商品-支付买家数', '浏览量', '店内跳转人数', '跳出本店人数', '商品收藏人数', '支付件数']
    col_float = ['支付转化率', '下单转化率', '浏览量占比']
    
    for col in col_int:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).replace(',', '').replace('-', '0'))
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    for col in col_float:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).replace('%', '').replace('-', '0'))
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0) / 100
            
    return df

def _parse_shop_excel(content, current_date, shopname, source_name):
    """解析全店Excel"""
    try:
        df = pd.read_excel(io.BytesIO(content), header=5, engine='xlrd')
        if df.empty: return None
        
        first_col = df.columns[0]
        df = df.dropna(subset=[first_col])
        df = df[df[first_col].astype(str) != '总计']
        
        df = convert_search_data_format(df)
        df['店铺名称'] = shopname
        df['统计日期'] = current_date
        df['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['流量来源'] = source_name
        return df
    except Exception as e:
        logger.error(f"全店Excel解析失败: {e}")
        return None

def _parse_item_excel(content, current_date, item_id, source_name, shopname):
    """解析单品Excel"""
    try:
        df = pd.read_excel(io.BytesIO(content), header=5, engine='xlrd')
        if df.empty: return None
        
        if '来源名称' in df.columns:
            df = df.dropna(subset=['来源名称'])
            df = df[df['来源名称'].astype(str) != '总计']
            df.rename(columns={'来源名称': '搜索词'}, inplace=True)
        elif '关键词' in df.columns:
            df = df.dropna(subset=['关键词'])
            df = df[df['关键词'].astype(str) != '总计']
            df.rename(columns={'关键词': '搜索词'}, inplace=True)

        df = convert_search_data_format(df)
        df['店铺名称'] = shopname
        df['商品ID'] = item_id
        df['渠道'] = source_name
        df['统计日期'] = current_date
        
        if '搜索词' in df.columns:
            df['搜索词'] = df['搜索词'].apply(clean_text)

        required_cols = [
            '搜索词', '访客数', '浏览量', '浏览量占比', '店内跳转人数', '跳出本店人数', 
            '商品收藏人数', '加购人数', '下单买家数', '下单转化率', '支付件数', 
            '支付买家数', '支付转化率', '直接支付买家数', '粉丝支付买家数', 
            '收藏商品-支付买家数', '加购商品-支付买家数', '渠道', '商品ID', '统计日期', '店铺名称'
        ]
        final_cols = [c for c in required_cols if c in df.columns]
        return df[final_cols]
    except Exception as e:
        logger.error(f"单品Excel解析失败 ID {item_id}: {e}")
        return None

def get_date_range(start_date, end_date):
    """生成从start_date到end_date的日期列表"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return date_list

def dl_cookies(account, password, shopname, current_date):
    """获取和更新天猫Cookie"""
    # cookie_path = 'D:\\工作文件夹\\Cookies\\cookies.json'
    cookie_path = os.path.join(DIR_PATH,'Cookies','cookies.json')
    try:
        with open(cookie_path, 'r', encoding='utf-8') as f:
            cookies_data = json.load(f)
    except FileNotFoundError:
        cookies_data = {}
        logger.error('没有读取到cookies文件，请检查文件路径是否正确')
    
    cookies = cookies_data.get(shopname, "")
    
    headers = {
        'Authorization': "",
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=UTF-8',
        'Transit-Id': 'YXgzeASTONT2qMP9A8JEwr2NMZQmqb99W9cOaQCNCaRM60yLt+veFXVyqgXIE2b2RNG8sebc3dyJlEm09fN02vXAeogUYzsOJ6aGuKsFE1o6ROUnp/joaq/hLzw5JpNalwyCYWU0Fl45oYoMtWJyWIYBiUXyu5NgUDbCAMAxHOY=',
        'Cookie': cookies,
        'Referer': 'https://sycm.taobao.com',
        'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    }
    # 验证 Cookie
    url1 = 'https://sycm.taobao.com/flow/new/guide/trend/overview.json?' \
           'dateType=day&dateRange={}%7C{}&device=0&_={}&token=bcd7e98bd'.format(current_date, current_date, r_time)
    try:
        res5 = requests.get(url1, headers=headers)
        code = res5.json().get('code')
    except:
        code = -1
    if code == 0:
        pass 
    else:
        logger.info (f"Cookie 失效 (code={code})，调用 Driver2 进行自动化登录...")
        success = TianMao(account, password, shopname).shengyidriver()
        if success:
            logger.info("自动化登录成功,最新Cookie已保存...")
            # with open(cookie_path, 'r', encoding='utf-8') as f:
            #     cookies_data = json.load(f)
        else:
            logger.error("自动化登录失败")    
    logger.info('cookies 校验完成')
    
def dl_mmcookies(account, password, shopname):
    """获取和更新阿里妈妈Cookie"""
    # cookie_path = 'D:\\工作文件夹\\Cookies\\mmcookies.json'
    cookie_path = os.path.join(DIR_PATH,'Cookies','mmcookies.json')
    try:
        with open(cookie_path, 'r', encoding='utf-8') as f:
            cookies_data = json.load(f)
    except FileNotFoundError:
        cookies_data = {}
        logger.error('没有读取到cookies文件，请检查文件路径是否正确')
        
    cookies = cookies_data.get(shopname, "")
    try:
        token = extract_tb_token(cookies)
    except:
        token = ""
    url2 = 'https://ad.alimama.com/openapi/param2/1/gateway.unionadv/data.home.overview.json?' \
          't={}&_tb_token_={}&startDate={}&endDate={}&type=cps&split=0&period=1d'.format(r_time, token, riqi[1], riqi[1])
          
    header = {
        'Authorization': "",
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': cookies,
        'Referer': 'https://ad.alimama.com/portal/v2/report/promotionDataPage.htm',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    }
    try:
        res = requests.get(url2, headers=header, allow_redirects=False)
        code = res.json().get('resultCode')
    except:
        code = -1
    if code == 200:
        pass
    else:
        logger.info(f"阿里妈妈 Cookie 失效 (code={code})，调用 Driver2 进行自动化登录...")
        success = TianMao(account, password, shopname).alimmdriver()
        if success:
            logger.info("阿里妈妈自动化登录成功，最新Cookie已保存")
            # with open(cookie_path, 'r', encoding='utf-8') as f:
            #     cookies_data = json.load(f)
        else:
            logger.error("阿里妈妈登录失败")       
    logger.info('mmcookies 校验完成')

#采用cookies预检方案，避免无法启动并发
def ensure_cookies(account, password, shopname, current_date):
    """cookies预检方案"""
    logger.info('开始进行cookies预检...')
    #1.预检阿里妈妈生意参谋cookies
    try:
        dl_cookies(account, password, shopname,current_date)
        logger.info('生意参谋cookies预检完成')
    except Exception as e:
        logger.error('生意参谋cookies预检失败')
    #2.预检阿里妈妈cookies
    try:
        dl_mmcookies(account, password, shopname)
        logger.info('阿里妈妈cookies预检完成')
    except Exception as e:
        logger.error('阿里妈妈cookies预检失败')
        
#替代正则提取tb_token 防止过度资源消耗
def extract_tb_token(cookies_str):
    """不使用正则提取tb_token"""
    if not cookies_str:
        return ""
        
    #1.按照分号拆分每个cookie项
    parts = cookies_str.split(';')
    for part in parts:
        #2.去掉首尾空格
        part = part.strip()
        #3.匹配开头
        if part.startswith('_tb_token_='):
            # 4. 截取等号后面的值 ()
            return part.split('=', 1)[1]  
    return ""

def clean_text(text):
    """移除字符串中的4字节UTF-8字符（如emoji）"""
    if isinstance(text, str):
        # 保留所有U+0000到U+FFFF范围内的字符，移除范围外的
        return re.sub(r'[^\u0000-\uFFFF]', '', text)
    return text


# 1.生意参谋数据
def _fetch_sycm_shop_basic(session, current_date, shopname):
    """获取店铺基础数据"""
    logger.info(" 开始获取店铺基础数据...")
    url3 = 'https://sycm.taobao.com/flow/new/guide/trend/overview.json?' \
           'dateType=day&dateRange={}%7C{}&device=0&_={}&token=bcd7e98bd'.format(current_date, current_date, r_time)
    
    try:
        res3 = session.get(url3)
        res_note_data = res3.json().get('data')
        
        if res_note_data:
            p_data = {}
            for key, value in res_note_data.items():
                p_data[key] = value.get('value')
            df = pd.json_normalize(p_data, max_level=0)
            df.rename({'cartByrCnt': '加购人数', 'pv': '曝光量', 'itmCltByrCnt': '商品收藏买家数',
                       'payOldByrCnt': '支付老买家数', 'newUv': '新访客数', 'itmUv': '商品访客数',
                       'uv': '访客数', 'payByrCnt': '支付买家数', 'oldUv': '老访客数',
                       'payAmt': '支付金额', 'payPct': '客单价', 'crtByrCnt': '下单买家数',
                       'newPayByrCnt':'新支付买家数','liveRoomUv':'直播间访客数','uv3d':'3天内访客数','bounceRate':'跳失率',
                       'itmAvgPv':'商品平均浏览量','stayTime':'停留时间','shortVideoUv':'短视频访客数','payRate':'支付转化率',
                       'shopCltByrCnt':'店铺收藏买家数','imageUv':'图片访客数','itmStayTime':'商品页停留时间','itmJmpRate':'商品跳出率',
                       'uvValue':'UV价值','itmPv':'商品浏览量','avgPv':'平均浏览量','miniDetailUv':'迷你详情页访客数','shopVisitUv':'店铺访客数','crtRate':'下单转化率'}, axis=1, inplace=True)
            df['店铺名称'] = shopname
            df['统计日期'] = current_date
            df['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 退款金额
            try:
                url_tk = 'https://sycm.taobao.com/portal/coreIndex/new/overview/v3.json?needCycleCrc=true&dateType=day&' \
                         'dateRange={}%7C{}&_={}&token=bcd7e98bd'.format(current_date, current_date, r_time)
                res_t = session.get(url_tk)
                res_t_data = res_t.json().get('content', {}).get('data', {}).get('self', {}).get('rfdSucAmt')
                if res_t_data:
                    df['退款金额'] = res_t_data.get('value')
            except Exception as e:
                logger.error(f"[ERROR] 获取退款金额失败: {e}")

            #入库
            try:
                engine = create_db_engine()
                df.to_sql('贝易_天猫基础数据', con=engine, index=False, if_exists="append")
                logger.info(f"[SUCCESS] 店铺基础数据保存成功: {current_date}")
                return True
            except Exception as e:
                logger.error(f"[ERROR] 店铺基础数据入库失败: {e}")
                return False
        else:
            logger.warning(f"[WARNING] 未获取到店铺基础数据: {current_date}")
            return False
    except Exception as e:
        logger.error(f"[ERROR] 店铺基础数据接口请求异常: {e}")
        return False

def _fetch_sycm_item_ranking(session, current_date, shopname):
    """获取店铺商品排行数据"""
    logger.info(" 开始获取店铺商品数据...")
    url7 = ("https://sycm.taobao.com/cc/item/view/top.json?dateRange={}%7C{}&dateType=day&pageSize=10&page=1&order=desc&orderBy=payAmt&device=0&compareType=cycle&keyword=&follow=false&cateId=&cateLevel=&indexCode=payAmt%2CsucRefundAmt%2CpayItmCnt%2CitemCartCnt%2CitmUv&_={}&token=79eea5dff").format(
        current_date, current_date, r_time)

    try:
        res7 = session.get(url7)
        if res7.json().get('data'):
            recordCount = res7.json().get('data').get('recordCount')
            logger.info(f"商品数据总记录数: {recordCount}")
            df_data = []
            
            for page in range(1, math.ceil(recordCount / 20) + 1):
                time.sleep(random.uniform(0, 1) + 1)
                logger.info(f"正在获取商品数据第 {page} 页")
                
                url7_page = 'https://sycm.taobao.com/cc/item/view/top.json?' \
                       'dateRange={}%7C{}&dateType=day&pageSize=20&page={}&order=desc&orderBy=payAmt&device=0&' \
                       'compareType=cycle&keyword=&follow=False&cateId=&cateLevel=&indexCode=payAmt%2' \
                       'CsucRefundAmt%2CpayItmCnt%2CpayByrCnt%2CpayRate%2CnewPayByrCnt%2CpayOldByrCnt%2ColderPayAmt%2CjuPayAmt%2CmtdPayAmt%2Cmt' \
                       'dPayItmCnt%2CytdPayAmt%2CitemStatus%2CitemCartCnt%2CitemCartByrCnt%2CitemCltByrCnt%2CvisitCartRate%2CvisitCltRa' \
                       'te%2CitmUv%2CitmPv%2CitmStayTime%2CitmBounceRate%2CseGuideUv%2CseGuidePayByrCnt%2CseGuidePayRate%2CuvAvgValue%2Cst' \
                       'arLevel001%2CitemUnitPrice1%2Cpv1dCtr&_={}&token=79eea5dff'.format(current_date, current_date, page, r_time)
                try:
                    res_p = session.get(url7_page)
                    if res_p.json().get('data') and res_p.json().get('data').get('data'):
                        p_datas = res_p.json().get('data').get('data')
                        t_data = []
                        for p_data in p_datas:
                            m_data = {}
                            for key, value in p_data.items():
                                if type(value) != dict:
                                    m_data[key] = value
                                elif key == 'item':
                                    m_data[key] = value
                                else:
                                    m_data[key] = value.get('value')
                            t_data.append(m_data)
                        df = pd.json_normalize(t_data)
                        df_data.append(df)
                except Exception as e:
                    logger.error(f"[ERROR] 获取第 {page} 页商品数据异常: {e}")
            
            if df_data:
                df = pd.concat(df_data)
                df.rename({'itemCltByrCnt': '商品收藏人数', 'payItmCnt': '支付件数', 'itmUv': '商品访客数',
                           'payByrCnt': '支付买家数', 'itemCartByrCnt': '商品加购人数', 'item.itemId': '商品ID','item.title':'商品名称','mainProductId':'主商品ID',
                           'payAmt': '支付金额','payRate':'商品支付转化率', 'newPayByrCnt': '支付新买家数','payOldByrCnt': '支付老买家数', 'itmPv': '商品浏览量','seGuideUv': '搜索引导访客数',
                           'itemCartCnt': '商品加购件数', 'sucRefundAmt': '成功退款金额','ytdPayAmt':'年累计支付金额','mtdPayAmt':'月累计支付金额','mtdPayItmCnt':'月累计支付件数',
                           'visitCartRate': '加购率','visitCltRate': '收藏率','crtAmt': '下单金额','crtByrCnt': '下单买家数','itmBounceRate':'商品详情页跳出率','payPct':'竞争力评分',
                           'crtItmQty': '下单件数','rfdSucGoodsAmt': '退款成功商品金额','itmStayTime': '商品页停留时间', 'stayTimeAvg': '平均停留时长',
                           'seGuidePayByrCnt': '搜索引导支付买家数','seGuidePayRate':'搜索引导支付转化率', 'uvAvgValue': '访客平均价值','olderPayAmt':'老买家支付金额','juPayAmt':'聚划算支付金额'}, axis=1,inplace=True)
                
                df = df[[   '商品收藏人数','支付件数','商品访客数','支付买家数','商品加购人数','商品ID','商品名称',
                            '主商品ID','支付金额','商品支付转化率','支付新买家数','支付老买家数','商品浏览量',
                            '搜索引导访客数','商品加购件数','成功退款金额','加购率','收藏率','下单金额','年累计支付金额','月累计支付金额','月累计支付件数',
                            '下单买家数','下单件数','退款成功商品金额','商品页停留时间','平均停留时长','商品详情页跳出率','竞争力评分',
                            '搜索引导支付买家数','搜索引导支付转化率','访客平均价值','老买家支付金额','聚划算支付金额']]
                df['店铺名称'] = shopname
                df['统计日期'] = current_date
                df['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                try:
                    engine = create_db_engine() 
                    df.to_sql('贝易_天猫旗舰店_商品数据源', con=engine, index=False, if_exists="append")
                    logger.info(f"[SUCCESS] 商品数据保存成功: {current_date}, 共{len(df)}条记录")
                    return True
                except Exception as e:
                    logger.error(f"[ERROR] 商品数据入库失败: {e}")
                    return False
            else:
                logger.warning(f"[WARNING] 未获取到商品数据: {current_date}")
                return False
        else:
            logger.error(f"[WARNING] 商品数据接口返回异常: {current_date}")
            return False
    except Exception as e:
        logger.error(f"[ERROR] 商品数据主请求异常: {e}")
        return False
    
    
def _fetch_sycm_search_shop(session, current_date, shopname):
    """获取全店搜索词数据"""
    logger.info(" 开始获取全店搜索词数据...")
    tasks = [('手淘搜索', '23.s1150', '30'), ('直通车', '22.2', '22.23')]
    all_data = []
    
    for source_name, pid, ppid in tasks:
        url = 'https://sycm.taobao.com/flow/gray/excel.do?' \
              'spm=a21ag.11910113.sycm-flow-shop-source-detail-list.2.788250a5L5NjtK&' \
              'dateType=day&dateRange={0}%7C{0}&device=2&belong=all&' \
              'pageId={1}&pPageId={2}&childPageType=se_keyword&' \
              '_path_=v3/excel/shop/source/detail/v3'.format(current_date, pid, ppid)
        try:
            res = session.get(url)
            if 'text/html' in res.headers.get('Content-Type', '').lower():
                logger.error(f"[{source_name}] 请求被拦截")
                continue
            df = _parse_shop_excel(res.content, current_date, shopname, source_name)
            if df is not None: all_data.append(df)
            time.sleep(random.uniform(2, 4))
        except Exception as e:
            logger.error(f"[{source_name}] 异常: {e}")

    if all_data:
        try:
            final_df = pd.concat(all_data, ignore_index=True)
            table_name = '贝易_天猫全店搜索词'
            engine = create_db_engine()
            # delete_single_day(engine, table_name, shopname, current_date)
            final_df.to_sql(table_name, con=engine, index=False, if_exists="append")
            logger.info(f"[SUCCESS] 全店搜索词数据保存成功: {current_date}, 共{len(final_df)}条")
            return True
        except Exception as e:
            logger.error(f"[ERROR] 全店搜索词入库失败: {e}")
            return False
    return False

def _save_brand_keywords(df, engine, current_date):
    """从单品搜索词中筛选品牌词并保存"""
    try:
        brand_keywords = ['贝易', 'beie']
        # 筛选包含关键词的数据
        brand_mask = df['搜索词'].apply(
            lambda x: any(k in str(x).lower() for k in brand_keywords) if pd.notna(x) else False
        )
        brand_data = df[brand_mask].copy()
        
        if not brand_data.empty:
            brand_data.rename(columns={'搜索词': '品牌词'}, inplace=True)
            delete_single_day(engine, '贝易_天猫本品品牌词', target_date=current_date)
            # 写入数据库
            brand_data.to_sql('贝易_天猫本品品牌词', con=engine, index=False, if_exists="append")
            logger.info(f"[SUCCESS] 本品品牌词提取并保存成功: {current_date}, 共{len(brand_data)}条")
        else:
            logger.info(f"日期 {current_date} 未筛选出品牌相关关键词")
    except Exception as e:
        logger.error(f"处理本品品牌词数据失败: {e}")
        
        
def _fetch_sycm_search_item(session, current_date, shopname):
    """获取单品搜索词数据"""
    logger.info(" 开始获取单品搜索词数据...")
    engine = create_db_engine()
    item_ids = get_target_item_ids(engine)
    
    if not item_ids:
        logger.warning("未获取到重点商品ID，跳过单品任务")
        return True 

    tasks_config = [('手淘搜索', '23.s1150', '30', '2'), ('直通车', '22.2', '22.23', '3')]
    all_item_data = []
    
    for idx, item_id in enumerate(item_ids):
        for source_name, pid, ppid, level in tasks_config:
            url = 'https://sycm.taobao.com/flow/excel.do?' \
                  '_path_=v3/new/excel/item/source/detail/v3&' \
                  'dateType=day&dateRange={0}%7C{0}&' \
                  'itemId={1}&device=2&' \
                  'pageId={2}&pPageId={3}&pageLevel={4}&' \
                  'childPageType=se_keyword&belong=all&order=desc&orderBy=uv'.format(
                      current_date, item_id, pid, ppid, level)
            try:
                res = session.get(url)
                if 'text/html' in res.headers.get('Content-Type', '').lower(): continue
                df = _parse_item_excel(res.content, current_date, item_id, source_name, shopname)
                if df is not None and not df.empty: all_item_data.append(df)
                time.sleep(random.uniform(1.5, 3))
            except: pass
    
    if all_item_data:
        try:
            final_df = pd.concat(all_item_data, ignore_index=True)
            table_name = '贝易_天猫单品搜索词'
            
            final_df.to_sql(table_name, con=engine, index=False, if_exists="append")
            logger.info(f"[SUCCESS] 单品搜索词数据保存成功: {current_date}, 共{len(final_df)}条")
            #保存品牌词
            _save_brand_keywords(final_df, engine, current_date)
            return True
        except Exception as e:
            logger.error(f"[ERROR] 单品搜索词入库失败: {e}")
            return False
    else:
        logger.warning("未采集到任何单品数据")
        return False

def sycm_main(account, password, seedToken, shopname, current_date, target_modules=None):
    """
    天猫店铺数据获取入口 - 支持增量重跑
    """
    try:
        # 1. 定义子任务
        all_tasks = {
            'basic': {'func': _fetch_sycm_shop_basic, 'name': '基础数据'},
            'item':  {'func': _fetch_sycm_item_ranking, 'name': '商品数据'},
            'search_shop': {'func': _fetch_sycm_search_shop, 'name': '全店搜索词'},
            'search_item': {'func': _fetch_sycm_search_item, 'name': '单品搜索词'}
        }

        # 2. 确定要跑的模块
        if target_modules:
            tasks_to_run = {k: v for k, v in all_tasks.items() if k in target_modules}
            logger.info(f"生意参谋数据下载-增量: {[v['name'] for v in tasks_to_run.values()]}")
        else:
            tasks_to_run = all_tasks
            logger.info(f" 开始获取生意参谋数据-全量下载，日期: {current_date}")

        # 3. 初始化 Cookie
        try:
            with open(os.path.join(DIR_PATH,'Cookies','cookies.json'), 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            cookies = cookies_data.get(shopname)
        except Exception as e:
            return {'code': 500, 'result': False, 'msg': f'Cookie读取失败: {str(e)}', 'method_name': 'sycm_main'}

        header = {
            'Authorization': "",
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Transit-Id': 'YXgzeASTONT2qMP9A8JEwr2NMZQmqb99W9cOaQCNCaRM60yLt+veFXVyqgXIE2b2RNG8sebc3dyJlEm09fN02vXAeogUYzsOJ6aGuKsFE1o6ROUnp/joaq/hLzw5JpNalwyCYWU0Fl45oYoMtWJyWIYBiUXyu5NgUDbCAMAxHOY=',
            'Cookie': cookies,
            'Referer': 'https://sycm.taobao.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }
        session = requests.Session()
        session.headers.update(header)
        
        # 4. 循环执行任务
        failed_keys = []
        success_names = []

        for key, task_info in tasks_to_run.items():
            func = task_info['func']
            name = task_info['name']
            
            try:
                # 执行子函数
                res = func(session, current_date, shopname)
                
                if res:
                    success_names.append(name)#成功
                else:
                    failed_keys.append(key)#失败
            except Exception as e:
                logger.error(f"[ERROR] 模块 [{name}] 异常: {e}")
                failed_keys.append(key)

        # 5. 返回结果
        if not failed_keys:
            return {
                'code': 200,
                'result': True,  # 成功，移出重跑队列
                'msg': f'生意参谋数据全部成功: {", ".join(success_names)}',
                'method_name': 'sycm_main',
                'missing_info': []
            }
        else:
            missing_names = [all_tasks[k]['name'] for k in failed_keys]
            return {
                'code': 200,
                'result': False, # 触发重跑
                'msg': f'数据未全，缺失: {", ".join(missing_names)}',
                'method_name': 'sycm_main',
                'missing_info': failed_keys # 关键：告诉调度器下次只跑这些
            }
            
    except Exception as e:
        logger.error(f"[ERROR] 获取生意参谋数据异常: {e}")
        return {'code': 500, 'result': False, 'msg': str(e), 'method_name': 'sycm_main','missing_info': []}





# 2.推广数据
def _init_alimama_session(account, password, shopname, end_date):
    """初始化阿里妈妈Session和Token"""
    try:
        # dl_cookies(account, password, shopname, end_date)
        
        with open(os.path.join(DIR_PATH,'Cookies','cookies.json'), 'r', encoding='utf-8') as f:
            cookies_data = json.load(f)
        cookies = cookies_data.get(shopname)
        
        header = {
            'Authorization': "",
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Cookie': cookies,
            'Referer': 'https://one.alimama.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }
        session = requests.Session()
        session.headers.update(header)
        
        logger.info(" 获取阿里妈妈访问令牌...")
        uid = 'https://one.alimama.com/member/checkAccess.json?bizCode=universalBP'
        # uid = 'https://one.alimama.com/member/checkAccess.json'
        res = session.post(uid)
        csrfId = res.json().get('data', {}).get('accessInfo', {}).get('csrfId')
        logger.info(f" 获取到csrfId: {csrfId}")
        
        return session, csrfId
    except Exception as e:
        logger.error(f"[ERROR] 初始化 Session 异常: {e}，请检查cookies是否正常更新")
        return None, None

def _fetch_bp_summary_data(session, csrfId, start_date, end_date, shopname):
    """获取推广汇总数据 (Account Report)"""
    logger.info(" 开始获取推广汇总数据...")
    url1 = 'https://one.alimama.com/report/query.json?csrfId={}&bizCode=universalBP'.format(csrfId)
    
    base_payload = {
        "lite2": False, "bizCode": "universalBP", "fromRealTime": False, "source": "baseReport",
        "byPage": True, "totalTag": True, "rptType": "account", "pageSize": 100, "offset": 0,
        "havingList": [], "endTime": end_date, "unifyType": "zhai", "effectEqual": 15, "startTime": start_date,
        "splitType": "day",
        "queryFieldIn": ["adPv", "click", "charge", "ctr", "ecpc", "alipayInshopAmt", "alipayInshopNum", "cvr", "cartInshopNum", "itemColInshopNum", "shopColDirNum", "colNum", "itemColInshopCost"],
        "queryDomains": ["original_scene", "date"], "csrfId": csrfId
    }
    
    try:
        res1 = session.post(url1, data=json.dumps(base_payload))
        if res1.json().get('data'):
            recordCount = res1.json().get('data').get('count')
            logger.info(f" 推广汇总数据总记录数: {recordCount}")
            if recordCount == 0:
                logger.info('推广汇总数据为空')
                return False
            
            df_data = []
            for page in range(1, math.ceil(recordCount / 100) + 1):
                try:
                    time.sleep(3 + random.random())
                    logger.info(f" 正在获取推广汇总数据第 {page} 页")
                    
                    payload = base_payload.copy()
                    payload["offset"] = 100 * (page - 1)
                    
                    res_page = session.post(url1, data=json.dumps(payload))
                    p_data = res_page.json().get('data', {}).get('list')
                    
                    if p_data:
                        df = pd.json_normalize(p_data, max_level=0)
                        df_data.append(df)
                except Exception as e:
                     logger.error(f"[ERROR] 获取第 {page} 页推广汇总数据异常: {e}")
                     return False
                     

            if df_data:
                df_summary = pd.concat(df_data)
                df_summary['渠道'] = df_summary['originalSceneName'].apply(tjqd)
                df_summary['operationList'] = df_summary['operationList'].apply(lambda x: str(x).replace('[]', ''))
                df_summary.rename({
                    'alipayInshopAmt': '成交金额', 'charge': '花费', 'alipayInshopNum': '成交单量',
                    'adPv': '曝光量', 'shopColDirNum': '收藏店铺数', 'click': '点击量', 'cartInshopNum': '加购数',
                    'thedate': '推广日期', 'itemColInshopNum': '收藏商品数', 'scene1Name': '营销场景',
                    'originalSceneName':'原二级营销场景', 'ctr': '点击率', 'ecpc': '点击成本', 'cvr': '转化率', 
                    'itemColInshopCost': '收藏成本'
                }, axis=1, inplace=True)
                df_summary = df_summary[['推广日期', '渠道', '成交金额', '花费', '成交单量', '曝光量', '点击量', 
                                         '点击率', '点击成本', '收藏商品数', '收藏店铺数', '加购数', '转化率','收藏成本']]
                #加一个最大日期的判断
                try:
                    max_date_in_data = df_summary['推广日期'].max()
                except:
                    max_date_in_data = "0000-00-00"
                    
                df_summary['店铺名称'] = shopname
                df_summary['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                engine = create_db_engine() 
                df_summary.to_sql('贝易_天猫店铺推广汇总数据', con=engine, index=False, if_exists="append")
                logger.info(f"[SUCCESS] 推广汇总数据保存成功，共{len(df_summary)}条记录")
                #返回值进行判断
                if max_date_in_data >= end_date:
                    return True
                else:
                    logger.warning(f"[WARNING] 推广数据已获取，但缺失最新日期 {end_date} (当前最新: {max_date_in_data})")
                    return False
            else:
                logger.error("[WARNING] 未获取到推广汇总数据")
                return False
        else:
            logger.error("[WARNING] 推广汇总数据接口返回异常")
            return False
    except Exception as e:
        logger.error(f"[ERROR] 推广汇总数据主流程异常: {e}")
        return False

def _get_bp_standard_items(session, csrfId, url_query, start_date, end_date):
    """内部函数：获取宝贝推广数据"""
    logger.info(" 正在获取 Item Promotion (宝贝) 数据...")
    df_data_item = []
    base_payload_item = {
        "lite2": False, "bizCode": "universalBP", "fromRealTime": False, "source": "baseReport",
        "byPage": True, "totalTag": True, "rptType": "item_promotion", "pageSize": 100, "offset": 0,
        "havingList": [], "endTime": end_date, "unifyType": "zhai", "effectEqual": 15, "startTime": start_date,
        "splitType": "day", "subPromotionTypes": ["ITEM"],
        "queryFieldIn": ["adPv", "click", "charge", "ctr", "ecpc", "alipayInshopAmt", "alipayInshopNum", "cvr", "cartInshopNum", "itemColInshopNum", "shopColDirNum", "colNum", "itemColInshopCost","cartDirNum","alipayDirAmt","alipayDirNum"],
        "searchValue": "", "searchKey": "itemIdOrName",
        "queryDomains": ["promotion", "date", "campaign"], "csrfId": csrfId
    }
    
    try:
        res_item = session.post(url_query, data=json.dumps(base_payload_item))
        if res_item.json().get('data'):
            count_item = res_item.json().get('data').get('count')
            logger.info(f" 推广商品数据(宝贝)总记录数: {count_item}")
            if count_item == 0:
                logger.info('推广宝贝数据为空')
                #return False
                return pd.DataFrame()
            
            for page in range(1, math.ceil(count_item / 100) + 1):
                try:
                    time.sleep(3.14 + random.random())
                    logger.info(f" 正在获取推广商品数据(宝贝)第 {page} 页")
                    payload = base_payload_item.copy()
                    payload["offset"] = 100 * (page - 1)
                    res_page = session.post(url_query, data=json.dumps(payload))
                    p_data = res_page.json().get('data', {}).get('list')
                    if p_data:
                        df_data_item.append(pd.json_normalize(p_data))
                except Exception as e:
                    logger.error(f"[ERROR] 获取宝贝推广数据页 {page} 异常: {e}")
                    return pd.DataFrame()
    except Exception as e:
        logger.error(f"[ERROR] 获取宝贝推广数据请求异常: {e}")
        #return False
        return pd.DataFrame()
        
    return pd.concat(df_data_item) if df_data_item else pd.DataFrame()

def _get_bp_short_video_items(session, csrfId, url_query, start_date, end_date):
    """内部函数：获取超级短视频数据"""
    logger.info(" 正在获取 Other Promotion (超级短视频) 数据...")
    df_data_other = []
    base_payload_other = {
        "lite2": False, "bizCode": "universalBP", "fromRealTime": False, "source": "baseReport",
        "byPage": True, "totalTag": True, "rptType": "other_promotion", "pageSize": 100, "offset": 0,
        "havingList": [], "endTime": end_date, "unifyType": "zhai", "effectEqual": 15, "startTime": start_date,
        "splitType": "day", 
        "subPromotionTypes": ["SHORT_VIDEO"],
        "strategySubPromotionTypeNotIn": ["11"],
        "queryFieldIn": ["adPv", "click", "charge", "ctr", "ecpc", "alipayInshopAmt", "alipayInshopNum", "cvr", "cartInshopNum", "itemColInshopNum", "shopColDirNum", "colNum", "itemColInshopCost"],
        "queryDomains": ["promotion", "campaign", "date"], "csrfId": csrfId
    }

    try:
        res_other = session.post(url_query, data=json.dumps(base_payload_other))
        if res_other.json().get('data'):
            count_other = res_other.json().get('data').get('count')
            logger.info(f" 推广超级短视频总记录数: {count_other}")
            if count_other == 0:
                logger.info('超级短视频总记录数为空')
                #return False
                return pd.DataFrame()
                
            for page in range(1, math.ceil(count_other / 100) + 1):
                try:
                    time.sleep(1.08 + random.random())
                    logger.info(f" 正在获取推广商品数据(超级短视频)第 {page} 页")
                    payload = base_payload_other.copy()
                    payload["offset"] = 100 * (page - 1)
                    res_page = session.post(url_query, data=json.dumps(payload))
                    p_data = res_page.json().get('data', {}).get('list')
                    if p_data:
                        df_data_other.append(pd.json_normalize(p_data))
                except Exception as e:
                    logger.error(f"[ERROR] 获取超级短视频数据页 {page} 异常: {e}")
                    #return False
                    return pd.DataFrame()
    except Exception as e:
        logger.error(f"[ERROR] 获取超级短视频数据请求异常: {e}")
        #return False
        return pd.DataFrame()

    return pd.concat(df_data_other) if df_data_other else pd.DataFrame()

def _fetch_bp_item_data(session, csrfId, start_date, end_date, shopname):
    """获取推广商品数据主流程"""
    logger.info(" 开始获取推广商品数据(宝贝主体 + 超级短视频)...")
    url_query = 'https://one.alimama.com/report/query.json?csrfId={}&bizCode=universalBP'.format(csrfId)
    
    all_product_data = []

    # 1. 获取 Item Promotion (宝贝推广)
    df_items = _get_bp_standard_items(session, csrfId, url_query, start_date, end_date)
    if not df_items.empty:
        df_items['渠道'] = df_items['originalSceneName'].apply(tjqd)
        df_items['商品ID'] = df_items['promotionId']
        df_items['promotions'] = df_items['promotions'].apply(str)
        df_items['operationList'] = df_items['operationList'].apply(str)
        all_product_data.append(df_items)

    # 2. 获取 Other Promotion (超级短视频)
    df_other = _get_bp_short_video_items(session, csrfId, url_query, start_date, end_date)
    if not df_other.empty:
        df_other['promotions'] = df_other['promotions'].apply(str)
        df_other['operationList'] = df_other['operationList'].apply(str)
        df_other['渠道'] = df_other['originalSceneName'].apply(tjqd)
        all_product_data.append(df_other)

    # 3. 合并数据入库
    if all_product_data:
        try:
            df_products = pd.concat(all_product_data)
            df_products.rename({
                'alipayInshopAmt': '成交金额', 'charge': '花费', 'alipayInshopNum': '成交单量',
                'adPv': '曝光量', 'shopColDirNum': '收藏店铺数', 'click': '点击量', 'cartInshopNum': '加购数','cartDirNum':'直接加购数','alipayDirAmt':'直接成交金额','alipayDirNum':'直接成交笔数',
                'thedate': '推广日期', 'itemColInshopNum': '收藏商品数', 'scene1Name': '营销场景',
                'campaignId': '计划ID', 'campaignName': '计划名称', 'colNum': '总收藏数'
            }, axis=1, inplace=True)
            
            df_products = df_products[['推广日期','商品ID','计划ID','计划名称','渠道','成交金额','花费','成交单量','曝光量','点击量',
                                       '收藏店铺数','收藏商品数','总收藏数','加购数','直接加购数','直接成交金额','直接成交笔数']]
            df_products['店铺名称'] = shopname
            df_products['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            engine = create_db_engine()
            df_products.to_sql('贝易_天猫店铺推广商品数据', con=engine, index=False, if_exists="append")
            logger.info(f"[SUCCESS] 推广商品数据保存成功，共{len(df_products)}条记录")
            return True
        except Exception as e:
            logger.error(f"[ERROR] 推广商品数据入库失败: {e}")
            return False
    else:
        logger.warning("[WARNING] 未获取到推广商品数据")
        return False

def _fetch_super_live_data(session, csrfId, start_date, end_date, shopname):
    """获取超级直播明细数据"""
    logger.info(" 开始获取超级直播数据...")
    url_live = 'https://one.alimama.com/report/query.json?csrfId={}&bizCode=onebpLive'.format(csrfId)
    
    base_payload = {
        "bizCode": "onebpLive",
        "bizCodeIn": ["adStrategyDuration", "adStrategyDurationGas", "fastLive"],
        "fromRealTime": False, "source": "baseReport", "byPage": True, "totalTag": True,
        "rptType": "live_migrate", "pageSize": 100, "offset": 0, "havingList": [],
        "endTime": end_date, "unifyType": "zhai", "effectEqual": 15, "startTime": start_date, "splitType": "day",
        "strategyOptimizeTargetIn": "1036,1025,1024,1055,1026,1029,1030,1061,1060,1028,1027,1053,1031,1032,1033,1082,1083",
        "strategyOptimizeTargetNotIn": [1077],
        "queryFieldIn": [
            "LivePromotionScene", "LiveOptimizeTarget", "LiveSmoothType", "LiveDayBudget", "LiveBidType",
            "LiveGmtCreate", "LiveLaunchTime", "charge", "liveViewNum", "liveViewCost", "adPv", "liveViewRate",
            "alipayInshopNum", "alipayInshopAmt", "alipayDirAmt", "alipayDirNum", "roi", "dirRoi", "click", "cartInshopNum"
        ],
        "queryDomains": ["campaign", "date"], "csrfId": csrfId
    }

    try:
        res_live = session.post(url_live, data=json.dumps(base_payload))
        if res_live.json().get('data'):
            recordCount = res_live.json().get('data').get('count')
            logger.info(f" 超级直播数据总记录数: {recordCount}")
            if recordCount == 0:
                #对超级直播数据进行判断
                logger.info('超级直播数据为空，无需抓取')
                return False
            
            df_data = []
            for page in range(1, math.ceil(recordCount / 100) + 1):
                try:
                    time.sleep(1 + random.random())
                    logger.info(f" 正在获取超级直播数据第 {page} 页")
                    payload = base_payload.copy()
                    payload["offset"] = 100 * (page - 1)
                    
                    res_page = session.post(url_live, data=json.dumps(payload))
                    p_data = res_page.json().get('data', {}).get('list')
                    if p_data:
                        df_data.append(pd.json_normalize(p_data, max_level=0))
                except Exception as e:
                    logger.error(f"[ERROR] 获取超级直播数据页 {page} 异常: {e}")
            
            if df_data:
                df_live = pd.concat(df_data)
                df_live['operationList'] = df_live['operationList'].apply(lambda x: str(x).replace('[]', ''))
                df_live.rename({
                    'alipayInshopAmt': '成交金额', 'charge': '花费', 'click': '点击量',
                    'alipayInshopNum': '成交单量', 'alipayDirAmt': '直接成交金额', 'alipayDirNum': '直接成交笔数',
                    'adPv': '曝光量', 'cartInshopNum': '加购数', 'thedate': '推广日期',
                    'liveViewNum': '观看次数', 'liveViewCost': '观看成本', 'liveViewRate': '观看率',
                    'roi': '投入产出比', 'dirRoi': '直接投入产出比', 'LivePromotionScene': '直播推广场景',
                    'LiveOptimizeTarget': '直播优化目标', 'LiveSmoothType': '直播投放方式', 'LiveDayBudget': '直播日预算',
                    'LiveBidType': '直播出价方式', 'LiveGmtCreate': '直播创建时间', 'LiveLaunchTime': '直播投放时间',
                    'campaignName': '计划名称', 'campaignId': '计划ID', 'originalSceneName': '营销场景'
                }, axis=1, inplace=True)
                
                df_live['店铺名称'] = shopname
                df_live['渠道'] = '超级直播'
                df_live['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                required_cols = [
                    '直播推广场景','成交金额','成交单量','直播优化目标','推广日期',
                    '直播投放时间','直播投放方式','观看次数','观看成本','投入产出比',
                    '直接成交金额','直播创建时间','直播日预算','花费','点击量','曝光量',
                    '计划ID','直接成交笔数','营销场景','加购数','直播出价方式',
                    '计划名称','店铺名称','渠道','导入日期'
                ]
                existing_cols = [c for c in required_cols if c in df_live.columns]
                df_live = df_live[existing_cols]
                
                engine = create_db_engine()
                df_live.to_sql('贝易_天猫超级直播明细数据', con=engine, index=False, if_exists="append")
                logger.info(f"[SUCCESS] 超级直播数据保存成功，共{len(df_live)}条记录")
                return True
            else:
                logger.error("[WARNING] 未获取到超级直播数据")
                return False
        else:
            logger.error("[WARNING] 超级直播数据接口返回异常")
            return False
    except Exception as e:
        logger.error(f"[ERROR] 超级直播数据主请求异常: {e}")
        return False

def _fetch_shop_direct_data(session, csrfId, start_date, end_date, shopname):
    """获取店铺直达明细数据"""
    logger.info(" 开始获取店铺直达数据...")
    url_shop = 'https://one.alimama.com/report/query.json?csrfId={}&bizCode=universalBP'.format(csrfId)
    
    base_payload = {
        "bizCode": "universalBP", "originalBizCodeIn": ["onebpStarShop"],
        "fromRealTime": False, "source": "baseReport", "byPage": True, "totalTag": True,
        "rptType": "account", "pageSize": 100, "offset": 0, "havingList": [],
        "endTime": end_date, "unifyType": "zhai", "effectEqual": 15, "startTime": start_date, "splitType": "day",
        "queryFieldIn": [
            "click", "charge", "roi", "ctr", "ecpc", "alipayInshopAmt",
            "alipayInshopNum", "cartCost", "cvr", "wwNum", "newAlipayInshopUv", "cartRate"
        ],
        "queryDomains": ["original_scene", "date"], "csrfId": csrfId
    }
    
    try:
        res_shop = session.post(url_shop, data=json.dumps(base_payload))
        if res_shop.json().get('data'):
            recordCount = res_shop.json().get('data').get('count')
            logger.info(f" 店铺直达数据总记录数: {recordCount}")
            if recordCount == 0:
                #对店铺直达数据进行判断
                logger.info('店铺直达数据为空，无需抓取')
                return False
            
            df_data = []
            for page in range(1, math.ceil(recordCount / 100) + 1):
                try:
                    time.sleep(1 + random.random())
                    logger.info(f" 正在获取店铺直达数据第 {page} 页")
                    payload = base_payload.copy()
                    payload["offset"] = 100 * (page - 1)
                    
                    res_page = session.post(url_shop, data=json.dumps(payload))
                    p_data = res_page.json().get('data', {}).get('list')
                    if p_data:
                        df_data.append(pd.json_normalize(p_data, max_level=0))
                except Exception as e:
                    logger.error(f"[ERROR] 获取店铺直达数据页 {page} 异常: {e}")
            
            if df_data:
                df_shop_direct = pd.concat(df_data)
                df_shop_direct.rename({
                    'alipayInshopAmt': '成交金额', 'charge': '花费', 'alipayInshopNum': '成交单量',
                    'adPv': '曝光量', 'click': '点击量', 'roi': '投入产出比', 'ctr': '点击率',
                    'ecpc': '平均点击成本', 'cartInshopNum':'加购数', 'cartCost': '加购成本',
                    'cvr': '转化率', 'wwNum': '旺旺咨询数', 'newAlipayInshopUv': '新成交用户数',
                    'cartRate': '加购率', 'thedate': '推广日期', 'originalSceneName': '营销场景'
                }, axis=1, inplace=True)
                
                df_shop_direct['店铺名称'] = shopname
                df_shop_direct['渠道'] = '店铺直达'
                df_shop_direct['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df_shop_direct = df_shop_direct[['导入日期','渠道','店铺名称','成交金额','花费','成交单量','曝光量','点击量','投入产出比','点击率',
                                                 '平均点击成本','加购数','加购成本','转化率','旺旺咨询数','新成交用户数','加购率','推广日期']]
                engine = create_db_engine()
                df_shop_direct.to_sql('贝易_天猫店铺直达明细数据', con=engine, index=False, if_exists="append")
                logger.info(f"[SUCCESS] 店铺直达数据保存成功，共{len(df_shop_direct)}条记录")
                return True
            else:
                logger.error("[WARNING] 未获取到店铺直达数据")
                return False
        else:
            logger.error("[WARNING] 店铺直达数据接口返回异常")
            return False
    except Exception as e:
        logger.error(f"[ERROR] 店铺直达数据主请求异常: {e}")
        return False

def get_promotion_data(account, password, shopname, start_date, end_date, target_modules=None):
    """
    获取推广费用数据 - 支持增量重跑
    :param target_modules: (可选) list, 指定要跑的模块key。如果不传或为None，则跑全量。
    """
    # 1. 定义所有子模块 (Key -> 执行函数)
    all_tasks = {
        'summary': {'func': _fetch_bp_summary_data, 'name': '推广汇总'},
        'item':    {'func': _fetch_bp_item_data,    'name': '单品数据'},
        'live':    {'func': _fetch_super_live_data, 'name': '超级直播'},
        'direct':  {'func': _fetch_shop_direct_data,'name': '店铺直达'}
    }

    # 2. 决定本次要跑哪些 (如果传入了 target_modules 就只跑指定的，否则跑全部)
    if target_modules:
        tasks_to_run = {k: v for k, v in all_tasks.items() if k in target_modules}
        logger.info(f" 推广数据增量重跑，锁定模块: {[v['name'] for v in tasks_to_run.values()]}")
    else:
        tasks_to_run = all_tasks
        logger.info(f" 开始获取推广数据全量，日期范围: {start_date} 至 {end_date}")

    # 3. 初始化会话 (只做一次)
    try:
        session, csrfId = _init_alimama_session(account, password, shopname, end_date)
        if not csrfId:
            return {'code': 500, 'result': False, 'msg': '无法获取 csrfId', 'method_name': 'get_promotion_data','missing_info': []}
    except Exception as e:
        return {'code': 500, 'result': False, 'msg': f'初始化会话异常: {str(e)}', 'method_name': 'get_promotion_data','missing_info': []}

    # 4. 循环执行任务
    failed_keys = [] # 记录失败的key (用于下次传参)
    success_names = [] 

    try:
        for key, task_info in tasks_to_run.items():
            func = task_info['func']
            name = task_info['name']
            
            try:
                # 执行具体的数据获取函数
                res = func(session, csrfId, start_date, end_date, shopname)
                
                if res:
                    success_names.append(name)
                else:
                    failed_keys.append(key) # 失败了，便于下次执行
            except Exception as e:
                logger.error(f"[ERROR] 模块 [{name}] 执行崩溃: {e}")
                failed_keys.append(key)

        # 5. 结果判定
        if not failed_keys:
            # 全部成功
            return {
                'code': 200, 
                'result': True, 
                'msg': f'站内推广数据本轮成功: {", ".join(success_names)}',
                'method_name': 'get_promotion_data',
                'missing_info': []
            }
        else:
            # 存在失败，返回 False
            missing_names = [all_tasks[k]['name'] for k in failed_keys]
            return {
                'code': 200, 
                'result': False, 
                'msg': f'部分模块未产出，缺失: {", ".join(missing_names)}',
                'method_name': 'get_promotion_data',
                'missing_info': failed_keys #告诉调度器下次只跑这些
            }

    except Exception as e:
        logger.error(f"[ERROR] 获取推广数据主流程异常: {e}")
        return {'code': 500, 'result': False, 'msg': f'主流程异常: {e}', 'method_name': 'get_promotion_data','missing_info': []}
        
           
        
# 3.淘宝客
def _fetch_taoke_summary(session, token, current_date, shopname):
    """获取淘客汇总数据并返回"""
    logger.info(f" 正在处理淘宝客汇总数据: {current_date}")
    url = 'https://ad.alimama.com/openapi/param2/1/gateway.unionadv/data.home.overview.json?' \
          't={}&_tb_token_={}&startDate={}&endDate={}&type=cps&split=0&period=1d'.format(
              r_time, token, current_date, current_date)
    
    try:
        res = session.get(url)
        logger.info(f"[DEBUG] {current_date} 接口状态码: {res.status_code}")
        
        if res.json().get('data') and res.json().get('data').get('result'):
            p_data = res.json().get('data').get('result')[0]
            df = pd.json_normalize(p_data)
            
            df['店铺名称'] = shopname
            df['推广日期'] = current_date
            df['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 完整的字段映射 - 横向优化
            field_mapping = {
                'pay_ord_amt_8': '成交金额', 'pay_ord_num_8': '成交单量', 'pay_ord_uv_8': '成交用户数', 'pay_itm_qty_8': '成交件数',
                'pay_ord_total_fee_8': '花费', 'pay_ord_cfee_8': '佣金花费', 'pay_ord_sfee_8': '服务费花费', 'pay_ord_cfee_rt_8': '佣金费率',
                'sett_ord_amt_8': '结算金额', 'sett_ord_num_8': '结算单量', 'sett_ord_uv_8': '结算用户数', 'sett_ord_total_fee_8': '结算总花费',
                'sett_ord_cfee_8': '结算佣金', 'sett_ord_sfee_8': '结算服务费', 'sett_ord_cfee_rt_8': '结算佣金费率',
                'conf_ord_amt_8': '确认金额', 'conf_ord_num_8': '确认单量', 'conf_ord_uv_8': '确认用户数',
                'pay_ser_ord_amt_8': '服务订单金额', 'pay_ser_ord_sfee_rt_8': '服务订单服务费率', 'sett_ser_ord_amt_8': '结算服务订单金额', 'sett_ser_ord_sfee_rt_8': '结算服务订单服务费率',
                'uclk_pv_8': '曝光量', 'uclk_uv_8': '点击量', 'uclk_itm_num_8': '点击商品数',
                'cart_itm_qty_8': '加购数', 'clt_itm_pv_8': '收藏商品数', 'send_cpn_pv_8': '发券曝光量',
                'union_cvr_8': '转化率', 'pay_pitm_qty_tfee_8': '件单价',
                'dep_ord_num_8': '定金订单数', 'dep_ord_total_amt_8': '定金总金额', 'dep_ord_dep_amt_8': '定金金额', 'dep_ord_rest_amt_8': '尾款金额',
                'dep_ord_total_cfee_8': '定金总佣金', 'dep_ord_total_cfee_rt_8': '定金佣金费率',
                'pay_bmkt_fee_8': '补差费用', 'sett_bmkt_fee_8': '结算补差费用', 'seller_id': '卖家ID', 'ds': '数据日期', 'thedate': '统计日期'
            }
            
            df.rename(columns=field_mapping,inplace=True)
            
            pct_cols1 = [col for col in df.columns if '百分比' in col]
            for col in pct_cols1:
                df[col] = df[col].apply(pct_to_float)
                
            columns_to_keep = [
                '店铺名称', '推广日期', '导入日期',
                '成交金额', '成交单量', '成交用户数', '成交件数',
                '花费', '佣金花费', '服务费花费', '佣金费率',
                '结算金额', '结算单量', '结算用户数', '结算总花费', '结算佣金', '结算服务费', '结算佣金费率',
                '确认金额', '确认单量', '确认用户数',
                '服务订单金额', '服务订单服务费率', '结算服务订单金额', '结算服务订单服务费率',
                '曝光量', '点击量', '点击商品数',
                '加购数', '收藏商品数', '发券曝光量',
                '转化率', '件单价',
                '定金订单数', '定金总金额', '定金金额', '尾款金额', '定金总佣金', '定金佣金费率',
                '补差费用', '结算补差费用', '卖家ID'
            ]
            
            existing_columns = [col for col in columns_to_keep if col in df.columns]
            df = df[existing_columns]
            
            # 入库 - 淘客汇总表
            engine = create_db_engine()
            df.to_sql('贝易_天猫店铺淘客汇总数据', con=engine, index=False, if_exists="append")
            logger.info(f" 淘宝客汇总数据保存成功: {current_date}")
            
            # 准备推广汇总表需要的数据
            need_cols = ['成交金额', '花费', '成交单量', '曝光量', '点击量', '收藏商品数', '加购数','转化率']
            exist_cols = [c for c in need_cols if c in df.columns]
            df_summary = df[exist_cols].copy()
            df_summary['店铺名称'] = shopname
            df_summary['渠道'] = '淘宝客'
            df_summary['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df_summary['推广日期'] = current_date
            
            return df_summary
            
        else:
            logger.error(f"[DEBUG] {current_date} 汇总数据条件判断失败或无数据")
            return False
    except Exception as e:
        logger.error(f"[ERROR] 淘宝客汇总数据获取异常 {current_date}: {e}")
        return False

def _fetch_taoke_items(session, token, current_date, shopname):
    """获取淘客商品明细并返回"""
    logger.info(f" 正在处理淘宝客商品数据: {current_date}")
    url2 = 'https://ad.alimama.com/openapi/param2/1/gateway.unionadv/analysis.data.itemAnalysisTopList.json?' \
        't={}&_tb_token_={}&startDate={}&endDate={}&pageNum=1&pageSize=40' \
        '&period=passed1&level3Dim=&orderMetric=alipayAmt&orderType=desc'.format(
            r_time, token, current_date, current_date)
    
    try:
        res2 = session.get(url2)
        if res2.json().get('data'):
            recordCount = res2.json().get('data').get('count')
            logger.info(f" 淘宝客商品数据总记录数({current_date}): {recordCount}")
            
            if recordCount > 0:
                df_date_product_data = []
                for page in range(1, math.ceil(recordCount / 40) + 1):
                    time.sleep(random.uniform(0, 1))
                    url2_page = 'https://ad.alimama.com/openapi/param2/1/gateway.unionadv/analysis.data.itemAnalysisTopList.json?' \
                        't={}&_tb_token_={}&startDate={}&endDate={}&pageNum={}&pageSize=40' \
                        '&period=passed1&level3Dim=&orderMetric=alipayAmt&orderType=desc'.format(
                            r_time, token, current_date, current_date, page)
                    
                    try:
                        res2_page = session.get(url2_page)
                        if res2_page.json().get('data') and res2_page.json().get('data').get('list'):
                            p_data = res2_page.json().get('data').get('list')
                            df = pd.json_normalize(p_data, max_level=0)
                            
                            df['店铺名称'] = shopname
                            df['推广日期'] = current_date
                            df['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # 商品数据字段映射 - 横向优化
                            product_field_mapping = {
                                'itemId': '商品ID', 'itemName': '商品名称', 'itemPrice': '商品价格', 'itemPicUrl': '商品图片URL', 'itemUrl': '商品链接',
                                'alipayAmt': '成交金额', 'alipayNum': '成交单量', 'alipayByrCnt': '成交用户数', 'alipayQuantity': '成交件数',
                                'preTotalFee': '花费', 'preCommissionFee': '预估佣金', 'preCommissionRate': '预估佣金费率', 'preCommissionRatePercent': '预估佣金费率百分比',
                                'preServiceFee': '预估服务费', 'preServiceRate': '预估服务费率', 'preServiceRatePercent': '预估服务费率百分比', 'preGPP': '预估笔单价',
                                'cpsSettleAmt': '结算金额', 'cpsSettleNum': '结算单量', 'cpsSettleByrCnt': '结算用户数',
                                'cmTotalFee': '结算总花费', 'cmCommissionFee': '结算佣金', 'cmCommissionRate': '结算佣金费率', 'cmCommissionRatePercent': '结算佣金费率百分比',
                                'cmServiceFee': '结算服务费', 'cmServiceRate': '结算服务费率', 'cmServiceRatePercent': '结算服务费率百分比',
                                'enterShopPvTk': '点击量', 'enterShopUvTk': '进店访客数', 'tkPvItemCnt': '曝光量',
                                'cartAddItmCnt': '加购数', 'cltAddItmCnt': '总收藏数', 'cvr': '转化率', 'cvrPercent': '转化率百分比',
                                'avgCouponAmount': '平均优惠券金额', 'couponDisRate': '优惠券折扣率', 'couponDisRatePercent': '优惠券折扣率百分比',
                                'alipayAmtDepositPresale': '定金预售成交金额', 'alipayNumDepositPresale': '定金预售成交单量', 'depPreAlipayAmt': '定金预售金额',
                                'depPreCommissionFee': '定金预售佣金', 'depPreCommissionRate': '定金预售佣金费率', 'depPreCommissionRatePercent': '定金预售佣金费率百分比', 'depPreRestAmt': '定金预售尾款金额',
                                'tkSuccAmt': '淘客成功金额', 'tkSuccCnt': '淘客成功笔数', 'tkSuccByrCnt': '淘客成功买家数', 'updateTime': '更新时间'
                            }
                            
                            df.rename(columns=product_field_mapping, inplace=True)
                            pct_cols2 = [col for col in df.columns if '百分比' in col]
                            for col in pct_cols2:
                                df[col] = df[col].apply(pct_to_float)
                                
                            product_columns_to_keep = [
                                '店铺名称', '推广日期', '导入日期',
                                '商品ID', '商品名称', '商品价格', '商品图片URL', '商品链接',
                                '成交金额', '成交单量', '成交用户数', '成交件数',
                                '花费', '预估佣金', '预估佣金费率', '预估佣金费率百分比', '预估服务费', '预估服务费率', '预估服务费率百分比', '预估笔单价',
                                '结算金额', '结算单量', '结算用户数', '结算总花费', '结算佣金', '结算佣金费率', '结算佣金费率百分比', '结算服务费', '结算服务费率', '结算服务费率百分比',
                                '点击量', '进店访客数', '曝光量', '加购数', '总收藏数', '转化率', '转化率百分比',
                                '平均优惠券金额', '优惠券折扣率', '优惠券折扣率百分比',
                                '定金预售成交金额', '定金预售成交单量', '定金预售金额', '定金预售佣金', '定金预售佣金费率', '定金预售佣金费率百分比', '定金预售尾款金额',
                                '淘客成功金额', '淘客成功笔数', '淘客成功买家数',
                            ]
                            
                            existing_product_columns = [col for col in product_columns_to_keep if col in df.columns]
                            df = df[existing_product_columns]
                            df_date_product_data.append(df)
                    except Exception as e:
                        logger.error(f"[ERROR] 淘宝客商品数据分页 {page} 获取失败: {e}")
                
                if df_date_product_data:
                    df_products = pd.concat(df_date_product_data)
                    
                    # 入库 - 淘客商品表
                    engine = create_db_engine()
                    df_products.to_sql('贝易_天猫淘客商品数据', con=engine, index=False, if_exists="append")
                    logger.info(f"[SUCCESS] 淘宝客商品数据保存成功: {current_date}, 共{len(df_products)}条记录")
                    
                    # 准备推广商品表数据
                    product_summary_columns = ['成交金额', '花费', '成交单量', '曝光量', '点击量', '总收藏数', '加购数', '商品ID']
                    existing_summary_cols = [col for col in product_summary_columns if col in df_products.columns]
                    df_products_formatted = df_products[existing_summary_cols].copy()
                    
                    df_products_formatted['店铺名称'] = shopname
                    df_products_formatted['渠道'] = '淘宝客'
                    df_products_formatted['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    df_products_formatted['推广日期'] = current_date
                    
                    return df_products_formatted
            return False
    except Exception as e:
        logger.error(f"[ERROR] 淘宝客商品数据主请求异常 {current_date}: {e}")
        return False

def taoke_main(account, password, shopname, start_date, end_date, target_dates=None):
    """
    淘宝客数据获取主入口 - 支持增量重跑
    :param target_dates: (可选) list, 指定要跑的日期列表。如果不传则自动生成 start_date 到 end_date。
    """
    try:
        # 1. 确定要跑的日期列表
        if target_dates:
            date_list = target_dates
            logger.info(f" 淘宝客进入增量重跑模式，将只抓取以下 {len(date_list)} 天: {date_list}")
        else:
            date_list = get_date_range(start_date, end_date)
            logger.info(f" 开始获取淘宝客数据全量，日期范围: {start_date} 至 {end_date}")

        # 2. 初始化 Cookie 和 Session
        try:
            # dl_mmcookies(account, password, shopname)
            with open(os.path.join(DIR_PATH,'Cookies','mmcookies.json'), 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            cookies = cookies_data.get(shopname)
        except Exception as e:
            logger.error(f"[ERROR] 读取 mmcookies 失败: {e}")
            return {'code': 500, 'result': False, 'msg': f'读取mmcookies失败: {str(e)}', 'method_name': 'taoke_main'}

        header = {
            'Authorization': "",
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Cookie': cookies,
            'Referer': 'https://ad.alimama.com/portal/v2/report/promotionDataPage.htm',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }
        session = requests.Session()
        session.headers.update(header)
        
        try:
            token = extract_tb_token(cookies)
        except Exception as e:
            logger.error(f"[ERROR] Token提取失败: {e}")
            return {'code': 500, 'result': False, 'msg': f'Token提取失败: {str(e)}', 'method_name': 'taoke_main'}
        
        # 3. 循环获取数据
        df_summary_data = []
        df_product_data = []
        failed_dates = []  # 用于记录本轮失败的日期

        for current_date in date_list:
            try:
                # A. 获取汇总数据
                summary_df = _fetch_taoke_summary(session, token, current_date, shopname)
                
                # B. 获取商品数据
                product_df = _fetch_taoke_items(session, token, current_date, shopname)
                
                # 只有当两者都成功获取时，才暂存
                if (summary_df is not False) and (product_df is not False):#DataFrame 的布尔值判断是模糊的
                # if summary_df and product_df:
                    df_summary_data.append(summary_df)
                    df_product_data.append(product_df)
                else:
                    # 只要有一个没拿到，就记录这一天失败了
                    failed_dates.append(current_date)
                    logger.warning(f"[WARNING] 日期 {current_date} 数据不完整，已加入重跑队列")
            except Exception as e:
                logger.error(f"[ERROR] 处理日期 {current_date} 时发生异常: {e}")
                failed_dates.append(current_date)
                
            # 避免请求频繁
            time.sleep(10)

        # 4. 批量入库
        #即使有失败的日期，成功的日期也要先入库，避免白跑
        if df_summary_data:
            try:
                df_final_summary = pd.concat(df_summary_data)
                engine = create_db_engine()
                df_final_summary.to_sql('贝易_天猫店铺推广汇总数据', con=engine, index=False, if_exists="append")
                logger.info(f"[SUCCESS] 淘宝客汇总数据入库成功，共{len(df_final_summary)}条记录")
            except Exception as e:
                logger.error(f"[ERROR] 淘宝客汇总数据入库失败: {e}")

        if df_product_data:
            try:
                df_final_products = pd.concat(df_product_data)
                engine = create_db_engine()
                df_final_products.to_sql('贝易_天猫店铺推广商品数据', con=engine, index=False, if_exists="append")
                logger.info(f"[SUCCESS] 淘宝客商品数据入库成功，共{len(df_final_products)}条记录")
            except Exception as e:
                logger.error(f"[ERROR] 淘宝客商品数据入库失败: {e}")
                
        # 5. 返回结果给调度器
        if not failed_dates:
            return {
                'code': 200,
                'result': True,
                'msg': f'淘宝客数据全部获取成功 ({date_list[0]} 至 {date_list[-1]})',
                'method_name': 'taoke_main',
                'missing_info': []
            }
        else:
            # 返回 False，并附带 missing_info，调度器会自动读取这个字段并在下一轮传给 target_dates
            return {
                'code': 200,
                'result': False,
                'msg': f'淘宝客数据存在缺失，缺失日期: {failed_dates}',
                'method_name': 'taoke_main',
                'missing_info': failed_dates  # 关键字段：告诉调度器下次只跑这些
            }

    except Exception as e:
        logger.error(f"[ERROR] 淘宝客数据获取主流程异常: {e}")
        return {'code': 500, 'result': False, 'msg': str(e), 'method_name': 'taoke_main','missing_info': []}


# 4. 品牌新享
def _fetch_ppxx_items(session, csrfID, current_date, shopname):
    """获取品牌新享商品数据并返回"""
    logger.info(f" 获取新客加速商品数据: {current_date}")
    url2 = 'https://ppxk.tmall.com/new/api/querySellerDataView.json?' \
           'r=mx_1553&param=%7B%22startDate%22%3A%22{}%22%2C%22endDate%22%3A%22{}%22%2C%22' \
           'page%22%3A{}%2C%22pageSize%22%3A%22{}%22%2C%22filterParams%22%3A%7B%22marketing_solution_id' \
           '%22%3A%221%22%7D%2C%22orderParam%22%3A%5B%7B%22id%22%3A%22item_id%22%2C%22asc%22%3Atrue%7D%5D%2C%22code%22%3A%22tm_ncrowd_item_sum%22%2C%22bizCode%22%3A%22ppxk%22%7D&' \
           'csrfID={}'.format(current_date, current_date, 1, 40, csrfID)
    
    try:
        res2 = session.get(url2,timeout=15)
        logger.info(f" 品牌新享返回诊断 | 状态码: {res2.status_code} | 内容长度: {len(res2.content)}")
        
        if 'application/json' not in res2.headers.get('Content-Type', ''):
            logger.error(f"品牌新享接口被拦截！收到非JSON内容。前100位: {res2.text.strip()[:100]}")
            return False
        resp_json = res2.json()
        if resp_json.get('data') and res2.json().get('data').get('model'):
            recordCount = res2.json().get('data').get('model').get('totalCount')
            logger.info(f" 新客加速商品数据记录数: {recordCount}")
            if recordCount == 0:
                return True
            
            if recordCount > 0:
                df_date_product_data = []
                for page in range(1, math.ceil(recordCount / 40) + 1):
                    time.sleep(random.uniform(0, 1))
                    url2_page = 'https://ppxk.tmall.com/new/api/querySellerDataView.json?' \
                               'r=mx_1553&param=%7B%22startDate%22%3A%22{}%22%2C%22endDate%22%3A%22{}%22%2C%22' \
                               'page%22%3A{}%2C%22pageSize%22%3A%22{}%22%2C%22filterParams%22%3A%7B%22marketing_solution_id' \
                               '%22%3A%221%22%7D%2C%22orderParam%22%3A%5B%7B%22id%22%3A%22item_id%22%2C%22asc%22%3Atrue%7D%5D%2C%22code%22%3A%22tm_ncrowd_item_sum%22%2C%22bizCode%22%3A%22ppxk%22%7D&' \
                               'csrfID={}'.format(current_date, current_date, page, 40, csrfID)
                    
                    try:
                        res2_page = session.get(url2_page)
                        if res2_page.json().get('data') and res2_page.json().get('data').get('model') and res2_page.json().get('data').get('model').get('rows'):
                            p_data = res2_page.json().get('data').get('model').get('rows')
                            df = pd.json_normalize(p_data)
                            df_date_product_data.append(df)
                    except Exception as e:
                         logger.error(f"[ERROR] 品牌新享商品数据分页 {page} 获取失败: {e}")
                
                if df_date_product_data:
                    df_products = pd.concat(df_date_product_data)
                    df_products['渠道'] = '品牌新享'
                    # 横向排版映射
                    df_products.rename({
                        'div_pay_amt': '成交金额', 'uv': '曝光量', 'byr_uv': '加购数', 'item_title': 'promotionName',
                        'collect_uv': '收藏商品数', 'item_id': '商品ID', 'pay_uv': '成交单量'
                    }, axis=1, inplace=True)
                    
                    df_products['item_ratio'] = df_products['item_ratio'].apply(lambda x: float(str(x).replace('%', '')) / 100 if x else 0)
                    df_products['花费'] = df_products['成交金额'] * df_products['item_ratio']
                    df_products['花费'] = df_products['花费'].astype(float)
                    
                    df_products['店铺名称'] = shopname
                    df_products['推广日期'] = current_date
                    df_products['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    df_products['营销场景'] = '新客加速'
                    
                    product_columns = ['成交金额', '花费', '成交单量', '曝光量', '收藏商品数', '加购数', '推广日期',  '商品ID', '店铺名称', '导入日期', '渠道']
                    existing_columns = [col for col in product_columns if col in df_products.columns]
                    df_products = df_products[existing_columns]
                    
                    # 入库
                    engine = create_db_engine()
                    df_products.to_sql('贝易_天猫店铺推广商品数据', con=engine, index=False, if_exists="append")
                    logger.info(f"[SUCCESS] 新客加速商品数据保存成功: {current_date}, 共{len(df_products)}条记录")
                    return df_products
    except Exception as e:
        logger.error(f"[ERROR] 品牌新享商品数据主流程异常 {current_date}: {e}")
    return False

def _fetch_ppxx_summary(session, csrfID, current_date, shopname):
    """获取品牌新享汇总数据并返回"""
    logger.info(f" 获取新客加速汇总数据: {current_date}")
    url3 = 'https://ppxk.tmall.com/new/api/querySellerDataView.json?' \
           'r=mx_2314&param=%7B%22startDate%22%3A%22{}%22%2C%22endDate%22%3A%22{}' \
           '%22%2C%22page%22%3A1%2C%22filterParams%22%3A%7B%22marketing_solution_id%22%3A%221%22%7D%2C%22orderParam' \
           '%22%3A%5B%7B%22id%22%3A%22ds%22%2C%22asc%22%3Atrue%7D%5D%2C%22code%22%3A%22tm_ncrowd_shop_detail%' \
           '22%2C%22bizCode%22%3A%22ppxk%22%7D&' \
           'csrfID={}'.format(current_date, current_date, csrfID)
    
    try:
        res3 = session.get(url3)
        if 'application/json' not in res3.headers.get('Content-Type', ''):
            logger.error(f"品牌新享汇总接口被拦截！")
            return False
        
        resp_json1 = res3.json()
        if resp_json1.get('data') and res3.json().get('data').get('model') and res3.json().get('data').get('model').get('rows'):
            p_data3 = res3.json().get('data').get('model').get('rows')
            df_summary = pd.json_normalize(p_data3)
            
            df_summary['渠道'] = '品牌新享'
            # 横向排版映射
            df_summary.rename({
                'div_pay_amt': '成交金额', 'uv': '曝光量', 'byr_uv': '加购数',
                'collect_uv': '收藏商品数', 'pay_uv': '成交单量', 'pre_cy_amt': '花费'
            }, axis=1, inplace=True)
            
            df_summary['花费'] = df_summary['花费'].astype(float)
            df_summary['店铺名称'] = shopname
            df_summary['推广日期'] = current_date
            df_summary['导入日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df_summary['营销场景'] = '新客加速'
            
            summary_columns = ['成交金额', '花费', '成交单量', '曝光量', '收藏商品数', '加购数', '推广日期', '店铺名称', '导入日期', '渠道']
            existing_columns = [col for col in summary_columns if col in df_summary.columns]
            df_summary = df_summary[existing_columns]
            
            return df_summary
    except Exception as e:
        logger.error(f"[ERROR] 品牌新享汇总数据异常 {current_date}: {e}")
    return False

def ppxx_main(account, password, shopname, start_date, end_date, target_dates=None):
    """
    品牌新享数据获取主入口 - 支持增量重跑
    :param target_dates: (可选) list, 指定要跑的日期列表
    """
    try:
        # 1. 确定日期列表
        if target_dates:
            date_list = target_dates
            logger.info(f" 品牌新享进入增量重跑模式，将只抓取以下 {len(date_list)} 天: {date_list}")
        else:
            date_list = get_date_range(start_date, end_date)
            logger.info(f" 开始获取品牌新享全量数据，日期范围: {start_date} 至 {end_date}")

        # 2. 初始化 Cookie 和 Session
        try:
            with open(os.path.join(DIR_PATH,'Cookies','cookies.json'), 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            cookies = cookies_data.get(shopname)
        except Exception as e:
            logger.error(f"读取 cookies 失败: {e}")
            return {'code': 500, 'result': False, 'msg': f'读取cookies失败: {str(e)}', 'method_name': 'ppxx_main'}
        
        header = {
            'Authorization': "",
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Cookie': cookies,
            'Referer': 'https://one.alimama.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }
        session = requests.Session()
        session.headers.update(header)
        
        # 3. 循环获取数据
        df_summary_data = []
        failed_dates = []

        for current_date in date_list:
            logger.info(f" 正在处理品牌新享数据: {current_date}")
            is_day_success = False # 单日成功标记

            try:
                # 获取 csrfID
                url = 'https://ppxk.tmall.com/new/api/querySellerDetail.json?r=mx_13&param=%7B%22bizCode%22%3A%22ppxk%22%7D&csrfID='
                res = session.get(url)
                
                if res.json().get('data') and res.json().get('data').get('csrfID'):
                    csrfID = res.json().get('data').get('csrfID')
                    
                    # A. 获取商品数据 (内部直接入库)
                    items_res = _fetch_ppxx_items(session, csrfID, current_date, shopname)
                    
                    # B. 获取汇总数据,暂时保存起来
                    summary_df = _fetch_ppxx_summary(session, csrfID, current_date, shopname)
                    
                    # 判定：两个都要有才算成功
                    if (items_res is not False) and (summary_df is not False):
                        df_summary_data.append(summary_df)
                        is_day_success = True
                        logger.info(f"[SUCCESS] 新客加速单日成功: {current_date}")
                    else:
                        logger.warning(f"[WARNING] 新客加速单日数据缺失: {current_date}")
                else:
                    logger.warning(f"[WARNING] 无法获取 CSRF ID: {current_date}")

            except Exception as e:
                logger.error(f"[ERROR] 品牌新享单日异常 {current_date}: {e}")
            
            # 如果没成功，加入失败列表
            if not is_day_success:
                failed_dates.append(current_date)
                
            # 随机休眠避免封控
            time.sleep(random.randint(600, 900) / 100)
        
        # 4. 汇总数据入库
        if df_summary_data:
            try:
                df_final_summary = pd.concat(df_summary_data)
                engine = create_db_engine()
                df_final_summary.to_sql('贝易_天猫店铺推广汇总数据', con=engine, index=False, if_exists="append")
                logger.info(f"[SUCCESS] 品牌新享汇总数据入库成功，共{len(df_final_summary)}条记录")
            except Exception as e:
                logger.error(f"[ERROR] 品牌新享汇总数据入库失败: {e}")
        
        # 5. 返回结果给调度器
        if not failed_dates:
            return {
                'code': 200, 
                'result': True, 
                'msg': f'品牌新享数据获取成功 ({date_list[0]} 至 {date_list[-1]})',
                'method_name':'ppxx_main',
                'missing_info': []
                
            }
        else:
            return {
                'code': 200, 
                'result': False, 
                'msg': f'品牌新享部分缺失，缺失日期: {failed_dates}',
                'method_name': 'ppxx_main',
                'missing_info': failed_dates # 关键：告诉调度器下次只跑这些
            }
            
    except Exception as e:
        logger.error(f"[ERROR] 品牌新享主流程失败: {e}")
        return {'code': 500, 'result': False, 'msg': str(e), 'method_name': 'ppxx_main','missing_info': []}


#删除函数
def tianmao_single_day():
    """
    单天数据处理函数 - 用于基础数据和商品数据
    只处理前一天的数据
    """
    
    logger.info(f" 开始单天数据处理，目标日期: {single_date}")
    
    # 第一步：删除单天旧数据
    logger.info("\n 开始删除单天旧数据...")
    engine = create_db_engine()
    
    tables_to_clean = ['贝易_天猫基础数据', '贝易_天猫旗舰店_商品数据源','贝易_天猫全店搜索词','贝易_天猫单品搜索词']
    for table in tables_to_clean:
        delete_single_day(engine, table, target_date=single_date)
    
    logger.info("[SUCCESS] 单天旧数据删除完成")
    
def tianmao_15days():
    """
    15天回溯处理函数 - 用于推广数据
    """
    logger.info(f" 开始15天推广数据处理，日期范围: {riqi[0]} 至 {riqi[1]}")

    # 第一步：统一删除15天推广数据
    logger.info("\n 开始统一删除15天推广旧数据...")
    engine = create_db_engine()
    
    tables_to_clean = [
        '贝易_天猫店铺推广汇总数据', '贝易_天猫店铺推广商品数据', '贝易_天猫超级直播明细数据',
        '贝易_天猫店铺直达明细数据', 
        '贝易_天猫店铺淘客汇总数据', '贝易_天猫淘客商品数据', 
    ]
    for table in tables_to_clean:
        delete_15days(engine, table, end_date=riqi[1])
    
    logger.info("[SUCCESS] 15天推广旧数据删除完成")
    

#执行主函数
if __name__ == "__main__":
    #执行cookies预检
    try:
        ensure_cookies('贝易BEIESTATE旗舰店:许凯', 'By888888', '贝易BEIESTATE旗舰店',riqi[1])
        ensure_cookies('beiestate贝易玩具旗舰店:许凯', 'beiestate2026', 'beiestate贝易玩具旗舰店',riqi[1])
    except Exception as e:
        logger.error(f"cookies预检失败: {e}")
    try:
        #执行单天数据处理
        tianmao_single_day()
        
        
        #执行单天相关数据获取
        logger.info("\n 开始获取单天新数据...")
        try:
            sycm_main('贝易BEIESTATE旗舰店:许凯', 'By888888', '149584139', '贝易BEIESTATE旗舰店', single_date)
            
            sycm_main('beiestate贝易玩具旗舰店:许凯', 'beiestate2026', '583466619', 'beiestate贝易玩具旗舰店', single_date)
            
            
            logger.info(f"[SUCCESS] 单天数据获取完成: {single_date}")
        except Exception as e:
            logger.error(f"[ERROR] 单天数据获取失败: {single_date}, 错误: {e}")
        logger.info(f"\n[SUCCESS] 单天数据处理完成！日期: {single_date}")
        
        
        #执行15天回溯（推广相关数据）
        tianmao_15days()
        
        #执行15天相关数据获取
        logger.info("\n 开始获取15天推广新数据...")
        try:
            # 万象台
            #旗舰店
            get_promotion_data('贝易BEIESTATE旗舰店:许凯', 'By888888', '贝易BEIESTATE旗舰店', riqi[0], riqi[1])
            
            #玩具店
            get_promotion_data('beiestate贝易玩具旗舰店:许凯', 'beiestate2026', 'beiestate贝易玩具旗舰店', riqi[0], riqi[1])
            
            logger.info(f"[SUCCESS] 15天推广数据获取完成: {riqi[0]} 至 {riqi[1]}")
        except Exception as e:
            logger.error(f"[ERROR] 15天推广数据获取失败: {e}")
        
        logger.info("\n 开始获取15天淘宝客数据...")
        try:
            taoke_main('贝易BEIESTATE旗舰店:许凯', 'By888888', '贝易BEIESTATE旗舰店', riqi[0], riqi[1])
            
            #玩具店
        
            # taoke_main('beiestate贝易玩具旗舰店:许凯', 'beiestate2026', 'beiestate贝易玩具旗舰店', riqi[0], riqi[1])
            logger.info(f"[SUCCESS] 15天淘宝客数据获取完成: {riqi[0]} 至 {riqi[1]}")
        except Exception as e:
            logger.error(f"[ERROR] 15天淘宝客数据获取失败: {e}")
            
        logger.info("\n 开始获取15天品牌新享数据...")
        
        try:
            ppxx_main('贝易BEIESTATE旗舰店:许凯', 'By888888',  '贝易BEIESTATE旗舰店', riqi[0], riqi[1])
            
            #玩具店
            # ppxx_main('beiestate贝易玩具旗舰店:许凯', 'beiestate2026', 'beiestate贝易玩具旗舰店', riqi[0], riqi[1])
            logger.info(f"[SUCCESS] 15天品牌新享数据获取完成: {riqi[0]} 至 {riqi[1]}")
        except Exception as e:
            logger.error(f"[ERROR] 15天品牌新享数据获取失败: {e}")

        logger.info(f"\n[SUCCESS] 15天推广数据处理完成！日期范围: {riqi[0]} 至 {riqi[1]}")
        
        
        logger.info('脚本执行完成')
        
    except Exception as e:
        logger.error(f"主程序运行发生错误: {e}")