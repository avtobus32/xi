import json
import logging
import sys
import time


logging.basicConfig(level=logging.INFO,
                        format="[%(asctime)s %(levelname)s] - %(message)s",
                        datefmt='%H:%M:%S',
                        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(f'logs/log {int(time.time())}.txt', encoding='utf-8', mode='w')])
logger = logging.getLogger()

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36",
}

MIN_REG_YEAR = 2015
CARS_PER_SERIES = 10
PARSING_DELAY_DAYS = 5

TRANSLATE_SET_LENGTH = 20
TRANSLATE_CATS = {
    'color': "ru",
    'wd': "ru",
    'fuel': "ru",
    'trans': "ru",
    'bdwk': "ru",
    'model': "eng"
}

DATA_FILES = {
    "che168": "data/che168.xlsx",
    "translated_che168": "data/translated_che168.xlsx",
    "dcd": "data/dcd.xlsx",
    "translated_dcd": "data/translated_dcd.xlsx",
    "translate_data": "data/translate_data.xlsx"
}

with open('data/sql_data.json', 'r') as f:
    sql_data = json.load(f)
    SQL_ENGINE_URL = f"mysql://{sql_data['login']}:{sql_data['password']}@{sql_data['ip']}:{sql_data['port']}/{sql_data['db_name']}"
    del sql_data

with open('brands_ch.json', 'r', encoding='utf8') as f:
    brands_en_ch = json.load(f)

brands_ch_en = {}
for key, value in brands_en_ch.items():
    brands_ch_en[value] = key