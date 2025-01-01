import asyncio
import json
import os
import time
import datetime

import numpy
import pandas

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from translator import myTranslator
from utils import get, dump_parsed_data, to_sql
from constants import logger, brands_en_ch, brands_ch_en, DATA_FILES, CARS_PER_SERIES, MIN_REG_YEAR, TRANSLATE_CATS, \
    PARSING_DELAY_DAYS

DELAY = 0

parsed_params = {}

def get_brands_data():
    """
    {
        brand_id	:	2
        brand_name	:	"奥迪"
        image_url	:	"https://p1-dcd.byteimg.com/img/motor-mis-img/62946ba030f3589e083d8d3e98a595eb~tplv-resize:100:100.image"
        open_url	:	"sslocal://webview?bounce_disable=1&enable_l0=dkx%2Cdky&hide_back=1&hide_back_buttonView=1&hide_bar=1&hide_progress_bar=1&hide_status_bar=1&url=https%3A%2F%2Fapi.dcarapi.com%2Fmotor%2Ffeoffline%2Fusedcar_channel%2Fcar-source.html%3Fbrand_id%3D2%26brand_name%3D%25E5%25A5%25A5%25E8%25BF%25AA%26hide_status_bar%3D1%26link_source%3Ddcd_esc_page_sh_car_home_car_brand_cell%26used_car_entry%3Dpage_brand_list_used_car&use_offline=1"
        pinyin	:	"A"
        on_sale	:	True
        desc	:	"598辆在售"
        link_source	:	""
    }
    :return:
    """
    brands_data = {}
    req = get("https://m.dcdapp.com/motor/sh_go/api/buy_car/brand_filter_list", DELAY)
    if req.status_code == 200 and req.json()['message'] == 'success':
        data = req.json()['data']
        for brand in data['brand_list']:
            if int(brand['type']) == 1076 and brand['info']['brand_name'] in brands_ch_en:
                brand['info']['name_en'] = brands_ch_en[brand['info']['brand_name']]
                brands_data[brand['info']['brand_name']] = brand['info']

    else:
        logger.critical(str(req.__dict__))
        logger.critical('failed get brands data')
        raise ConnectionError
    return brands_data


def get_series_data(brand_id):
    """
    {
       "category_key": "all",
       "category_name": "全部",
       "list": [
           {
              "type": 10051,
              "info": {
                 "key": "",
                 "text": "轿车"
              }
           },
           {
              "type": 10052,
              "info": {
                  "key": "all_0_99",
                  "series_id": 99,
                  "series_name": "奥迪A6L",
                  "cover_url": "https://p3-dcd.byteimg.com/motor-mis-img/f8250b5672059b6380b04eeeb0d7fe33~tplv-f042mdwyw7-original:256:0.image?psm=motor.sh_search.api",
                  "price": "1.03-44.50万",
                  "sub_title": "全国3557辆",
                  "tags": null
           }
       ]
    }
    :param brand_id:
    :return:
    """
    series_data = {}
    req = get("https://m.dcdapp.com/motor/sh_search/api/selection/series_list", DELAY, params={"brand_id": brand_id})
    if req.status_code == 200 and req.json()['message'] == 'success':
        data = req.json()['data']["category_list"][0]['info']
        if data["category_key"] != "all":
            logger.critical(f"wrong category picked\n{data['info']}")
            raise ValueError
        for series in data['list']:
            if int(series['type']) == 10052 and series['info']['price'] != "暂无报价": # 暂无报价 - no quotation for series on market
                series_data[series['info']['series_id']] = series['info']
    else:
        logger.critical(str(req.__dict__))
        logger.critical('failed get series data')
        raise ConnectionError
    return series_data

def get_cars_list(brand_id, series_id):
    """
    {
        "sku_id": 17099403,
        "shop_id": "118608",
        "series_id": 95,
        "series_name": "奥迪A3",
        "brand_id": 2,
        "brand_name": "奥迪",
        "car_id": 25344,
        "car_name": "30周年年型 Sportback 35 TFSI 时尚型",
        "year": 2018,
        "status": 1,
        "car_source_city_name": "武汉",
        "sku_version": "7445546808907943998",
        "spu_version": "7445546808903749694",
        "spu_id": 16592360,
        "platform_type": 0,
        "group_id": 7440358872230953526,
        "activity_plan_id": 0,
        "activity_id": "",
        "group_id_str": "7440358872230953526",
        "req_id": "202412071526216A3B26BAF97BC0F74804",
        "luxury_car": 0,
        "full_name": "武汉空间变换科技有限公司",
        "first_registration_time": 1516118400,
        "mileage": 4.81,
        "short_name": "懂车帝汽车商城·武汉店",
        "official_price": 21.38,
        "generalize_id": 0,
        "generalize_type": "",
        "has_third_party_test_report": 1,
        "fuel_form": 1,
        "source_type": 4,
        "source_type_name": "寄售车",
        "trade_type": 0,
        "trade_car_source_status": 0,
        "is_national_buy": 0,
        "publish_time": 1732348439,
        "search_id": "202412071526216A3B26BAF97BC0F74804"
    }
    :param brand_id:
    :param series_id:
    :return:
    """
    max_page_size = 50
    limit = min(max_page_size, CARS_PER_SERIES)
    cars = []
    params = {
        "brand_id": brand_id,
        "series_id": series_id,
        "cur_tab": 1001,
        "limit": limit,
        "sh_city_name": "全国"
    }
    for i in range(0, (CARS_PER_SERIES - 1) // max_page_size + 1):
        params['offset'] = i * max_page_size
        req = get("https://m.dcdapp.com/motor/searchapi/searchv2/", DELAY, to200=True, params=params)
        if req.status_code == 200 and req.json()['message'] == 'success':
            data = req.json()
            for line in data['data']:
                if int(line['type']) == 10001 and line['info']['base_info']['brand_id'] == brand_id and line['info']['base_info']['series_id'] == series_id:
                    cars.append(line['info'])
            if data["return_count"] < limit:
                break
        else:
            logger.critical(str(req.__dict__))
            logger.critical('failed get cars data')
            raise ConnectionError
    return cars


def get_car_card(sku_id):
    req = get("https://m.dcdapp.com/motor/sh_information/api/h5/sku_detail/base/", DELAY, params={"sku_id": sku_id})
    if req.status_code == 200 and req.json()['message'] == 'success':
        return req.json()['data']
    else:
        logger.critical(str(req.__dict__))
        logger.critical('failed get car data')
        raise ConnectionError


def get_car_details(sku_id):
    result = {}
    req = get("https://m.dcdapp.com/motor/sh_info/api/info/detail/", DELAY, params={"sku_id": sku_id})
    if req.status_code == 200 and req.json()['message'] == 'success':
        if req.json()['data']["detail_list"]:
            for detail in req.json()['data']["detail_list"]:
                if detail['type'] == 10036:
                    key = detail['info']['key']
                    if key in result and result[key] != detail['info']['value']:
                        logger.critical(f"details duplication {key} / id: {sku_id}")
                        raise ValueError
                    result[key] = detail['info']['value']
                elif detail['type'] == 10037:
                    for d in detail['info']['list']:
                        key = d['key']
                        if key in result and result[key] != d['value']:
                            logger.critical(f"details duplication {key} / id: {sku_id}")
                            raise ValueError
                        result[key] = d['value']
        else:
            logger.warning(f"received empty details list for skuid: {sku_id}")
        return result
    else:
        logger.critical(str(req.__dict__))
        logger.critical('failed get car data')
        raise ConnectionError


def get_car_params(car_id):
    if car_id in parsed_params:
        return parsed_params[car_id]
    else:
        req = get(f"https://m.dongchedi.com/auto/params-carIds-{car_id}", DELAY)
        if req.status_code == 200:
            soup = BeautifulSoup(req.text, "html.parser")
            props = json.loads(soup.find('script', id="__NEXT_DATA__").text)['props']['pageProps']
            config = props['rawData']['car_info'][0]
            parsed_params[car_id] = config
            return config
        else:
            logger.critical(str(req.__dict__))
            logger.critical('failed get car data')
            raise ConnectionError


async def get_car_data(sku_id, car_id):
    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_running_loop()

        tasks = [
            loop.run_in_executor(executor, get_car_card, sku_id),
            loop.run_in_executor(executor, get_car_details, sku_id),
            loop.run_in_executor(executor, get_car_params, car_id),
        ]

        results = await asyncio.gather(*tasks)
        data = {
            "card": results[0],
            "details": results[1],
            "params": results[2]
        }
    return data


def parse():
    parsed_brands = pandas.read_excel(DATA_FILES['dcd'], sheet_name='cars')['mark'].unique() if os.path.exists(
        DATA_FILES['dcd']) else []
    brands_data = get_brands_data()
    for brand, brand_ch in brands_en_ch.items():
        if brand_ch not in brands_data:
            logger.warning(f"{brand} / {brand_ch} not found")
    for i, (brand, brand_info) in enumerate(brands_data.items()):
        if brand_info['name_en'] in parsed_brands:
            logger.info(f"{brand_info['name_en']} already parsed")
            continue
        series_data = get_series_data(brand_info['brand_id'])
        data = {
            "cars": [],
            "images": []
        }
        for ii, (series_id, series_info) in enumerate(series_data.items()):
            cars_list = get_cars_list(brand_info['brand_id'], series_id)
            for iii, car in enumerate(cars_list):
                logger.info(
                    f"Brand {brand_info['name_en']} {i + 1}/{len(brands_data)} Series {series_info["series_name"]} {ii + 1}/{len(series_data)} Car {iii + 1}/{len(cars_list)}")
                reg_year = int(car['base_info']['year'])
                if reg_year < MIN_REG_YEAR:
                    logger.debug(f'Skip <{MIN_REG_YEAR}y data id: {car['base_info']['sku_id']}')
                    continue

                car_data = asyncio.run(get_car_data(car['base_info']['sku_id'], car['base_info']['car_id']))
                if not all(car_data.values()): continue

                if "head_images" not in car_data['card']["components"][0]['vo']['display']:
                    logger.warning(f"no images for {car['base_info']['sku_id']}, skipping")
                    continue

                volume_value = car_data['params']["info"].get("capacity_l", {'value': numpy.nan})['value']
                engine_power_value = car_data['params']["info"].get("engine_max_horsepower", {'value': '-'})['value']
                engine_power = int(engine_power_value) if engine_power_value not in ['-', ''] else numpy.nan
                electric_power_value = car_data['params']["info"].get("front_electric_max_horsepower", {'value': '-'})[
                    'value']
                try:
                    electric_power = int(electric_power_value)
                except ValueError:
                    electric_power = int(
                        electric_power_value.split('(')[1].strip('Ps)')) if electric_power_value not in ['-',
                                                                                                         ''] else numpy.nan
                comprehensive_power = engine_power if not pandas.isnull(
                    engine_power) else 0 + electric_power if not pandas.isnull(electric_power) else 0

                car_params = {
                    "id": car['base_info']['sku_id'],
                    "name": f"{series_info["series_name"]} {car['base_info']['car_name']}",
                    "mark": brand_info['name_en'],
                    "model": series_info["series_name"],
                    "price": int(float(car["card_info"]['price']) * 10000),
                    "date": reg_year,
                    "mileage": int(float(car['base_info']["mileage"]) * 10000),
                    "color": car_data['details']['car_body_color'],
                    "wd": car_data['details']['driven_form'],
                    "volume": float(volume_value) if volume_value not in ['-', ''] else numpy.nan,
                    "engine_power": engine_power,
                    "electric_power": electric_power,
                    "comprehensive_power": comprehensive_power if comprehensive_power else numpy.nan,
                    "fuel": car_data['params']["info"]["fuel_form"]['value'],
                    "fuelcons": numpy.nan,
                    "trans": car_data['details']["gear_box"] if "gear_box" in car_data['details'] else
                    car_data['params']["info"]['gearbox_type']['value'],
                    "bdwk": car_data['params']["info"]["body_struct"]['value'],
                    "about": numpy.nan,
                    "url": f"https://www.dongchedi.com/usedcar/{car['base_info']['sku_id']}"
                }
                car_images = [{
                    'car_id': car['base_info']['sku_id'],
                    'img': pic['pic_url']
                } for pic in car_data['card']["components"][0]['vo']['display']['head_images']]
                data['cars'].append(car_params)
                data['images'].extend(car_images)
            parsed_params.clear()
        dump_parsed_data(data, DATA_FILES["dcd"])


def main():
    logger.info(f"Parsing")
    parse()
    logger.info(f"Translating")
    translator = myTranslator()
    translator.translate_excel("dcd", 'cars', TRANSLATE_CATS)
    translator.translate_excel("dcd", 'images', {})
    logger.info(f"to sql")
    to_sql("dcd")
    os.rename(DATA_FILES['dcd'], f"{DATA_FILES['dcd'].replace(".xlsx", '')}_{int(time.time())}.xlsx")
    os.remove(DATA_FILES['translated_dcd'])
    logger.info('DONE')


if __name__ == "__main__":
    while True:
        start_datetime = datetime.datetime.now()
        main()
        while start_datetime + datetime.timedelta(days=PARSING_DELAY_DAYS) > datetime.datetime.now():
            time_to_wait = start_datetime + datetime.timedelta(days=PARSING_DELAY_DAYS) - datetime.datetime.now()
            logger.info(f"Sleepin for {time_to_wait.days} days & {time_to_wait.seconds // 60 // 60 % 24} hours")
            time.sleep(min(60 * 60, time_to_wait.seconds))




