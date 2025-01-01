import requests
import pandas
import os
from time import sleep
from requests import ReadTimeout
from sqlalchemy import create_engine

from constants import logger, HEADERS, DATA_FILES, SQL_ENGINE_URL


def get(url, delay, to200=False, **kwargs) -> requests.Response:
    if delay > 0:
        sleep(delay)
    success = False
    while not success:
        try:
            response = requests.get(url, timeout=4, headers=HEADERS, **kwargs)
            if to200 and response.status_code != 200:
                raise ValueError
            success = True
        except ReadTimeout:
            logger.warning('request timeout, retrying')
        except requests.exceptions.ConnectionError:
            logger.warning('connection error, retrying')
        except ValueError:
            logger.warning(f'statuscode !=200 / {response.status_code}, retrying')
        except requests.exceptions.ChunkedEncodingError:
            logger.warning('invalid response received, retrying')

    return response

def create_cars_frame(cars_data) -> pandas.DataFrame:
    columns = {
        "id": pandas.Series([car["id"] for car in cars_data], dtype="int"),
        "name": [car["name"] for car in cars_data],
        "mark": [car["mark"] for car in cars_data],
        "model": [car["model"] for car in cars_data],
        "price": pandas.Series([car["price"] for car in cars_data], dtype="int"),
        "date": [car["date"] for car in cars_data],
        "mileage": pandas.Series([car["mileage"] for car in cars_data], dtype="int"),
        "color": [car["color"] for car in cars_data],
        "wd": [car["wd"] for car in cars_data],
        "volume": pandas.Series([car["volume"] for car in cars_data], dtype="float"),
        "engine_power": pandas.Series([car["engine_power"] for car in cars_data], dtype="Int64"),
        "electric_power": pandas.Series([car["electric_power"] for car in cars_data], dtype="Int64"),
        "comprehensive_power": pandas.Series([car["comprehensive_power"] for car in cars_data], dtype="Int64"),
        "fuel": [car["fuel"] for car in cars_data],
        "fuelcons": pandas.Series([car["fuelcons"] for car in cars_data], dtype="float"),
        "trans": [car["trans"] for car in cars_data],
        "bdwk": [car["bdwk"] for car in cars_data],
        "about": [car["about"] for car in cars_data],
        "url": [car["url"] for car in cars_data]
    }
    return pandas.DataFrame(columns)

def create_images_frame(image_data) -> pandas.DataFrame:
    columns = {
        "car_id": pandas.Series([img['car_id'] for img in image_data], dtype="int"),
        "url": [img['img'] for img in image_data]
    }
    return pandas.DataFrame(columns)

def dump_parsed_data(data, file_name) -> None:
    cars_frame = create_cars_frame(data['cars'])
    images_frame = create_images_frame(data['images'])

    dump_data(cars_frame, file_name, "cars", index=False)
    dump_data(images_frame, file_name, "images", index=False)

def dump_data(data, file_name, sheet_name, index=True, mode="a", replace_sheet=False):
    file_exists = os.path.exists(file_name)
    if file_exists:
        if replace_sheet:
            if_sheet_exists = 'replace'
        else:
            if_sheet_exists = 'overlay' if mode == "a" else None
        writer = pandas.ExcelWriter(file_name, mode=mode, if_sheet_exists=if_sheet_exists, engine="openpyxl")
    else:
        writer = pandas.ExcelWriter(file_name, mode='x', engine="openpyxl")

    header = not file_exists or replace_sheet or sheet_name not in writer.sheets
    if sheet_name in writer.sheets and not replace_sheet:
        start_row = writer.sheets[sheet_name].max_row
    else:
        start_row = 0

    data.to_excel(writer, sheet_name=sheet_name, header=header, index=index, startrow=start_row)
    writer.close()

    logger.info(f"Dump -> {sheet_name} {len(data)} rows")

def to_sql(site_name):
    file_name = DATA_FILES[site_name].replace(f"{site_name}.xlsx", f"translated_{site_name}.xlsx")
    cars_df = pandas.read_excel(file_name, sheet_name='cars')
    images_df = pandas.read_excel(file_name, sheet_name='images')
    engine = create_engine(SQL_ENGINE_URL)
    cars_df.to_sql(f"{site_name}_cars", engine, if_exists="replace")
    images_df.to_sql(f"{site_name}_images", engine, if_exists="replace")

if __name__ == "__main__":
    pass