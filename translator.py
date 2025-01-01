import json
from time import sleep

import numpy
import pandas
from googletrans import Translator

from constants import DATA_FILES, logger, TRANSLATE_SET_LENGTH
from utils import dump_data

DELAY = 0.2


class myTranslator:
    def __init__(self):
        self.translate_data = {}
        self.translator = Translator(service_urls=["translate.google.ru"])
        self.load_translate_data()

    def load_translate_data(self):
        writer = pandas.ExcelWriter(DATA_FILES["translate_data"], mode='r+', engine="openpyxl")
        for sheet in writer.sheets:
            self.translate_data[sheet] = pandas.read_excel(writer, sheet_name=sheet, index_col=0).to_dict(orient="index")
        writer.close()

    def save_translate_data(self):
        writer = pandas.ExcelWriter(DATA_FILES["translate_data"], mode='a', if_sheet_exists='overlay', engine="openpyxl")
        for cat, data in self.translate_data.items():
            frame = pandas.DataFrame.from_dict(data, orient="index")
            frame.to_excel(writer, sheet_name=cat, header=True)
        writer.close()

    def translate_list(self, values: list, column, lang):
        for value in values.copy():
            if value in self.translate_data[column] and (not pandas.isnull(self.translate_data[column][value][lang]) or not pandas.isnull(self.translate_data[column][value][f"{lang}_auto"])) or pandas.isnull(value):
                values.remove(value)
        if len(values) > 0:
            delimiter = "\n"
            t_lang = "en" if lang == "eng" else "ru"
            for i in range(0, len(values) // TRANSLATE_SET_LENGTH + 1):
                logger.info(f"Translating {column} {TRANSLATE_SET_LENGTH * i}/{len(values)}")
                values_set = values[i * TRANSLATE_SET_LENGTH:min((i+1) * TRANSLATE_SET_LENGTH, len(values))]
                translate = self.translator.translate(delimiter.join(values_set), src='zh-cn', dest=t_lang)
                translated_values = translate.text.split(delimiter)
                if len(values_set) != len(translated_values):
                    logger.critical(f"origin length != translated\n{values_set}\n{translated_values}\n{translate.__dict__()}")
                    raise ValueError
                for v, t in zip(values_set, translated_values):
                    if v not in self.translate_data[column]:
                        self.translate_data[column][v] = {'ru_auto': numpy.nan, 'eng_auto': numpy.nan, 'ru': numpy.nan, 'eng': numpy.nan}
                    self.translate_data[column][v][f"{lang}_auto"] = t.strip()
                sleep(DELAY)
            self.save_translate_data()
        else:
            logger.debug(f"{column} empty translate list")

    def get_translation(self, column, value, lang):
        if value not in self.translate_data[column] or (pandas.isnull(self.translate_data[column][value][lang]) and pandas.isnull(self.translate_data[column][value][f"{lang}_auto"])):
            logger.critical(f"no translation {column} / {value} / {lang}")
            raise ValueError
        if not pandas.isnull(self.translate_data[column][value][lang]):
            return self.translate_data[column][value][lang]
        else:
            return self.translate_data[column][value][f"{lang}_auto"]

    def translate_excel(self, site_name, sheet, translate_cats):
        frame = pandas.read_excel(DATA_FILES[site_name], sheet_name=sheet)
        for column, lang in translate_cats.items():
            column_data = frame[column].unique()
            self.translate_list(column_data.tolist(), column, lang)
            for value in frame[column].unique():
                if not pandas.isnull(value):
                    frame[column] = frame[column].replace(value, self.get_translation(column, value, lang))
        dump_data(frame, DATA_FILES["translated_" + site_name], sheet, index=False, mode="a", replace_sheet=True)

if __name__ == "__main__":
    t = myTranslator()

