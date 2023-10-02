from loguru import logger

from src.utils.time_script import time_stamp

from src.Database_manage.multiprocess_load import load_file_multiprocces


# Загрузка всех данных из указанных файлов в соответствующие таблицы базы данных
#
# Функция принимает словарь с расположение таблиц с данными и соответствующими моделями таблиц, и осуществляет многопроцессорную загрузку в базу данных
#
# Параметр - data_mappig : словарь с ключем - путь к файлам
#                                    значение - модель таблицы

@time_stamp
def load_all_data(data_mapping):
    for data_path, model in data_mapping.items():
        try:
            logger.info(f"Начата загрузка данных из {data_path} в таблицу {model.__tablename__}...")
            load_file_multiprocces(data_path, model)
            logger.info(f"Данные успешно загружены из {data_path} в таблицу {model.__tablename__}...")

        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из {data_path} в таблицу {model.__tablename__}: {e}")
