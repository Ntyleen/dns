import pandas as pd

from src.utils.time_script import time_stamp

from loguru import logger
from src.Database_manage.connect import create_connection_to_database


@time_stamp
def load_data_to_db(data_path, model, engine=create_connection_to_database()):      # Обычный загрузчик данных в бд
    try:
        data = pd.read_csv(data_path)
        data.to_sql(model.__tablename__, engine, if_exists='append', index=False)
        logger.info(f"Данные загружены успешно из {data_path} в {model.__tablename__} таблицу")
    except Exception as e:
        logger.error(f"Ошибка загрузки данных из {data_path} в {model.__tablename__} таблицу: {e}")
