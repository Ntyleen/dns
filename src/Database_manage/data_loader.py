import pandas as pd

from src.utils.time_script import time_stamp

from loguru import logger
from connect import create_connection_to_database


@time_stamp
def save_to_csv(query_result, file_name="table.csv"):
    df = pd.read_sql(query_result.statement, query_result.session.bind)
    df.to_csv(file_name, index=False)


def load_data_to_db(data_path, model, engine=create_connection_to_database()):
    try:
        data = pd.read_csv(data_path)
        data.to_sql(model.__tablename__, engine, if_exists='append', index=False)
        logger.info(f"Данные загружены успешно из {data_path} в {model.__tablename__} таблицу")
    except Exception as e:
        logger.error(f"Ошибка загрузки данных из {data_path} в {model.__tablename__} таблицу: {e}")



