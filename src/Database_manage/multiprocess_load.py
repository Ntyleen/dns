import pandas as pd

from connect import create_connection_to_database
from loguru import logger
from multiprocessing import Pool
from sqlalchemy import create_engine
from src.utils.time_script import time_stamp


#   Модуль многопроцессорной загрузки данных в базу данных
def load_chunk(args):  # Фуникция загрузки части данных
    chunk, model, db_url = args
    engine = create_engine(db_url)
    try:
        logger.info(f"Загрузка части размером {chunk.shape[0]} строк в {model.__tablename__}.")
        with engine.connect() as connection:
            chunk.to_sql(model.__tablename__, engine, if_exists='append', index=False)
        return chunk.shape[0]
    except Exception as e:
        logger.error(f"Ошибка при загрузке блока данных в {model.__tablename__} таблицу: {e}")


@time_stamp
def load_file_multiprocces(data_path, model):  # Функция многопроцессорной загрузки файла
    logger.info(f"Начинается многопроцессорная загрузка из {data_path}...")
    db_url = create_connection_to_database(return_url=True)
    total_rows_loaded = 0
    chunk_size = 100000
    chunks = []

    for chunk in pd.read_csv(data_path, chunksize=chunk_size, index_col=0):
        chunks.append((chunk, model, db_url))

    with Pool(processes=4) as pool:
        results = pool.map(load_chunk, chunks)

    total_rows_loaded = sum(results)
    logger.info(f"Загружено {total_rows_loaded} строк из {data_path} в {model.__tablename__} таблицу")
