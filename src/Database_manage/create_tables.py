from src.utils.time_script import time_stamp

from loguru import logger
from src.Database_manage.connect import create_connection_to_database


@time_stamp
def create_tables(tables, engine=None):  # Модуль создания таблиц
    if engine is None:
        engine = create_connection_to_database()

    for table in tables:
        logger.info(f"Начато создание таблицы {table.__tablename__}...")
        try:
            table.__table__.create(bind=engine, checkfirst=True)
            logger.info(f"Таблица {table.__tablename__} создана.")
        except Exception as e:
            logger.error(f"Ошибка создания таблицы {table.__tablename__}: {e}")
