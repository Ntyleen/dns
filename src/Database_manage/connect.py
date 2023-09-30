import os
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def create_connection_to_database(return_url=False):    #   Создание подключения к бд и получение данных из окружения
    db_username = os.environ.get('DB_USERNAME')
    db_password = os.environ.get('DB_PASSWORD')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')

    db_url = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

    try:
        if return_url:
            logger.info("Ссылка успешно сгенерировна")
            return db_url
        else:
            logger.info("Соединение с базой создано успешно")
            return create_engine(db_url)

    except SQLAlchemyError as e:
        logger.error(f"Неудалось создать соединение с базой. Ошибка: {e}")
        raise
