import os
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def create_connection_to_database(return_url=False):
    db_username = os.environ.get('DB_USERNAME')
    db_password = os.environ.get('DB_PASSWORD')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')



    db_url = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

    if return_url:
        return db_url
    else:
        return create_engine(db_url)

