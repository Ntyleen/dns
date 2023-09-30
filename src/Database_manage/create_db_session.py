from create_tables import create_connection_to_database
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text


def create_db_session():    #   Создание сессии и установка локали
    engine = create_connection_to_database()
    session = sessionmaker(bind=engine)
    sess = session()
    sess.execute(text("SET lc_time TO 'ru_RU'"))
    return sess
