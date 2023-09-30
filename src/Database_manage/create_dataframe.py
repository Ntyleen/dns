import pandas as pd

from create_db_session import create_db_session


def create_dataframe_by(func_sql, columns):     #   Модуль создания dataframe для запросов
    return pd.DataFrame(func_sql, columns=columns)
