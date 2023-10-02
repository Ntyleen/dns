import pandas as pd



def create_dataframe_by(func_sql, columns):  # Модуль создания dataframe для запросов
    return pd.DataFrame(func_sql, columns=columns)
