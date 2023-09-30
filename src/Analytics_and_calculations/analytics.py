import pandas as pd
from loguru import logger

from src.Database_manage.create_dataframe import create_dataframe_by
from src.utils.time_script import time_stamp

@time_stamp
def queries_to_csv(sess, query_funcs, column_mapping, save_to_csv=True):
    results = {}

    for query_func in query_funcs:
        data = query_func(sess)

        df = create_dataframe_by(data, column_mapping[query_func.__name__])

        if save_to_csv:
            csv_filename = f"{query_func.__name__}.csv"
            df.to_csv(csv_filename, index=False)
            logger.info(f"Результат сохранен в {csv_filename}")
        else:
            results[query_func.__name__] = df
            logger.info(f"Данные сохранены во фрейм")
    if not save_to_csv:
        return results
