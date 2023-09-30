import pandas as pd
from loguru import logger

from src.Database_manage.create_dataframe import create_dataframe_by
from src.utils.time_script import time_stamp

# Функция для выполнения запросов к базе данных и сохранения результатов в CSV-файлы или возвращения их как DataFrame.
#
# Параметры:
# - sess: объект сессии, используется для выполнения запросов к базе данных.
# - query_funcs: список запросов-функций, каждая из которых выполняет определенный запрос к базе данных.
# - column_mapping: словарь, который определяет, как столбцы в результатах запроса должны быть переименованы для DataFrame.
#
# - Если save_to_csv=False, возвращает colunm_mapping, где ключ - именя функции запроса, а значение - соответствующий DataFrame с результатами запроса.

@time_stamp
def queries_to_csv(sess, query_funcs, column_mapping, save_to_csv=True):
    results = {}

    for query_func in query_funcs:
        data = query_func(sess)
        try:
            df = create_dataframe_by(data, column_mapping[query_func.__name__])
            logger.info(f"Данные успешно загружены в DataFrame для {query_func.__name__}.")

        except Exception as e:
            logger.error(f"Ошибка при создании DataFrame для {query_func.__name__}.")
            continue

        if save_to_csv:
            try:
                csv_filename = f"{query_func.__name__}.csv"
                df.to_csv(csv_filename, index=False)
                logger.info(f"Результат сохранен в {csv_filename}")

            except Exception as e:
                logger.error(f"Ошибка при сохранении в {csv_filename}: {e}")
        else:
            results[query_func.__name__] = df
            logger.info(f"Данные сохранены во фрейм")
    if not save_to_csv:
        return results
