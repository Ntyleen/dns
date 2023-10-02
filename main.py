from loguru import logger

from src.utils.time_script import time_stamp
from src.Database_manage.create_db_session import create_db_session
from src.Database_manage.data_loader_multiprocess import load_all_data
from src.Database_manage.table_class import Sales, ProductsClasses, Products, Cities, Branches
from src.Database_manage.create_tables import create_tables
from src.Database_manage.mapping import data_mapping, column_mapping
from src.Analytics_and_calculations.analytics import queries_to_csv
from src.Analytics_and_calculations.Calcs import product_to_csv, load_classified_products
from src.Analytics_and_calculations.visualisation import vis_to_queries
from src.Analytics_and_calculations.sql_queries import (get_top_sales_hour_and_day, get_top_cities_by_sales,
                                                        get_top_branches_by_sales, get_top_branches_by_sales_value,
                                                        get_top_products_by_sales_at_branches,
                                                        get_top_products_by_sales_at_storages,
                                                        get_sales_by_hour, get_sales_by_day)


@time_stamp
def main():
    logger.info("Начато выполнение задания")
    try:
        tables = [Cities, Products, Branches, Sales, ProductsClasses]

        # Создание таблиц
        create_tables(tables)

        # Загрузка данных в таблицы
        load_all_data(data_mapping)

        #   Создание сессии
        sess = create_db_session()

        # Выполнение запросов к бд и сохранение результатов в папку результов
        queries_to_csv(sess,
                       [get_top_branches_by_sales, get_top_branches_by_sales_value,
                        get_top_products_by_sales_at_branches,
                        get_top_products_by_sales_at_storages, get_top_cities_by_sales, get_top_sales_hour_and_day,
                        get_sales_by_day, get_sales_by_hour], column_mapping)
        # создание dataframe для построения график
        df_line = queries_to_csv(sess, [get_sales_by_hour], column_mapping, False)
        # Запуск скрипта построения графика
        vis_to_queries(df_line, "Час", "Количество_продаж", "Продаж в каждом часе")

        df_bar = queries_to_csv(sess, [get_sales_by_day], column_mapping, False)
        vis_to_queries(df_bar, "День_недели", "Количество_продаж", "Продаж по дням недели", "bar")

        # Запуск скрипта для классификации товаров и сохранения результатов в папку результатов
        product_to_csv(sess, column_mapping["classify_products"])

        # Загрузка результатов в бд
        load_classified_products()
    except Exception as e:
        logger.error(f"Ошибка при выполнении одного из модулей: {e}")


if __name__ == '__main__':
    main()
