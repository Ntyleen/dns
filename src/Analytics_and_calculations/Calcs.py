from loguru import logger

from sql_queries import compute_product_sales, compute_quantiles, classify_products
from src.Database_manage.create_dataframe import create_dataframe_by
from src.Database_manage.data_loader import load_data_to_db
from src.Database_manage.table_class import ProductsClasses
from src.utils.time_script import time_stamp

@time_stamp
def product_to_csv(sess, column_mapping):
    product_sales = compute_product_sales(sess)

    quantile_90, quantile_30 = compute_quantiles(sess, product_sales)
    classified_products = classify_products(sess, product_sales, quantile_90, quantile_30)

    df = create_dataframe_by(classified_products, column_mapping["classify_products"])
    csv_filename = "classified_products.csv"
    df.to_csv(csv_filename, index=False)
    logger.info(f"Данные сохранены в файл {csv_filename}")

    return df

@time_stamp
def load_classified_products():
    csv_filename = "classified_products.csv"
    load_data_to_db(csv_filename, ProductsClasses)
    logger.info(f"Данные загруженны из {csv_filename} в базу")