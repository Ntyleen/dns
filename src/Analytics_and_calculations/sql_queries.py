import inspect

from src.Database_manage.table_class import Cities, Products, Branches, Sales
from src.utils.time_script import time_stamp

from sqlalchemy import func, extract, Integer, case
from loguru import logger


@time_stamp
def get_top_branches_by_sales(sess, limit=10):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        top_branches_by_sales = (
            sess.query(Branches.Наименование.label("Название_филиала"),
                       func.sum(Sales.Количество).label("Количество_продаж"))
            .join(Sales, Branches.Ссылка == Sales.Филиал)
            .filter(~Branches.Наименование.op('SIMILAR TO')('%(склад|Склад|cклад|Cклад)%'))
            .group_by(Branches.Наименование)
            .order_by(func.sum(Sales.Количество).desc())
            .limit(limit)
        ).all()
        logger.info("Запрос успешно выполнен.")
        return top_branches_by_sales
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def get_top_branches_by_sales_value(sess, limit=10):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        top_branches_by_sales_value = (
            sess.query(Branches.Наименование.label("Название_филиала"),
                       func.sum(Sales.Количество).label("Количество_продаж"))
            .join(Sales, Branches.Ссылка == Sales.Филиал)
            .filter(Branches.Наименование.op('SIMILAR TO')('%(склад|Склад|cклад|Cклад)%'))
            .group_by(Branches.Наименование)
            .order_by(func.sum(Sales.Количество).desc())
            .limit(limit)
        ).all()
        logger.info("Запрос успешно выполнен.")
        return top_branches_by_sales_value
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def get_top_products_by_sales_at_storages(sess, limit=10):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        top_products_by_sales_at_storages = (
            sess.query(Products.Наименование.label("Название_товара"),
                       func.sum(Sales.Количество).label("Количество_продаж"))
            .join(Sales, Products.Ссылка == Sales.Номенклатура)
            .join(Branches, Sales.Филиал == Branches.Ссылка)
            .filter(Branches.Наименование.op('SIMILAR TO')('%(склад|Склад|cклад|Cклад)%'))
            .group_by(Products.Наименование)
            .order_by(func.sum(Sales.Количество).desc())
            .limit(limit)
        ).all()
        logger.info("Запрос успешно выполнен.")
        return top_products_by_sales_at_storages
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def get_top_products_by_sales_at_branches(sess, limit=10):
    func_name = inspect.currentframe().f_code.co_name

    logger.info(f"Выполнение запроса {func_name}...")
    try:
        top_products_by_sales_at_branches = (
            sess.query(Products.Наименование.label("Название_товара"),
                       func.sum(Sales.Количество).label("Количество_продаж"))
            .join(Sales, Products.Ссылка == Sales.Номенклатура)
            .join(Branches, Sales.Филиал == Branches.Ссылка)
            .filter(~Branches.Наименование.op('SIMILAR TO')('%(склад|Склад|cклад|Cклад)%'))
            .group_by(Products.Наименование)
            .order_by(func.sum(Sales.Количество).desc())
            .limit(limit)
        ).all()
        logger.info("Запрос успешно выполнен.")
        return top_products_by_sales_at_branches
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def get_top_cities_by_sales(sess, limit=10):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        top_cities_by_sales = (
            sess.query(Cities.Наименование.label("Город"), func.sum(Sales.Количество).label("Количество_продаж"))
            .join(Branches, Cities.Ссылка == Branches.Город)
            .join(Sales, Branches.Ссылка == Sales.Филиал)
            .group_by(Cities.Наименование)
            .order_by(func.sum(Sales.Количество).desc())
            .limit(limit)
        ).all()
        logger.info("Запрос успешно выполнен.")
        return top_cities_by_sales
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def get_top_sales_hour_and_day(sess, limit=1):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        top_sales_hour_and_day = [
            (
                sess.query(
                    func.to_char(Sales.Период, 'Day').label("День_недели"),
                    func.cast(extract('hour', Sales.Период), Integer).label("Час"),
                    func.count(Sales.Количество).label("Количество_продаж")
                )
                .group_by(func.to_char(Sales.Период, 'Day'), extract('hour', Sales.Период))
                .order_by(func.count(Sales.Количество).desc())
                .limit(limit)
            ).first()
        ]
        logger.info("Запрос успешно выполнен.")
        return top_sales_hour_and_day
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def get_sales_by_hour(sess):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        sales_by_hour = (
            sess.query(
                func.cast(extract('hour', Sales.Период), Integer).label("Час"),
                func.count(Sales.Количество).label("Количество_продаж")
            )
            .group_by(extract('hour', Sales.Период))
            .order_by(extract('hour', Sales.Период))
        ).all()
        logger.info("Запрос успешно выполнен.")
        return sales_by_hour
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def get_sales_by_day(sess):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        sales_by_day = (
            sess.query(
                func.to_char(Sales.Период, 'Day').label("День_недели"),
                func.count(Sales.Количество).label("Количество_продаж")
            )
            .group_by(func.to_char(Sales.Период, 'Day'))
            .order_by(func.to_char(Sales.Период, 'Day'))
        ).all()
        logger.info("Запрос успешно выполнен.")
        return sales_by_day
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def compute_product_sales(sess):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        product_sales = (
            sess.query(
                Products.Ссылка.label("Номенклатура"),
                func.sum(Sales.Количество).label("Объем_продаж")
            )
            .join(Sales, Products.Ссылка == Sales.Номенклатура)
            .group_by(Products.Ссылка)
        ).subquery()
        logger.info("Запрос успешно выполнен.")
        return product_sales
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def compute_quantiles(sess, product_sales):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        quantile_90 = sess.query(func.percentile_cont(0.9).within_group(product_sales.c.Объем_продаж.asc())).scalar()
        quantile_30 = sess.query(func.percentile_cont(0.3).within_group(product_sales.c.Объем_продаж.asc())).scalar()
        logger.info("Запрос успешно выполнен.")
        return quantile_90, quantile_30
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")


@time_stamp
def classify_products(sess, product_sales, quantile_90, quantile_30):
    func_name = inspect.currentframe().f_code.co_name
    logger.info(f"Выполнение запроса {func_name}...")
    try:
        products_classified = sess.query(
            product_sales.c.Номенклатура,
            case(

                (product_sales.c.Объем_продаж >= quantile_90, "Наиболее продаваемый"),
                (product_sales.c.Объем_продаж < quantile_30, "Наименее продаваемый"),
                else_="Средне продаваемый"
            ).label("КлассТовара")
        )
        logger.info("Запрос успешно выполнен.")
        return products_classified
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса {func_name}: {e}")
