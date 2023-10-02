from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.relationships import Relationship

base = declarative_base()


# Определение таблиц классами

class Sales(base):
    __tablename__ = 't_sales'

    #   Определение стобцов и их типов

    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    Период = Column(DateTime)  # "period"
    Филиал = Column(String(100), ForeignKey('t_branches.Ссылка'), index=True)  # "link_on_shop"
    Номенклатура = Column(String(100), ForeignKey('t_products.Ссылка'), index=True)  # "link_on_product"
    Количество = Column(Float(1))  # "amount"
    Продажа = Column(Float)  # "sale"

    product_rel = Relationship("Products", backref="sales_rel")
    branch_rel = Relationship("Branches", backref="sales_rel")


class Products(base):
    __tablename__ = 't_products'
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    Ссылка = Column(String(100), primary_key=True, unique=True)  # "link_product"
    Наименование = Column(String(500))  # " name"


class Cities(base):
    __tablename__ = 't_cities'
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    Ссылка = Column(String(100), primary_key=True, unique=True)  # " link_city"
    Наименование = Column(String(50), unique=True)  # " city"


class Branches(base):
    __tablename__ = 't_branches'
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    Ссылка = Column(String(100), primary_key=True, unique=True)  # " link_branch"
    Наименование = Column(String(50), unique=True)  # " name_of_branch"
    Город = Column(String(100), ForeignKey('t_cities.Ссылка'), index=True)  # " link_on_city"
    КраткоеНаименование = Column(String(50))  # " short_name_of_branch"
    Регион = Column(String(50))  # " region"

    city_rel = Relationship("Cities", backref="branches_rel")


class ProductsClasses(base):
    __tablename__ = 'Classified_products'
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    Номенклатура = Column(String(500), ForeignKey('t_products.Ссылка'))
    КлассТовара = Column(String(50))
