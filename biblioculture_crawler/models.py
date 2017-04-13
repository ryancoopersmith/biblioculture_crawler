from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL

import settings


DeclarativeBase = declarative_base()


def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(URL(**settings.DATABASE))


def create_books_table(engine):
    """"""
    DeclarativeBase.metadata.create_all(engine)


class Books(DeclarativeBase):
    """Sqlalchemy books model"""
    __tablename__ = "books"

    title = Column('Title', String, nullable=True)
    price = Column('Price', String, nullable=True)

    # id = Column(Integer, primary_key=True)
    # upc = Column('UPC', String, nullable=True)
    # product_type = Column('Product Type', String, nullable=True)
    # price_without_tax = Column('Price (excl. tax)', String, nullable=True)
    # price_with_tax = Column('Price (incl. tax)', String, nullable=True)
    # tax = Column('Tax', String, nullable=True)
    # availability = Column('Availability', String, nullable=True)
    # number_of_reviews = Column('Number of reviews', String, nullable=True)
