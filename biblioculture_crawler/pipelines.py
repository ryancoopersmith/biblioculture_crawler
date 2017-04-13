# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from sqlalchemy.orm import sessionmaker
from models import Books, db_connect, create_books_table

class BibliocultureCrawlerPipeline(object):
    def process_item(self, item, spider):
        return item


class AmazonBooksPipeline(object):
    """AmazonBooks pipeline for storing scraped items in the database"""
    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates books table.
        """
        engine = db_connect()
        create_books_table(engine)
        self.Session = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        """Save books in the database.

        This method is called for every item pipeline component.

        """
        session = self.Session()
        book = Books(**item)

        try:
            session.add(book)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item
