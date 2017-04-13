# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
from scrapy.item import Item, Field

class AmazonBooks(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = Field()
    price = Field()

    # upc = Field()
    # product_type = Field()
    # price_without_tax = Field()
    # price_with_tax = Field()
    # tax = Field()
    # availability = Field()
    # number_of_reviews = Field()
    pass
