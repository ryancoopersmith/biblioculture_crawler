# -*- coding: utf-8 -*-
import os
import csv
import glob
import MySQLdb
from scrapy import Spider
from scrapy.http import Request
import ConfigParser

config = ConfigParser.ConfigParser()
config.read(os.path.dirname(__file__) + '/../config.ini')

class AlibrisBooksSpider(Spider):
    name = 'alibris_books'
    allowed_domains = ['alibris.com']
    start_urls = ['http://alibris.com/search/books/subject/Architecture']

    def parse(self, response):
        books = response.xpath('//*[@id="selected-works"]/ul/li/a/@href').extract()
        for book in books:
            absolute_url = response.urljoin(book)
            yield Request(absolute_url, callback=self.parse_book)

        # process next page
        next_page_url = response.xpath('//*[@id="selected-works"]/ol/li[12]/a/@href').extract_first()
        absolute_next_page_url = response.urljoin(next_page_url)
        yield Request(absolute_next_page_url)

    def parse_book(self, response):
        name = response.xpath('//*[@class="product-title"]/h1/text()').extract_first()
        author = response.xpath('//*[@itemprop="author"]/*[@itemprop="name"]/text()').extract_first()
        image = response.xpath('//*[@itemprop="image"]/@src').extract_first()

        isbn_13 = response.xpath('//*[@class="isbn-link"]/text()').extract_first()

        yield {
            'title': title,
            'rating': rating,
            'upc': upc,
            'product_type': product_type}

    def close(self, reason):
        csv_file = max(glob.iglob('*.csv'), key=os.path.getctime)

        Username = config.get('database', 'Username')
        Password = config.get('database', 'Password')

        mydb = MySQLdb.connect(host='localhost',
                               user=Username,
                               passwd=Password,
                               db='books_db')
        cursor = mydb.cursor()

        csv_data = csv.reader(file(csv_file))

        row_count = 0
        for row in csv_data:
            if row_count != 0:
                cursor.execute('INSERT IGNORE INTO books_table(rating, product_type, upc, title) VALUES(%s, %s, %s, %s)', row)
            row_count += 1

        mydb.commit()
        cursor.close()
