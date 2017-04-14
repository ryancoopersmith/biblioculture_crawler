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

def isbn(response, value):
    return response.xpath('//*[text()="' + value + '"]/following-sibling::span/a/text()').extract_first()

class EbayBooksSpider(Spider):
    name = 'ebay_books'
    allowed_domains = ['ebay.com']
    start_urls = ['http://books.products.half.ebay.com/antiques-collectibles_W0QQcZ4QQcatZ218176']

    def parse(self, response):
        books = response.xpath('//*[@class="imageborder"]/@href').extract()
        for book in books:
            absolute_url = response.urljoin(book)
            yield Request(absolute_url, callback=self.parse_book)

        # process next page
        next_page_url = response.xpath('//*[text()="Next"]/@href').extract_first()
        absolute_next_page_url = response.urljoin(next_page_url)
        yield Request(absolute_next_page_url)

    def parse_book(self, response):
        name = response.xpath('//*[@class="pdppagetitle"]/text()').extract_first()
        author = response.xpath('//*[@class="pdplinks"]/*[@class="pdplinks"]/text()').extract_first()
        image = response.xpath('//tr/td/*[@class="imageborder"]/@src').extract_first()

        ISBN_10 = isbn(response, 'ISBN-10:')
        ISBN_13 = isbn(response, 'ISBN-13:')

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
