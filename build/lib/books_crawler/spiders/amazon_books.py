import os
import csv
import glob
import MySQLdb
from scrapy import Spider
from scrapy.http import Request
import ConfigParser
import re
import uuid

config = ConfigParser.ConfigParser()
config.read(os.path.dirname(__file__) + '/../config.ini')

def isbn(response, value):
    if response.xpath('//*[@class="content"]/ul/li/text()')[value].extract() == u' English' or response.xpath('//*[@class="content"]/ul/li/text()')[value - 1].extract() == u' English' and value == 4:
        value += 1
    return response.xpath('//*[@class="content"]/ul/li/text()')[value].extract()

class AmazonBooksSpider(Spider):
    name = 'amazon_books'
    allowed_domains = ['amazon.com']
    start_urls = ['https://amazon.com/b/ref=usbk_surl_books/?node=283155']

    def parse(self, response):
        categories = response.xpath('//*[@id="ref_1000"]/li/a/@href').extract()
        for category in categories:
            absolute_url = response.urljoin(category)
            yield Request(absolute_url, callback=self.parse_category)
            # Not going to next pages before switching categories
    def parse_category(self, response):
        books = response.xpath('//a[contains(@class, "a-link-normal") and contains(@class, "s-access-detail-page")]/@href').extract()
        for book in books:
            absolute_url = response.urljoin(book)
            yield Request(absolute_url, callback=self.parse_book)

        next_page_url = response.xpath('//a[@title="Next Page"]/@href').extract_first()
        if next_page_url:
            absolute_next_page_url = response.urljoin(next_page_url)
            yield Request(absolute_next_page_url, callback=self.parse_category)

    def parse_book(self, response):
        name = response.xpath('//span[@id="productTitle"]/text()').extract_first()

        authors = response.xpath('//*[@id="byline"]/span/span/a/text()|//*[@id="byline"]/span/a/text()').extract()
        author = ', '.join(authors)

        isbn_10 = isbn(response, 3)
        isbn_13 = isbn(response, 4)

        image = response.xpath('//*[@id="imgBlkFront"]/@data-a-dynamic-image').extract_first()
        image = re.sub('\":.*', '', image)
        image = re.sub('{\"', '', image)

        used_price = response.xpath('//*[@class="olp-used olp-link"]/a/text()')[1].extract()
        used_price = re.sub('\\n.*', '', used_price)
        used_price_compare = re.sub('\$', '', used_price)
        used_price_compare = float(used_price_compare)
        new_price = response.xpath('//*[@class="olp-new olp-link"]/a/text()')[1].extract()
        new_price = re.sub('\\n.*', '', new_price)
        new_price_compare = re.sub('\$', '', new_price)
        new_price_compare = float(new_price_compare)

        if used_price_compare <= new_price_compare:
            price = used_price
        else:
            price = new_price

        book_id = uuid.uuid4()
        price_id = uuid.uuid4()
        site_id = 1

        yield {
            'name': name,
            'author': author,
            'isbn_10': isbn_10,
            'isbn_13': isbn_13,
            'image': image,
            'price': price,
            'book_id': book_id,
            'price_id': price_id,
            'site_id': site_id
            }

    def close(self, reason):
        csv_file = max(glob.iglob('*.csv'), key=os.path.getctime)

        Username = config.get('database', 'Username')
        Password = config.get('database', 'Password')

        mydb = MySQLdb.connect(host='localhost',
                               user=Username,
                               passwd=Password,
                               db='biblioculture_development')
        cursor = mydb.cursor()

        csv_data = csv.reader(file(csv_file))

        row_count = 0
        for row in csv_data:
            if row_count != 0:
                cursor.execute('INSERT IGNORE INTO books(name, author, isbn_10, isbn_13, image) VALUES(%s, %s, %s, %s, %s)', row)
                cursor.execute('INSERT IGNORE INTO locations(book_id, site_id) VALUES(%s, %s)', row)
                cursor.execute('INSERT IGNORE INTO prices(price, book_id) VALUES(%s, %s)', row)
                cursor.execute('INSERT IGNORE INTO site_prices(site_id, price_id) VALUES(%s, %s)', row)
            row_count += 1

        mydb.commit()
        cursor.close()
