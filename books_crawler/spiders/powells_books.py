import os
import csv
import glob
import MySQLdb
from scrapy import Spider
from scrapy.http import Request
import ConfigParser
from random import randint
import re
import csv

config = ConfigParser.ConfigParser()
config.read(os.path.dirname(__file__) + '/../config.ini')

class PowellsBooksSpider(Spider):
    name = 'powells_books'
    allowed_domains = ['powells.com']
    start_urls = ['http://powells.com/used/arts-and-entertainment']

    def parse(self, response):
        books = response.xpath('//*[@class="width-fixer"]/a/@href').extract()
        for book in books:
            absolute_url = response.urljoin(book)
            yield Request(absolute_url, callback=self.parse_book)

        next_page_urls = response.xpath('//*[@class="dontmiss"]/ul/li/a/@href').extract()
        for next_page_url in next_page_urls:
            absolute_next_page_url = response.urljoin(next_page_url)
            yield Request(absolute_next_page_url)

    def parse_book(self, response):
        name = response.xpath('//*[@class="book-title"]/text()').extract_first()

        authors = response.xpath('//*[@itemprop="author"]/a/text()').extract()
        author = ','.join(authors)

        isbn_10 = ''
        isbn_13 = response.xpath('//*[@id="seemore"]/p/text()[2]').extract_first()

        image = response.xpath('//*[@id="gallery"]/img/@src').extract_first()

        price = response.xpath('//*[@class="price"]/text()').extract_first()
        if price:
            price = re.sub('\\r', '', price)
            price = re.sub('\\n', '', price)
            price = re.sub('\$', '', price)
        else:
            price = 0

        book_id = randint(0,2000000000)
        price_id = randint(0,2000000000)
        site_id = 4

        yield {
            'book_id': book_id,
            'name': name,
            'author': author,
            'isbn_10': isbn_10,
            'isbn_13': isbn_13,
            'image': image,
            'locations_book_id': book_id,
            'locations_site_id': site_id,
            'price_id': price_id,
            'price': price,
            'prices_book_id': book_id,
            'site_prices_site_id': site_id,
            'site_prices_price_id': price_id
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
                book_id = cursor.execute('SELECT id FROM books WHERE name = %s', [row[1]])
                if book_id:
                    locations_values = [book_id, row[7]]
                    cursor.execute('INSERT IGNORE INTO locations(book_id, site_id) VALUES(%s, %s)', locations_values)
                    prices_values = [row[8], row[9], book_id]
                    cursor.execute('INSERT IGNORE INTO prices(id, price, book_id) VALUES(%s, %s, %s)', prices_values)
                else:
                    cursor.execute('INSERT IGNORE INTO books(id, name, author, isbn_10, isbn_13, image) VALUES(%s, %s, %s, %s, %s, %s)', row[0:6])
                    cursor.execute('INSERT IGNORE INTO locations(book_id, site_id) VALUES(%s, %s)', row[6:8])
                    cursor.execute('INSERT IGNORE INTO prices(id, price, book_id) VALUES(%s, %s, %s)', row[8:11])
                cursor.execute('INSERT IGNORE INTO site_prices(site_id, price_id) VALUES(%s, %s)', row[11:13])
            row_count += 1

        mydb.commit()
        cursor.close()
