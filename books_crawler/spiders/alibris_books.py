import os
import csv
import glob
import MySQLdb
from scrapy import Spider
from scrapy.http import Request
import ConfigParser
from random import randint
import csv
import re

config = ConfigParser.ConfigParser()
config.read(os.path.dirname(__file__) + '/../config.ini')

categories = ['Architecture', 'Art', 'religion-Bibles', 'Biography-Autobiography', 'Business-Economics', 'Business-Economics-Careers',
    'Children\'s-Fiction', 'Comics-Graphic-Novels', 'Computers', 'Cooking', 'Reference-Dictionaries', 'Drama', 'Reference-Encyclopedias',
    'Fiction-Erotica', 'Family-Relationships', 'Fiction-Fantasy', 'Fiction', 'Comics-Graphic-Novels-Graphic-Novels', 'Health-Fitness',
    'History', 'Fiction-Horror', 'Humor', 'Reference-Maps', 'Mathematics', 'Biography-Autobiography-Personal-Memoirs', 'Fiction-Mystery-Detective',
    'Poetry', 'religion', 'Fiction-Romance', 'Science', 'Fiction-Science-Fiction', 'Self-Help', 'Sports-Recreation', 'Travel']

class AlibrisBooksSpider(Spider):
    name = 'alibris_books'
    allowed_domains = ['alibris.com']
    start_urls = ['http://alibris.com/search/books']

    def parse(self, response):
        for category in categories:
            absolute_url = response.urljoin('/search/books/subject/' + category)
            yield Request(absolute_url, callback=self.parse_category)

    def parse_category(self, response):
        books = response.xpath('//*[@id="selected-works"]/ul/li/a/@href').extract()
        for book in books:
            absolute_url = response.urljoin(book)
            yield Request(absolute_url, callback=self.parse_book)

        next_page_url = response.xpath('//*[@id="selected-works"]/ol/li[12]/a/@href').extract_first()
        if next_page_url:
            absolute_next_page_url = response.urljoin(next_page_url)
            yield Request(absolute_next_page_url, callback=self.parse_category)

    def parse_book(self, response):
        name = response.xpath('//*[@class="product-title"]/h1/text()').extract_first()

        authors = response.xpath('//*[@itemprop="author"]/*[@itemprop="name"]/text()').extract()
        author = ', '.join(authors)

        isbn_10 = ''
        isbn_13 = response.xpath('//*[@class="isbn-link"]/text()').extract_first()

        image = response.xpath('//*[@itemprop="image"]/@src').extract_first()

        price = response.xpath('//*[@id="tabAll"]/span/text()').extract_first()
        if price:
            price = price.split(' ')
            price = price[1]
            price = re.sub('\$', '', price)
        else:
            price = 0

        book_id = randint(0,2000000000)
        price_id = randint(0,2000000000)
        site_id = 3

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
