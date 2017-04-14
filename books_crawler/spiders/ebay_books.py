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
    start_urls = ['http://books.products.half.ebay.com']

    def parse(self, response):
        categories = response.xpath('//tr/td[2]/a/@href')[3:54].extract()
        for category in categories:
            absolute_url = response.urljoin(category)
            yield Request(absolute_url, callback=self.parse_category)

    def parse_category(self, response):
        books = response.xpath('//*[@class="imageborder"]/@href').extract()
        for book in books:
            absolute_url = response.urljoin(book)
            yield Request(absolute_url, callback=self.parse_book)

        next_page_url = response.xpath('//*[text()="Next"]/@href').extract_first()
        absolute_next_page_url = response.urljoin(next_page_url)
        yield Request(absolute_next_page_url, callback=self.parse_category)

    def parse_book(self, response):
        name = response.xpath('//*[@class="pdppagetitle"]/text()').extract_first()

        authors = response.xpath('//*[@class="pdplinks"]/*[@class="pdplinks"]/text()').extract_first()
        author = ', '.join(authors)

        isbn_10 = isbn(response, 'ISBN-10:')
        isbn_13 = isbn(response, 'ISBN-13:')

        image = response.xpath('//tr/td/*[@class="imageborder"]/@src').extract_first()

        price = response.xpath('//*[@class="pdpbestpricestyle"]/text()').extract_first()

        book_id = 0
        price_id = 0
        site_id = 2

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
