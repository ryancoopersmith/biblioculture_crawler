import os
import csv
import glob
import MySQLdb
from scrapy import Spider
from scrapy.http import Request
import ConfigParser
import re
from random import randint
import csv

config = ConfigParser.ConfigParser()
config.read(os.path.dirname(__file__) + '/../config.ini')

def isbn(response, value):
    if response.xpath('//*[@class="content"]/ul/li/text()')[value].extract() == u' English' or response.xpath('//*[@class="content"]/ul/li/text()')[value - 1].extract() == u' English' and value == 4:
        value += 1

    unicode_alphabet = [u'a', u'b', u'c', u'd', u'e', u'f', u'g', u'h', u'i', u'j', u'k',
        u'l', u'm', u'n', u'o', u'p', u'q', u'r', u's', u't', u'u', u'v',
        u'w', u'x', u'y', u'z']
    for letter in unicode_alphabet:
        if letter == response.xpath('//*[@class="content"]/ul/li/text()')[value].extract()[3]:
            return 'not provided'

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
        if not name:
            name = response.xpath('//span[@id="ebooksProductTitle"]/text()').extract_first()

        authors = []
        roles = response.xpath('//*[@id="byline"]/span/span/span/text()').extract()
        if len(roles) > 0:
            if 'Author' in roles[0]:
                author1 = response.xpath('//*[@id="byline"]/span/span/a/text()|//*[@id="byline"]/span/a/text()')[0].extract()
                authors.append(author1)
        if len(roles) > 1:
            if 'Author' in roles[1]:
                author2 = response.xpath('//*[@id="byline"]/span/span/a/text()|//*[@id="byline"]/span/a/text()')[1].extract()
                authors.append(author2)
        if len(roles) > 2:
            if 'Author' in roles[2]:
                author3 = response.xpath('//*[@id="byline"]/span/span/a/text()|//*[@id="byline"]/span/a/text()')[2].extract()
                authors.append(author3)
        author = ', '.join(authors)
        if author == '':
            author = 'not provided'

        isbn_10 = isbn(response, 3)
        isbn_13 = isbn(response, 4)

        image = response.xpath('//*[@id="imgBlkFront"]/@data-a-dynamic-image').extract_first()
        if not image:
            image = response.xpath('//*[@id="ebooksImgBlkFront"]/@data-a-dynamic-image').extract_first()
        image = re.sub('\":.*', '', image)
        image = re.sub('{\"', '', image)

        if response.xpath('//*[@class="olp-used olp-link"]/a/text()').extract():
            used_price = response.xpath('//*[@class="olp-used olp-link"]/a/text()')[1].extract()
            used_price = re.sub('\\n.*', '', used_price)
            used_price = re.sub('\$', '', used_price)
            used_price_compare = float(used_price)
        else:
            used_price_compare = 0
            used_price = 0

        if response.xpath('//*[@class="olp-new olp-link"]/a/text()').extract():
            new_price = response.xpath('//*[@class="olp-new olp-link"]/a/text()')[1].extract()
            new_price = re.sub('\\n.*', '', new_price)
            new_price = re.sub('\$', '', new_price)
            new_price_compare = float(new_price)
        else:
            new_price_compare = 0
            new_price = 0

        if response.xpath('//*[@class="a-section a-spacing-small a-spacing-top-small"]/span/span/text()').extract():
            new_price = response.xpath('//*[@class="a-section a-spacing-small a-spacing-top-small"]/span/span/text()')[0].extract()
            used_price = response.xpath('//*[@class="a-section a-spacing-small a-spacing-top-small"]/span/span/text()')[1].extract()
            new_price = re.sub('\$', '', new_price)
            used_price = re.sub('\$', '', used_price)
            new_price_compare = float(new_price)
            used_price_compare = float(used_price)

        if used_price_compare <= new_price_compare:
            price = used_price
        else:
            price = new_price

        book_id = randint(0,2000000000)
        price_id = randint(0,2000000000)
        site_id = 1

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
