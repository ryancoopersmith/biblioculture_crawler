import os
import csv
import glob
import MySQLdb
from scrapy import Spider
from scrapy.http import Request
import ConfigParser

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

        authors = response.xpath('//*[@itemprop="author"]/a/text()').extract_first()
        author = ','.join(authors)
        
        image = response.xpath('//*[@id="gallery"]/img/@src').extract_first()

        isbn_13 = response.xpath('//*[@id="seemore"]/p/text()[2]').extract_first()

        price = response.xpath('//*[@class="price"]/text()').extract_first()

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
