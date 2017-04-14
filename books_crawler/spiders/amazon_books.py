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
    return response.xpath('//*[@class="content"]/ul/li/text()')[value].extract()

class AmazonBooksSpider(Spider):
    name = 'amazon_books'
    allowed_domains = ['amazon.com']
    start_urls = ['https://amazon.com/b/ref=usbk_surl_books/?node=283155']

    def parse(self, response):
        categories = response.xpath('//*[@id="ref_1000"]/li/a/@href').extract()
        for category in categories:
            absolute_url = response.urljoin(book)
            yield Request(absolute_next_page_url, callback=self.parse_category)

    def parse_category(self, response):
        books = response.xpath('//a[contains(@class, "a-link-normal") and contains(@class, "s-access-detail-page")]/@href').extract()
        for book in books:
            absolute_url = response.urljoin(book)
            yield Request(absolute_url, callback=self.parse_book)

        next_page_url = response.xpath('//a[@title="Next Page"]/@href').extract_first()
        absolute_next_page_url = response.urljoin(next_page_url)
        yield Request(absolute_next_page_url, callback=self.parse_category)

    def parse_book(self, response):
        name = response.xpath('//span[@id="productTitle"]/text()').extract_first()
        author = response.xpath('//a[contains(@class, "a-link-normal") and contains(@class, "contributorNameID")]/text()').extract_first()
        image = response.xpath('//img[contains(@class, "a-dynamic-image") and contains(@class, "image-stretch-vertical") and contains(@class,"frontImage")]/@src').extract_first()

        isbn_10 = isbn(response, 4)
        isbn_13 = isbn(response, 5)

        used_price = response.xpath('//*[@id="tmmSwatches"]/ul/li[2]/span/span[3]/span[1]/a/text()')[1].extract()
        new_price = response.xpath('//*[@id="tmmSwatches"]/ul/li[2]/span/span[3]/span[2]/a/text()')[1].extract()
        
        if used_price <= new_price:
            price = used_price
        else:
            price = new_price

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
