# -*- coding: utf-8 -*-
from scrapy import Spider
from scrapy.http import Request

class AmazonBooksSpider(Spider):
    name = 'books'
    allowed_domains = ['books.toscrape.com']
    start_urls = (
        'http://books.toscrape.com/',
    )

    def parse(self, response):
        books = response.xpath('//*[@class="book"]')
        for book in books:
            text = book.xpath(
                './/*[@class="text"]/text()').extract_first()

            author = book.xpath(
                './/*[@itemprop="author"]/text()').extract_first()

            tags = book.xpath('.//*[@class="tag"]/text()').extract()

            yield {
                    'Text': text,
                    'Author': author,
                    'Tags': tags
            }

        next_page_url = response.xpath(
            '//*[@class="next"]/a/@href').extract_first()
        absolute_next_page_url = response.urljoin(next_page_url)
        yield Request(absolute_next_page_url)
