# -*- coding: utf-8 -*-
from scrapy import Spider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request

from scrapy.loader import XPathItemLoader
from scrapy.loader.processor import Join, MapCompose

import AmazonBooks

# def product_info(response, value):
#     return response.xpath('//th[text()="' + value + '"]/following-sibling::td/text()').extract_first()

class AmazonBooksSpider(Spider):
    name = 'amazon_books'
    allowed_domains = ['books.toscrape.com']
    start_urls = ['http://books.toscrape.com']

    books_list_xpath = '//h3/a/@href'
    item_fields = {
        'title': './/h1/text()',
        'price': './/*[@class="price_color"]/text()'
    }

    def parse(self, response):
        selector = HtmlXPathSelector(response)

        for book in selector.select(self.books_list_xpath):
            loader = XPathItemLoader(AmazonBooks(), selector=book)

            # define processors
            loader.default_input_processor = MapCompose(unicode.strip)
            loader.default_output_processor = Join()

            # iterate over fields and add xpaths to the loader
            for field, xpath in self.item_fields.iteritems():
                loader.add_xpath(field, xpath)
            yield loader.load_item()

        loader = XPathItemLoader(AmazonBooks(), selector=book)

        # process next page
        # next_page_url = response.xpath('//a[text()="next"]/@href').extract_first()
        # absolute_next_page_url = response.urljoin(next_page_url)
        # yield Request(absolute_next_page_url)

        # image_url = response.xpath('//img/@src').extract_first()
        # image_url = image_url.replace('../..', 'http://books.toscrape.com/')
        #
        # rating = response.xpath('//*[contains(@class, "star-rating")]/@class').extract_first()
        # rating = rating.replace('star-rating ', '')
        #
        # description = response.xpath(
        #     '//*[@id="product_description"]/following-sibling::p/text()').extract_first()
        #
        # # product information data points
        # upc = product_info(response, 'UPC')
        # product_type = product_info(response, 'Product Type')
        # price_without_tax = product_info(response, 'Price (excl. tax)')
        # price_with_tax = product_info(response, 'Price (incl. tax)')
        # tax = product_info(response, 'Tax')
        # availability = product_info(response, 'Availability')
        # number_of_reviews = product_info(response, 'Number of reviews')

        # yield {
        #     'title': title,
        #     'price': price
        #     }
