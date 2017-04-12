import scrapy
from scrapy import Selector

class AmazonBooksSpider(scrapy.Spider):
    name = "amazon_books"
    start_urls = [
        'https://www.amazon.com/b/ref=s9_acss_bw_en_BGG15eve_d_1_1_w?_encoding=UTF8&node=1&pd_rd_r=XH38QTWP922WGKVA3KP0&pd_rd_w=FM9M4&pd_rd_wg=poHR3&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-top-3&pf_rd_r=XH38QTWP922WGKVA3KP0&pf_rd_r=XH38QTWP922WGKVA3KP0&pf_rd_t=101&pf_rd_p=e8ce74da-9c3d-45b7-a3fd-ea13b4732f00&pf_rd_p=e8ce74da-9c3d-45b7-a3fd-ea13b4732f00&pf_rd_i=283155',
    ]

    def parse(self, response):

        for book in response.css('div.a-fixed-left-grid'):
            yield {
                'name': book.css('h2.s-access-title::text').extract_first(),
                'author': book.css('a.a-link-normal::text').extract_first()
            }

        # next_page = response.css('li.next a::attr(href)').extract_first()
        # if next_page is not None:
        #     next_page = response.urljoin(next_page)
        #     yield scrapy.Request(next_page, callback=self.parse)
