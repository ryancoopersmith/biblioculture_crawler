import scrapy
from scrapy.crawler import CrawlerProcess
# import spiders
from spiders.alibris_books import *
from spiders.amazon_books import *
from spiders.ebay_books import *
from spiders.powells_books import *

process = CrawlerProcess()
process.crawl(AlibrisBooksSpider)
process.crawl(AmazonBooksSpider)
process.crawl(EbayBooksSpider)
process.crawl(PowellsBooksSpider)
process.start()
