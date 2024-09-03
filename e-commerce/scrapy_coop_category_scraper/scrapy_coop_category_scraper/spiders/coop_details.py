import csv
from datetime import datetime

import scrapy


class CoopDetailsSpider(scrapy.Spider):
    name = 'coop_details'
    base_url = 'https://www.coop.nl{}'
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'coop_details.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }
    headers = {
        'authority': 'www.coop.nl',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36 '
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_urls = self.get_search_urls()

    def get_search_urls(self):
        with open('coop_categories.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_urls[0:1]:
            cate_url = data.get('Category_url', '')
            item = {'category_url': cate_url,
                    'page_no': 1}
            yield scrapy.Request(url=cate_url, callback=self.parse, meta={'item': item}, headers=self.headers)

    def parse(self, response):
        item = response.meta['item']
        rank = 1
        for data in response.xpath("//div[@class='product-list ng-star-inserted']/div[contains(@class,"
                                   "'product-list__column')]//div[contains(@class,'ng-star-inserted')]"):
            yield {
                'Product_Name': data.xpath("./a[contains(@class,'product-card-mobile__title')]/p["
                                           "@itemprop='name']/text()").get(),
                'Product_Weight': data.xpath("./a[contains(@class,'product-card-mobile__title')]/p[contains(@class,"
                                             "'ng-star-inserted')]/text()").get(),
                'Price': data.xpath("./div[2]/custom-product-price/meta[@itemprop='price']/@content").get(),
                'Image_url': data.xpath(".//div[@class='defer-load']/img/@src").get(),
                'Product_url': self.base_url.format(data.xpath("./a/@href").get()) if not data.xpath(
                    "./a/@href").get().startswith(self.base_url) else data.xpath("./a/@href").get(),
                'Product_Rank': rank,
                'Page_no': item.get('page_no', ''),
                'Category_url': item.get('category_url', ''),
                'Page_url': response.url,
                'TimeStamp': datetime.now().strftime("%d-%m-%Y %H:%M:%S")

            }
            rank += 1
        next_page = response.xpath("//span[@class='pagination-list-paging__next']/a/@href").get()
        if next_page:
            item['page_no'] = next_page.split('=')[-1]
            yield scrapy.Request(url=self.base_url.format(next_page), callback=self.parse,
                                 meta={'item': item}, headers=self.headers)
