import csv
from datetime import datetime

import scrapy


class RossmannDetailsSpider(scrapy.Spider):
    name = 'rossmann_details'
    base_url = 'https://www.rossmann.de{}'
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'rossmann_details.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }

    headers = {
        'authority': 'www.rossmann.de',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36 '
    }

    def __init__(self, *args):
        super().__init__(*args)
        self.request_urls = self.get_search_urls()

    def get_search_urls(self):
        with open('rossmann_categories.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_urls:
            cate_url = data.get('Category_Url', '')
            item = {'category_url': cate_url,
                    'page_no': 1}
            yield scrapy.Request(url=cate_url, headers=self.headers,
                                 meta={'item': item}, callback=self.parse)

    def parse(self, response):
        item = response.meta['item']
        rank = 1
        for data in response.css(".rm-tile-product"):
            pro_url = data.css("::attr(href)").get()
            if not pro_url.startswith(self.base_url):
                pro_url = self.base_url.format(pro_url)
            value = data.css("div.rm-price__base::text").getall()
            yield {
                'Product_id': data.css("::attr(data-product-id)").get(),
                'Product_Name': data.css("::attr(data-product-name)").get(),
                'Brand_Name': data.css("::attr(data-product-brand)").get(),
                'Product_NetPrice': data.css("::attr(data-product-netprice)").get(),
                'Product_Price': data.css("::attr(data-product-price)").get(),
                'Product_Baseprice': value[1].replace('\n', '').replace(' ', '') if value[1] else '',
                'Product_Weight': value[3].replace('\n', '').replace(' ', '') if value[3] else '',
                'Product_Category': data.css("::attr(data-product-category)").get(),
                'Product_reviewcount': data.css("div.rm-rating span.rm-rating__note::text").get(),
                'Image_Url': data.css("img::attr(data-src)").get(),
                'Product_Url': pro_url,
                'Category_Url': item.get('category_url'),
                'Product_Rank': rank,
                'Page_No': item.get('page_no'),
                'TimeStamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            rank += 1
        next_page = response.css("a[rel='next']::attr(href)").get()
        if next_page:
            item['page_no'] = item.get('page_no') + 1
            yield scrapy.Request(url=self.base_url.format(next_page), headers=self.headers,
                                 meta={'item': item}, callback=self.parse)
