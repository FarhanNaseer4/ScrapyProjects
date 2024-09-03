import csv
import json
from datetime import datetime
import scrapy


class DmDetailsSpider(scrapy.Spider):
    name = 'dm_details'
    request_api = 'https://product-search.services.dmtech.com/de/search/static?brandName={' \
                  '}&pageSize=30&sort=editorial_relevance&type=search-static&currentPage={} '
    base_url = 'https://www.dm.de{}'
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'dm_details.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }

    def __init__(self, *args):
        super().__init__(*args)
        self.brand_name = self.get_search_data()

    def get_search_data(self):
        with open('dm_categories.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.brand_name:
            brand_name = data.get('category_name', '')
            item = {'cate_url': data.get('category_url', ''),
                    'brand_name': brand_name}
            yield scrapy.Request(url=self.request_api.format(brand_name, '0'),
                                 meta={'item': item}, callback=self.parse)

    def parse(self, response):
        result = json.loads(response.body)
        item = response.meta['item']
        current_page = result.get('currentPage', '')
        rank = 1
        for data in result.get('products', ''):
            pro_url = data.get('relativeProductUrl', '')
            if not pro_url.startswith(self.base_url):
                pro_url = self.base_url.format(pro_url)
            yield {
                'product_id': data.get('gtin', ''),
                'product_name': data.get('name', ''),
                'brand_name': data.get('brandName', ''),
                'net_weight': data.get('netQuantityContent', ''),
                'price': data.get('priceLocalized', ''),
                'base_weight': data.get('basePriceRelNetQuantity', ''),
                'base_price': data.get('basePriceLocalized', ''),
                'fragments': ', '.join(frag for frag in data.get('subheadlineFragments', '')),
                'average_rating': data.get('ratingValue', ''),
                'total_ratings': data.get('ratingCount', ''),
                'image_url': ', '.join(img for img in data.get('imageUrlTemplates', '')),
                'product_url': pro_url,
                'category_url': item.get('cate_url', ''),
                'product_rank': rank,
                'page_no': current_page + 1,
                'TimeStamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            rank += 1
        total_pages = result.get('totalPages', '')
        if current_page < total_pages:
            yield scrapy.Request(url=self.request_api.format(item.get('brand_name', ''), current_page + 1),
                                 meta={'item': item}, callback=self.parse)

