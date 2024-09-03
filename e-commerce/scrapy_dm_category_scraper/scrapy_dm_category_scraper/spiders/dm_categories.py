import json

import scrapy


class DmCategoriesSpider(scrapy.Spider):
    name = 'dm_categories'
    request_api = 'https://content.services.dmtech.com/rootpage-dm-shop-de-de/marken?json'
    base_url = 'https://www.dm.de{}'
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'dm_categories.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }

    def start_requests(self):
        yield scrapy.Request(url=self.request_api, callback=self.parse)

    def parse(self, response):
        result = json.loads(response.body)
        main_data = result.get('mainData', [])
        for data in main_data[3].get('data', {}).get('children', []):
            module = data.get('modules', [])
            if len(module) > 1:
                detail_data = module[3].get('data', '').get('text', {}).get('childNodes', [])[0]
            else:
                detail_data = module[0].get('data', '').get('text', {}).get('childNodes', [])[0]
            for cate in detail_data.get('childNodes', []):
                list_data = cate.get('childNodes', [])[0]
                if 'childNodes' in list_data:
                    cate_url = list_data.get('href', '')
                    if not cate_url.startswith(self.base_url):
                        cate_url = self.base_url.format(cate_url)
                    yield {
                        'category_name': list_data.get('childNodes', [])[0],
                        'category_url': cate_url
                    }
                else:
                    cate_url = cate.get('href', '')
                    if cate:
                        if not cate_url.startswith(self.base_url):
                            cate_url = self.base_url.format(cate_url)
                        yield {
                            'category_name': cate.get('childNodes', [])[0],
                            'category_url': cate_url

                        }
