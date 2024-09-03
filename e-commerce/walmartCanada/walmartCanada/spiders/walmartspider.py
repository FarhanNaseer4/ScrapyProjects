import copy
import csv
import json
from datetime import datetime
from time import strftime, localtime

import scrapy


class WalmartspiderSpider(scrapy.Spider):
    name = 'walmartspider'
    allowed_domains = ['walmart.ca']
    start_urls = ['http://walmart.ca/']
    today = f'output/Walmart Canada {strftime("%Y-%m-%d %H-%M-%S", localtime())}.csv'
    custom_settings = {
        'FEED_URI': today,
        'FEED_FORMAT': 'csv',
    }
    request_url = 'https://www.walmart.ca/api/bsp/search?experience=whiteGM&q={}&lang=en&p={}&c=all'
    req_api = 'https://www.walmart.ca/api/bsp/fetch-products'
    price_api = 'https://www.walmart.ca/api/bsp/v2/price-offer'
    base_url = 'https://www.walmart.ca/en/ip/'
    payload = {
        "products": [
        ],
        "lang": "en"
    }

    headers = {
        'authority': 'www.walmart.ca',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }

    def __init__(self, **kwargs):
        # this function initialize the variables
        super().__init__(**kwargs)
        self.search_data = self.get_search_keys()

    def get_search_keys(self):
        # this function reads the text file to get start and end date range for calendar
        with open('input/search_input.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.search_data:
            yield scrapy.Request(url=self.request_url.format(data.get('keyword', ''), '1'),
                                 meta={'item_search': data},
                                 callback=self.parse,
                                 headers=self.headers)

    def parse(self, response):
        result = json.loads(response.body)
        product_to_fetch = result.get('items', {}).get('productsToFetch', [])
        pro_data = result.get('items', {}).get('products', {})
        item_key = response.meta['item_search']
        search_keyword = item_key.get('keyword', '')
        current_page = result.get('pagination', {}).get('pageNumber', '')
        item_key['current_page'] = current_page
        pro_ids = result.get('items', {}).get('productIds', [])
        i = 1
        predata = {}
        product_to_price = []
        product_datas = {}
        for data in pro_ids:
            item = {}
            get_id = pro_data[data].get('skus', {})
            sku_id = pro_data[data].get('skuIds', [])
            if data in pro_data.keys():
                rating = pro_data[data].get('rating', {})
                get_dict = {}
                pro_code = pro_data[data].get('id', '')
                get_dict['productId'] = pro_code
                get_dict['skuIds'] = sku_id
                images = [get_id[key].get('images', '') for key in get_id.keys()]
                product_to_price.append(get_dict)
                item = {
                    'Sku_ID': sku_id[0] if sku_id[0] else '',
                    'pro_code': pro_data[data].get('id', ''),
                    'pro_name': pro_data[data].get('name', ''),
                    'total_rating_count': rating.get('totalCount', '') if rating is not None else '',
                    'average_rating': rating.get('averageRating', '') if rating is not None else '',
                    'img_url': ','.join(
                        [img[0].get('thumbnail', {}).get('url', '') for img in images] if images else ''),
                    'short_description': pro_data[data].get('description', ''),
                    'search_keyword': search_keyword,
                    'product_url': self.base_url + pro_code,
                    'Product_Rank': i,
                    'Page_No': item_key['current_page'],
                    'TimeStamp': datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                }
            i += 1
            product_datas[sku_id[0]] = item
        payload = copy.deepcopy(self.payload)
        item_key['rank'] = i - 1
        predata['ids'] = product_to_price
        payload['products'] = product_to_fetch
        yield scrapy.Request(url=self.req_api, method='POST',
                             callback=self.getproducts_data,
                             meta={'search_key': item_key, 'item': product_datas, 'more_data': predata},
                             body=json.dumps(payload), headers=self.headers)
        if item_key['Pagination'] == 'True':
            if current_page < 25:
                next_page = current_page + 1
                yield scrapy.Request(url=self.request_url.format(item_key.get('keyword', ''), next_page),
                                     meta={'item_search': item_key}, callback=self.parse,
                                     headers=self.headers)

    def getproducts_data(self, response):
        result = json.loads(response.body)
        pro_data = result.get('products', {})
        pro_ids = result.get('productIds', [])
        item_key = response.meta['search_key']
        search_keyword = item_key.get('keyword', '')
        product_datas = response.meta['item']
        pre_ids = response.meta['more_data']
        i = item_key.get('rank', 1)
        product_to_fetch = pre_ids.get('ids', '')
        for data in pro_ids:
            get_id = pro_data[data].get('skus', {})
            rating = pro_data[data].get('rating', {})
            pay_dict = {}
            sku_id = pro_data[data].get('skuIds', [])
            item = {}
            if data in pro_data.keys():
                pro_code = pro_data[data].get('id', '')
                pay_dict['productId'] = pro_code
                pay_dict['skuIds'] = sku_id
                product_to_fetch.append(pay_dict)

                images = [get_id[key].get('images', '') for key in get_id.keys()]
                item = {
                    'Sku_ID': sku_id[0] if sku_id[0] else '',
                    'pro_code': pro_data[data].get('id', ''),
                    'pro_name': pro_data[data].get('name', ''),
                    'total_rating_count': rating.get('totalCount', '') if rating is not None else '',
                    'average_rating': rating.get('averageRating', '') if rating is not None else '',
                    'img_url': ','.join(
                        [img[0].get('thumbnail', {}).get('url', '') for img in images] if images else ''),
                    'short_description': pro_data[data].get('description', ''),
                    'search_keyword': search_keyword,
                    'product_url': self.base_url + pro_code,
                    'Product_Rank': i,
                    'Page_No': item_key['current_page'],
                    'TimeStamp': datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                }
            i += 1
            product_datas[sku_id[0]] = item
        payload = copy.deepcopy(self.payload)
        payload['fsa'] = 'L5V'
        payload['products'] = product_to_fetch
        payload['experience'] = "whiteGM"
        payload['pricingStoreId'] = "1061"
        payload['fulfillmentStoreId'] = "1061"
        yield scrapy.Request(url=self.price_api, method='POST',
                             callback=self.getproduct_price, meta={'item': product_datas}, body=json.dumps(payload),
                             headers=self.headers)

    def getproduct_price(self, response):
        result = json.loads(response.body)
        item = response.meta['item']
        sku = result.get('skus', {})
        pro_data = result.get('offers', {})
        for data in item:
            if data in sku.keys():
                new_id = sku[data][0] if sku[data] else []
                item[data]['Price'] = pro_data[new_id].get('currentPrice', '') if sku[data] else ''
                item[data]['Price_per_unit'] = pro_data[new_id].get('pricePerUnit', '') if sku[data] else ''
                yield item[data]
