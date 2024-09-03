import copy
import csv
import json
import re
from datetime import datetime
from time import strftime, localtime

import scrapy


class WalmartDetailPageSpider(scrapy.Spider):
    name = 'walmart_detail_page'
    allowed_domains = ['walmart.ca']
    today = f'output/detailpageScrapper {strftime("%Y-%m-%d %H-%M-%S", localtime())}.csv'
    custom_settings = {
        'FEED_URI': today,
        'FEED_FORMAT': 'csv',
    }
    start_urls = ['http://walmart.ca/']
    data_api = 'https://www.walmart.ca/api/product-page/carousel/rr?carouselName=RichRelevanceCarousel'
    payload = {
        "sessionId": "f5871b0a-f81c-469f-86d7-a59779e7e33d",
        "platform": "desktop",
        "placement": "item_page.fbt|item_page.rr1|item_page.rr2|item_page.rr3",
        "fsa": "L5V",
        "availabilityStoreId": "1061",
        "lang": "en",
        "pricingStoreId": "1061",
        "fulfillmentStoreId": "1061",
        "experience": "whiteGM",
        "categoryId": "6000188228193",
        "productId": "6000192174725"
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
        self.search_url = self.get_search_urls()

    def get_search_urls(self):
        # this function reads the text file to get start and end date range for calendar
        with open('output/WalmartCanada.csv', 'r', encoding='utf-8-sig') as reader:
            dictobj = csv.DictReader(reader)
            product_id = []
            for data in dictobj:
                product_id.append(data['product_url'])
            return product_id

    def start_requests(self):
        for data in self.search_url:
            yield scrapy.Request(url=data, callback=self.get_detail_api, headers=self.headers)

    def get_detail_api(self, response):
        detail_api = json.loads(response.css('script').re_first('window.__PRELOADED_STATE__=(.+);'))
        sku_id = detail_api.get('product', {}).get('activeSkuId', '')
        rating = detail_api.get('product', {}).get('item', {}).get('rating', {})
        pro_data = detail_api.get('entities', {}).get('skus', {}).get(sku_id, {})
        image = pro_data.get('images', [])
        pro_name = pro_data.get('name', '')
        img_url = [''.join(data.get('large', '').get('url', '')) for data in image]
        item = {
            'Sku_id': sku_id,
            'Product_Name': pro_name,
            'Product_Code': pro_data.get('modelNumber', ''),
            'Short_Description': pro_data.get('description', ''),
            'Total_Reviews': rating.get('totalCount', ''),
            'Average_Rating': rating.get('averageRating', ''),
            'Image_url': img_url,
            'Product_Url': response.url,
            'Nutritional_Info': pro_data.get('nutritionalInfo', []),
            'Long_Description': pro_data.get('longDescription', ''),
            'Specification': pro_data.get('facets', []),
            'Product_Name_url': {pro_name: x for x in img_url},
            'TimeStamp': datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        }
        payload = copy.deepcopy(self.payload)
        payload['productId'] = response.url.split('/')[-1]
        yield scrapy.Request(url=self.data_api, method='POST', meta={'Item': item},
                             callback=self.sponsored_data, body=json.dumps(payload),
                             headers=self.headers)

    def sponsored_data(self, response):
        result = json.loads(response.body)
        item = response.meta['Item']
        sponsor_pro = result.get('item_page.rr1', {}).get('products', [])
        item['Sponsored_Product'] = [''.join(data.get('name', '')) for data in sponsor_pro]
        yield item
