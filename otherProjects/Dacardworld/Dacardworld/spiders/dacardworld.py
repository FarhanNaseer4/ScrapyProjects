# -*- coding: utf-8 -*-
import copy
import json
from math import ceil

import scrapy


class DacardworldSpider(scrapy.Spider):
    name = 'dacardworld'
    request_api = 'https://typesense.dacardworld.com/shopping/0/multi_search?x-typesense-api-key=QlMpn1N3bx'
    custom_settings = {
        'FEED_URI': 'dacardworld.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORT_FIELDS': ['id', 'sku', 'Name', 'Product Highlights', 'company', 'category', 'sub_category', 'package',
                               'Series', 'Barcode', 'Release Date', 'Price', 'presell', 'year', 'Key Rookies',
                               'Item Description', 'Main Image', 'Images', 'Detail_Url']
    }
    headers = {
        'authority': 'typesense.dacardworld.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'content-type': 'text/plain',
        'origin': 'https://www.dacardworld.com',
        'referer': 'https://www.dacardworld.com/',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    }
    data = {"searches": [
        {"query_by": "title,barcode,artist,player,rookie", "sort_by": "featured:desc,_text_match:desc,year_sort:desc",
         "per_page": 100, "highlight_full_fields": "title,barcode,artist,player,rookie", "collection": "items",
         "q": "*", "facet_by": "", "page": 1}]}

    def start_requests(self):
        yield scrapy.Request(url=self.request_api, callback=self.parse, headers=self.headers, method='POST',
                             body=json.dumps(self.data))

    def parse(self, response):
        json_data = json.loads(response.body)
        pro_data = json_data.get('results', [])
        if any(pro_data):
            results = pro_data[0].get('hits', [])
            for data in results:
                item = dict()
                item['Name'] = data.get('document', {}).get('title', '')
                item['Barcode'] = data.get('document', {}).get('barcode', '')
                item['Price'] = data.get('document', {}).get('price', '')
                item['Main Image'] = data.get('document', {}).get('image', '')
                img1 = data.get('document', {}).get('image_170', 'image_340')
                img2 = data.get('document', {}).get('image_340', '')
                item['Images'] = img1 + ' | ' + img2
                url = data.get('document', {}).get('url', '')
                item['Detail_Url'] = url
                item['category'] = data.get('document', {}).get('category', '')
                item['company'] = data.get('document', {}).get('company', '')
                item['id'] = data.get('document', {}).get('id', '')
                item['package'] = data.get('document', {}).get('package', '')
                item['presell'] = data.get('document', {}).get('presell', '')
                item['sku'] = data.get('document', {}).get('sku', '')
                item['sub_category'] = data.get('document', {}).get('sub_category', '')
                item['year'] = data.get('document', {}).get('year', '')
                rookies = data.get('document', {}).get('rookie', [])
                if any(rookies):
                    item['Key Rookies'] = ' | '.join(rookie for rookie in rookies)
                else:
                    item['Key Rookies'] = ''
                item['Series'] = data.get('document', {}).get('series', '')
                if url:
                    yield response.follow(url=url, callback=self.parse_detail, headers=self.headers,
                                          meta={'item': item})
            total_records = pro_data[0].get('found', '')
            total_pages = ceil(int(total_records)/100)
            current_page = pro_data[0].get('page', '')
            next_page = int(current_page) + 1
            if next_page < total_pages:
                payload = copy.deepcopy(self.data)
                payload['searches'][0]['page'] = next_page
                yield scrapy.Request(url=self.request_api, callback=self.parse, headers=self.headers, method='POST',
                                     body=json.dumps(payload))

    def parse_detail(self, response):
        item = response.meta['item']
        item['Item Description'] = ' '.join(response.css('li#moredetailsTab p::text').getall())
        item['Product Highlights'] = response.xpath(
            '//li[@id="itemdetailsTab"]//li[contains(text(), "Product")]/text()').get('').replace('Product:',
                                                                                                  '').strip()
        item['Release Date'] = response.xpath(
            '//li[@id="itemdetailsTab"]//li[contains(text(), "Release Date")]/text()').get('').replace('Release Date:',
                                                                                                       '').strip()
        yield item
