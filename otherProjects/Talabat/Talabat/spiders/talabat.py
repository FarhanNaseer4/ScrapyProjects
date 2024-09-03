# -*- coding: utf-8 -*-
import json

import scrapy


class TalabatSpider(scrapy.Spider):
    name = 'talabat'
    request_api = 'https://www.talabat.com/_next/data/b1a260ec-0d6c-4458-828e-26a7ced55d66/all-restaurants.json' \
                  '?countrySlug=uae&page= '
    detail_url = 'https://www.talabat.com/uae/restaurant/{}/{}?aid=1272'
    headers = {
        'authority': 'www.talabat.com',
        'accept': 'application/json',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/109.0.0.0 Safari/537.36',
    }
    detail_headers = {
        'authority': 'www.talabat.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/109.0.0.0 Safari/537.36',
    }
    zyte_key = 'ce28cc8d68484a0ba9168cc550cd77f8'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'talabat.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'HTTPERROR_ALLOW_ALL': True,
    }

    def start_requests(self):
        yield scrapy.Request(url=self.request_api, callback=self.parse, headers=self.headers)

    def parse(self, response):
        json_data = json.loads(response.body)
        restaurant_details = json_data.get('pageProps', {}).get('restaurants', [])
        for data in restaurant_details:
            res_id = data.get('id', '')
            slug = data.get('slug', '')
            yield scrapy.Request(url=self.detail_url.format(res_id, slug), callback=self.parse_details,
                                 headers=self.detail_headers)

    def parse_details(self, response):
        detail_json = response.css('script[id="__NEXT_DATA__"]::text').get()
        if detail_json:
            res_json = json.loads(detail_json)
            props = res_json.get('props', {}).get('pageProps', {}).get('initialMenuState', {})
            for cate_data in props.get('menuData', {}).get('items', []):
                item = dict()
                res_detail = props.get('restaurant', {})
                item['Name'] = res_detail.get('name', '')
                item['Restaurant ID'] = res_detail.get('id', '')
                item['Category'] = res_detail.get('cuisineString', '')
                item['Location'] = res_detail.get('areaName', '')
                item['Latitude'] = res_detail.get('latitude', '')
                item['Longitude'] = res_detail.get('longitude', '')
                item['Min Order'] = res_detail.get('minimumOrderAmount', '')
                item['Logo Url'] = res_detail.get('logo', '')
                item['Rating'] = res_detail.get('rate', '')
                item['Reviews'] = res_detail.get('totalReviews', '')
                item['Delivery Fee'] = res_detail.get('deliveryFee', '')
                item['Menu Dish Name'] = cate_data.get('name', '')
                item['Menu Dish Description'] = cate_data.get('description', '')
                item['Menu Dish Price'] = cate_data.get('price', '')
                item['Menu Category'] = cate_data.get('originalSection', '')
                item['Menu Image Url'] = cate_data.get('image', '')
                item['URL'] = response.url
                yield item

