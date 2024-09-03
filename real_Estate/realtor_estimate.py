import csv
import json
from datetime import date

import scrapy


class RealtorEstimateSpider(scrapy.Spider):
    name = 'realtor_estimate'
    url = "https://parser-external.geo.moveaws.com/search?input={" \
          "}&limit=1&client_id=rdc-home&area_types=city%2Ccounty%2Cpostal_code%2Caddress%2Cstreet%2Cneighborhood" \
          "%2Cschool%2Cschool_district%2Cuniversity%2Cpark&lat=-1&long=-1"
    detail_api = "https://sellers-marketplace-api.rdc.moveaws.com/spot-offer-estimate?propertyId={" \
                 "}&source=pdp_spot_offer "
    custom_settings = {
        'FEED_URI': f'output/realtor_estimate_v1.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }
    payload = {}
    headers = {
        'authority': 'parser-external.geo.moveaws.com',
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        'origin': 'https://www.realtor.com',
        'pragma': 'no-cache',
        'referer': 'https://www.realtor.com/',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    address = '{} {},{} {}'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_keyword = self.get_search_keyword()

    def get_search_keyword(self):
        with open('output/zillow_estimate_v1.csv', 'r', encoding='utf-8') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_keyword:
            street = data.get('PROP_ADDRESS', '').strip()
            city = data.get('PROP_CITY', '').strip()
            state = data.get('PROP_STATE', '').strip()
            zips = data.get('PROP_ZIP', '').strip()
            full = data.get('Full Property Address', '').strip()
            # address = self.address.format(street, city, state, zips)
            yield scrapy.Request(url=self.url.format(full), headers=self.headers, meta={'item': data})

    def parse(self, response):
        item = response.meta['item']
        json_data = json.loads(response.text)
        hits = json_data.get('hits', [])
        if any(hits):
            mp_id = hits[0].get('mpr_id', '')
            yield scrapy.Request(url=self.detail_api.format(mp_id), callback=self.parse_details,
                                 headers=self.headers, meta={'item': item})
        else:
            yield item

    def parse_details(self, response):
        item = response.meta['item']
        json_data = json.loads(response.text)
        # print(json_data)
        price = json_data.get('homeValue', '')
        item['Realtor'] = price
        yield item
