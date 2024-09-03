import csv
import json
from datetime import datetime

import scrapy


class TargetSpider(scrapy.Spider):
    name = 'target'
    request_api = 'https://redsky.target.com/redsky_aggregations/v1/web_platform/nearby_stores_v1?limit=20&within=100&place={}&key=8df66ea1e1fc070a6ea99e942431c9cd67a80f02&channel=WEB&page=%2Fs%2F'
    headers = {
        'authority': 'redsky.target.com',
        'accept': 'application/json',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cookie': 'TealeafAkaSid=Z7itdCjcAWLePNHkrR0hnn1vOLVM8ZvU; visitorId=0185200C983B0201901D9F4DBA09E89D; sapphire=1; UserLocation=50700|32.590|74.080|PB|PK; _mitata=MjI4ODZkYjljZTNjYmRhOWIyMzRlZTE4Y2ZkZGU5ZTU4YWVkNzEwMmRkY2U1MWQzNDNmZjc1YmZhYTM3NzY4NA==_/@#/1671432675_/@#/cuOofJU4mr7wOFDI_/@#/NzQ4Zjg0NjAyYjRjZjY5MjEzYTBiMGZlYWU5YTg0MWMzMWZjMDhhZjhjOWIyMTcyNzA5YzJmZjRhNjA2M2JlOQ==_/@#/000; ffsession={%22sessionHash%22:%226e62ec3d7376e1671432619149%22}; _mitata=ZjYxNTdlODU2ZWVjOGI5NTVhMDgzYzk0ZjI5ZGJkMzRkNDUzZDcwZDdjZmQwZWQ3YTZmYjE4OGYyZmNkNzg2NQ==_/@#/1671432942_/@#/cuOofJU4mr7wOFDI_/@#/NTAzN2NkNmRiNzc1NGIxMGFmNTg3MTE4MzVmMTQ1NDY4OTFiNTZkNWE1NjFhZmIwZTY4M2E3ODFmOGMyMDI1NQ==_/@#/000',
        'origin': 'https://www.target.com',
        'referer': 'https://www.target.com/store-locator/find-stores/36104',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }
    custom_settings = {
        'FEED_URI': 'target.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORT_FIELDS': ['Business Name', 'Full Name', 'First Name', 'Last Name', 'Street Address',
                               'Valid To', 'State', 'Zip', 'Description', 'Phone Number', 'Phone Number 1', 'Email',
                               'Business_Site', 'Social_Media', 'Record_Type',
                               'Category', 'Rating', 'Reviews', 'Source_URL', 'Detail_Url', 'Services',
                               'Latitude', 'Longitude', 'Occupation',
                               'Business_Type', 'Lead_Source', 'State_Abrv', 'State_TZ', 'State_Type',
                               'SIC_Sectors', 'SIC_Categories',
                               'SIC_Industries', 'NAICS_Code', 'Quick_Occupation', 'Scraped_date', 'Meta_Description']
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_zip = self.get_search_zip()

    def get_search_zip(self):
        with open('state_zip.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_zip:
            zip_code = data.get('Zip_code', '')
            yield scrapy.Request(url=self.request_api.format(zip_code), callback=self.parse, headers=self.headers)

    def parse(self, response):
        json_data = json.loads(response.body)
        for data in json_data.get('data', {}).get('nearby_stores', {}).get('stores', []):
            item = dict()
            store_id = data.get('store_id', '')
            store_name = data.get('location_name', '')
            item['Business Name'] = 'Target Store ' + store_name + ' ' + store_id
            item['Phone Number'] = data.get('main_voice_phone_number', '')
            item['Street Address'] = data.get('mailing_address', {}).get('address_line1', '')
            item['State'] = data.get('mailing_address', {}).get('region', '')
            item['Zip'] = data.get('mailing_address', {}).get('postal_code', '')
            item['State_Abrv'] = data.get('mailing_address', {}).get('state', '')
            item['Source_URL'] = 'https://www.target.com/store-locator/find-stores'
            item['Lead_Source'] = 'target'
            item['Meta_Description'] = "Find a Target store near you quickly with the Target Store Locator. Store " \
                                       "hours, directions, addresses and phone numbers available for more than 1800 " \
                                       "Target store locations across the US."
            item['Occupation'] = 'Store'
            item['Record_Type'] = 'Business'
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield item

