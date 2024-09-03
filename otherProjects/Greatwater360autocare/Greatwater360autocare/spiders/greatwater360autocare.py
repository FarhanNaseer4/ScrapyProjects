import csv
import json
from datetime import datetime

import pgeocode
import scrapy


class Greatwater360autocareSpider(scrapy.Spider):
    name = 'greatwater360autocare'
    request_api = 'https://greatwater360autocare.com/wp-admin/admin-ajax.php?action=store_search&lat={}&lng={}&max_results=75&search_radius=50'
    custom_settings = {
        'FEED_URI': 'greatwater360autocare.csv',
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

    headers = {
        'authority': 'greatwater360autocare.com',
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cookie': '_gcl_au=1.1.1372144152.1671528348; _ga=GA1.1.1000194877.1671528348; _ga_8X7K8K727D=GS1.1.1671528347.1.1.1671528358.0.0.0',
        'referer': 'https://greatwater360autocare.com/find-a-shop-great-water-360-auto-care/',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_zip = self.get_search_zip()

    def get_search_zip(self):
        with open('33_states_cities_zip.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_zip:
            zip_code = data.get('zipcode', '')
            nomi = pgeocode.Nominatim('us')
            location = nomi.query_postal_code(codes=zip_code)
            lati = location.latitude
            longi = location.longitude
            yield scrapy.Request(url=self.request_api.format(lati, longi), callback=self.parse, headers=self.headers)

    def parse(self, response):
        json_data = json.loads(response.body)
        for data in json_data:
            item = dict()
            item['Business Name'] = data.get('store', '')
            item['Phone Number'] = data.get('phone', '')
            item['Street Address'] = data.get('address', '')
            item['State'] = data.get('state', '')
            item['Zip'] = data.get('zip', '')
            item['Email'] = data.get('email', '')
            item['Latitude'] = data.get('lat', '')
            item['Longitude'] = data.get('lng', '')
            detail_url = data.get('permalink', '')
            if detail_url:
                item['Detail_Url'] = detail_url
            item['Source_URL'] = 'https://greatwater360autocare.com/find-a-shop-great-water-360-auto-care/'
            item['Lead_Source'] = 'greatwater360autocare'
            item['Occupation'] = 'Auto Care Store'
            item['Record_Type'] = 'Business'
            item['Meta_Description'] = "Find a GreatWater 360 Auto Care location near you. Search by city, state or " \
                                       "zip code to find a garage near you."
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield item
