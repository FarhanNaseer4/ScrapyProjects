import csv
import json
from datetime import datetime

import pgeocode
import scrapy


class FlooringyouwellSpider(scrapy.Spider):
    name = 'flooringyouwell'
    request_api = 'https://flooringyouwell.com/wp-admin/admin-ajax.php?action=store_search&lat={}&lng={}&max_results=100&search_radius=25'
    base_url = 'https://flooringyouwell.com/{}'
    headers = {
        'accept': 'application/json',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Cookie': 'PHPSESSID=ke0r22iekhgngu9n676c8iv8t7'
    }
    custom_settings = {
        'FEED_URI': 'flooringyouwell.csv',
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
            item['Email'] = data.get('email', '')
            item['Street Address'] = data.get('address', '')
            item['State'] = data.get('state', '')
            item['Zip'] = data.get('zip', '')
            detail_url = data.get('retailer_storeurl', '')
            if detail_url:
                item['Detail_Url'] = self.base_url.format(detail_url)
            item['Latitude'] = data.get('lat', '')
            item['Longitude'] = data.get('lng', '')
            item['Source_URL'] = 'https://flooringyouwell.com/find-sfn-retailer/'
            item['Lead_Source'] = 'flooringyouwell'
            item['Meta_Description'] = "Find Your Nearest SFN Retailer"
            item['Occupation'] = 'SFN Retailer'
            item['Record_Type'] = 'Business'
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if item['Business Name']:
                yield item
