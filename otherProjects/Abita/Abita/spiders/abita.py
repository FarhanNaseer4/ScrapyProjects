import csv
import json
from datetime import datetime

import pgeocode
import scrapy


class AbitaSpider(scrapy.Spider):
    name = 'abita'
    request_api = 'https://abita.com/finder/find-abita.php?beer=&storeType%5B%5D=bar&storeType%5B%5D=restaurant&storeType%5B%5D=store&storeType%5B%5D=event&storeType%5B%5D=other&location={}&lat={}&long={}&distance=50'
    custom_settings = {
        'FEED_URI': 'abita.csv',
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
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        # 'Cookie': 'exp_last_visit=1354863189; exp_csrf_token=a26248fdd69a6ac073e0ed8703c03450f985c5cb; exp_cartthrob_session_id=c44a538753a44f9adc6e022e211b3c88; _gcl_au=1.1.614700093.1670223198; over21=true; exp_tracker=%7B%220%22%3A%22find-abita%22%2C%221%22%3A%22about%2Fnews%2F6-best-things-to-do-in-abita-springs%22%2C%22token%22%3A%2250cacc76def3519ae73b854987fea2f0%22%7D; _gid=GA1.2.556845110.1670223211; _gat=1; notification-the-tap-room-is-closed-this-week=hide; exp_last_activity=1670223243; _ga_8TM9GP52TH=GS1.1.1670223198.1.1.1670223247.0.0.0; _ga=GA1.2.2090373819.1670223198; _tq_id.TV-8127815445-1.e636=9a64b82d29ae3e2f.1670223199.0.1670223248..',
        'Referer': 'https://abita.com/find-abita?location=36104',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/108.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
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
            nomi = pgeocode.Nominatim('us')
            location = nomi.query_postal_code(codes=zip_code)
            lati = location.latitude
            longi = location.longitude
            yield scrapy.Request(url=self.request_api.format(zip_code, lati, longi),
                                 callback=self.parse, headers=self.headers)

    def parse(self, response):
        try:
            json_result = json.loads(response.body)
            for data in json_result.get('places', []):
                item = dict()
                item['Business Name'] = data.get('name', '')
                item['Street Address'] = data.get('address_street', '')
                item['State'] = data.get('address_state', '')
                item['Zip'] = data.get('address_zip', '')
                item['Phone Number'] = data.get('phone', '')
                item['Source_URL'] = 'https://abita.com/find-abita?location='
                item['Lead_Source'] = 'abita'
                item['Occupation'] = 'Abita Bar and Store'
                item['Record_Type'] = 'Business'
                item['Meta_Description'] = "Find Abita - Abita Beer"
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item
        except Exception as ex:
            print('Error In Parse | ' + str(ex))
