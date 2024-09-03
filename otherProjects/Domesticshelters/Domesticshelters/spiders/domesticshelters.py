import csv
import json
from datetime import datetime

import pgeocode
import scrapy


class DomesticsheltersSpider(scrapy.Spider):
    name = 'domesticshelters'
    request_api = 'https://www.domesticshelters.org/search?q={}&latitude={}&longitude={}8&_=1670307531525'

    custom_settings = {
        'FEED_URI': 'domesticshelters.csv',
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
        'authority': 'www.domesticshelters.org',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cookie': '_gcl_au=1.1.301825490.1670228190; _ga=GA1.2.1368190258.1670228191; _gid=GA1.2.760652740.1670228191; _hjSessionUser_1770413=eyJpZCI6Ijk5ZThjZGQ2LWE4NTUtNTdhNC05Y2NlLTY0YmFiYzcwYWI4NyIsImNyZWF0ZWQiOjE2NzAyMjgxOTMxNjYsImV4aXN0aW5nIjp0cnVlfQ==; _hjIncludedInSessionSample=1; _hjSession_1770413=eyJpZCI6IjA0ZDkxMDg4LTc1MDQtNDY0MC1hZjczLTE4YWZhMWE0NmJmZCIsImNyZWF0ZWQiOjE2NzAzMDc1MTUzOTQsImluU2FtcGxlIjp0cnVlfQ==; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=0; __atuvc=1%7C49; __atuvs=638edeeb43f5fb9c000; _gat_gtag_UA_47346704_1=1; _domestic_shelters_session_store=OHY0VGxIQVFZcmJHb3QzTUpFOUE1S3lFMzZyd24rZEttalM1UG11TVM3c1ZET3p2dVhUc0JpZUVuY3Bqa2IwNDgyYW50NmZWMnhaeVpzU0E4WThyQ3F6ZnVPT2oxamE3TmJsemhIUUZPWklrTmduZHkvQStCRjVJNGtUdEdvZ0ptTGp4Ni9TZHBtSm8yYjZ1d1EvUWNqYWZha0laS2k0Z0J6NklsRzJYRERtZEM3Vzk5NkQvM0lnQkc3MDRVTUtaSlI3YTR6eU9NRVcxMkhXaHl0TWc3S1djVXY2UUVBRi9SNjlnQzRwTWUvS0RBM1F0cTVZQUx1NVBlNnpvMjh1QnpnelMzeWd5L01UMTR3UW55dFl0Q0tGaUwzaFRuWGxzQllDUDZnTEtHeXZNa08xVGpNck9qK0tHYUw1clNuVGFBR0phYkZoYi9NZzgzN25tditKdzBRPT0tLTc1eWR2VXQ0WUd4YWxla1U5cHkrQXc9PQ%3D%3D--b7d8f4c0710eeec496dade2e85ad9a7629fea63c; _domestic_shelters_session_store=NnZBbmtuaG02TmJtblNHcGxhS1lGOVkyMGhjdHUrUW5sYlVERU5yaDNDdU1oVHh1cjFibkVWTXRCT2dhOURyQ2F4Mi9YNVBETGg1eEZLNVp6SW5jb0cxYUNLY2tUU0UzdnoyWmFKZzZFY0RnWXh3b2xDc1orcE92ZHNXbWJGeHlOYmZrWkw1V2drcmNZZy9XbUdydFR3dXlJMnQ0VXFyU25MbUcrVitpMUhSVHN3aVdxbUxkQTl2Wi8vN3Eyb3JzOFBUT3QzMS9Va3dpV1NTaW0zYnVCWjR5eW4vMFk2dTFZcTh5Q3BzZG1TRm9OV0JpKzRSVjlCYmpyMkFtcWZyOFI1ODI2MWlyMjJrbDJlUmRRY2wyR3Y5NGJQNzF6a1pXaE9neTBNRzA0QnplWHZ1cFFRMS9FM3lJSTBKbzB4UWZUUm9SMzhkdy9BdC9NTUFxamFBc21BPT0tLVd0UURNNWlqR0d2UDZQTUhHTHdEcnc9PQ%3D%3D--72d8916f289294e329907badf35f629a16be196a',
        'referer': 'https://www.domesticshelters.org/search',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'x-csrf-token': 'vx1sxZ+y4xrsrxNuxLbIB0q9JffTawOQeciLEgnt9D2oB8rgIjxsuKAd8LUqp56Sfs7/Jad2b/ukFy1VC5m+3A==',
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
            yield scrapy.Request(url=self.request_api.format(zip_code, lati, longi),
                                 callback=self.parse, headers=self.headers)

    def parse(self, response):
        try:
            json_data = json.loads(response.body)
            for data in json_data:
                item = dict()
                item['Business Name'] = data.get('title', '')
                item['Zip'] = data.get('zipcode', '')
                item['Phone Number'] = data.get('phone_number', '')
                item['Detail_Url'] = data.get('wishlist_permalink', '')
                item['State'] = data.get('state', {}).get('state_abbr', '')
                item['State_Abrv'] = data.get('state', {}).get('state_name', '')
                item['Street Address'] = data.get('city', {}).get('name', '')
                item['Latitude'] = data.get('city', {}).get('latitude', '')
                item['Longitude'] = data.get('city', {}).get('longitude', '')
                item['Source_URL'] = 'https://www.domesticshelters.org/'
                item['Occupation'] = 'Domestic Shelter'
                item['Lead_Source'] = 'domesticshelters'
                item['Meta_Description'] = "Make finding domestic violence help easier. Info on 3,000 shelters, " \
                                           "agencies, hotlines. Physical abuse, emotional abuse, psychological abuse " \
                                           "or verbal abuse, this free service can help."
                item['Record_Type'] = 'Business'
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item
        except Exception as ex:
            print('Error in Json | ' + str(ex))

