import csv
import json
from datetime import datetime

import pgeocode
import scrapy


class GeniusvetsSpider(scrapy.Spider):
    name = 'geniusvets'
    request_api = 'https://emaps.geniusvets.xyz/api/v1/maps/listings/box?lon={}&lat={}&dist=40km&limit=500&offset=0'
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Access-Control-Allow-Headers': 'application/json',
        'Access-Control-Allow-Methods': 'GET',
        'Access-Control-Allow-Origin': '*',
        'Connection': 'keep-alive',
        'Digest': 'md5-793f4ae70f5683193bff8089aa0b22cf',
        'Origin': 'https://www.geniusvets.com',
        'Referer': 'https://www.geniusvets.com/pet-care/find/alabama/birmingham/veterinarians',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    base_url = 'https://www.geniusvets.com{}'
    custom_settings = {
        'FEED_URI': 'geniusvets.csv',
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


    def parse(self, response):
        json_data = json.loads(response.body)
        for data in json_data.get('data', {}).get('listings', []):
            item = dict()
            item['Business Name'] = data.get('title', '')
            item['Street Address'] = data.get('address_line1', '')
            item['State'] = data.get('area', '')
            item['Zip'] = data.get('postal_code', '')
            item['Phone Number'] = data.get('phone', '')
            item['Latitude'] = data.get('lat', '')
            item['Longitude'] = data.get('lon', '')
            url = data.get('url', '')
            if url:
                item['Detail_Url'] = self.base_url.format(url)
            item['Source_URL'] = 'https://www.geniusvets.com/pet-care'
            item['Lead_Source'] = 'geniusvets'
            item['Meta_Description'] = ""
            item['Record_Type'] = 'Business'
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            item['Occupation'] = 'Veterinarian Hospital'
            yield item



