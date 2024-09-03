import json
import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class VelawSpider(scrapy.Spider):
    name = 'velaw'
    request_api = 'https://www.velaw.com/api/people?page={}'
    base_url = 'https://www.velaw.com{}'
    headers = {
        'authority': 'www.velaw.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'referer': 'https://www.velaw.com/people/search/?page=1',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/108.0.0.0 Safari/537.36',
    }

    custom_settings = {
        'FEED_URI': 'velaw.csv',
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

    def start_requests(self):
        yield scrapy.Request(url=self.request_api.format(1), callback=self.parse,
                             headers=self.headers)

    def parse(self, response):
        try:
            json_data = json.loads(response.body)
            for data in json_data.get('results', []):
                item = dict()
                fullname = self.get_name_parts(data.get('name', ''))
                item['Full Name'] = fullname.get('full_name', '')
                item['First Name'] = fullname.get('first_name', '')
                item['Last Name'] = fullname.get('last_name', '')
                item['Email'] = data.get('email', '')
                detail_url = data.get('url', '')
                if detail_url:
                    item['Detail_Url'] = self.base_url.format(detail_url)
                item['Business Name'] = data.get('title', '')
                office = data.get('offices', [])
                if office:
                    item['Phone Number'] = office[0].get('phone', '')
                    item['Street Address'] = office[0].get('title', '')
                item['Source_URL'] = 'https://www.velaw.com/people/search/'
                item['Occupation'] = "Lawyer"
                item['Lead_Source'] = 'velaw'
                item['Detail_Url'] = response.url
                item['Record_Type'] = 'Person'
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                item['Meta_Description'] = "Individually Talented, Collectively Powerful"
                yield item
            current_p = json_data.get('page', '')
            next_p = int(current_p) + 1
            total_p = json_data.get('totalPages', '')
            if next_p <= total_p:
                yield scrapy.Request(url=self.request_api.format(next_p), callback=self.parse,
                                     headers=self.headers)

        except Exception as ex:
            print('Error in Parser | ' + str(ex))

    def get_name_parts(self, name):
        name_parts = HumanName(name)
        punctuation_re = re.compile(r'[^\w-]')
        return {
            'full_name': name.strip(),
            'prefix': re.sub(punctuation_re, '', name_parts.title),
            'first_name': re.sub(punctuation_re, '', name_parts.first),
            'middle_name': re.sub(punctuation_re, '', name_parts.middle),
            'last_name': re.sub(punctuation_re, '', name_parts.last),
            'suffix': re.sub(punctuation_re, '', name_parts.suffix)
        }
