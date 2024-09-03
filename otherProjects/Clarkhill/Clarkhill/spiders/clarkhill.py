import json
import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class ClarkhillSpider(scrapy.Spider):
    name = 'clarkhill'
    first_name = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    request_api = 'https://www.clarkhill.com/api/people?page={}&letter={}'
    base_url = 'https://www.clarkhill.com{}'
    headers = {
        'authority': 'www.clarkhill.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        # 'cookie': 'BE_CLA3=p_id%3D42N28R2RJAL4RJLL8J64N8468AAAAAAAAH%26bf%3D00105030a98cbd0a5ebd1788e7294fae%26bn%3D1%26bv%3D3.44%26s_expire%3D1671004528587%26s_id%3DL2N28R2RJAL4R8248664N8468AAAAAAAAH; OptanonAlertBoxClosed=2022-12-13T07:55:32.709Z; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Dec+13+2022+12%3A55%3A32+GMT%2B0500+(Pakistan+Standard+Time)&version=6.17.0&isIABGlobal=false&hosts=&consentId=20eb0e9a-bf4c-42c3-82de-9e2f97d37230&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0002%3A1%2CC0001%3A1%2CC0003%3A1; _ga=GA1.2.609127897.1670918133; _gid=GA1.2.1689499707.1670918133; _gat_UA-39219170-5=1',
        'referer': 'https://www.clarkhill.com/people/?page=1&letter=C',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    }
    custom_settings = {
        'FEED_URI': 'clarkhill.csv',
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
        for first in self.first_name:
            letter = {'letter': first}
            yield scrapy.Request(url=self.request_api.format(1, first), callback=self.parse,
                                 headers=self.headers, meta={'letter': letter})

    def parse(self, response):
        try:
            letter_slug = response.meta['letter']
            json_data = json.loads(response.body)
            for data in json_data.get('results', []):
                item = dict()
                fullname = self.get_name_parts(data.get('name', ''))
                item['Full Name'] = fullname.get('full_name', '')
                item['First Name'] = fullname.get('first_name', '')
                item['Last Name'] = fullname.get('last_name', '')
                item['Phone Number'] = data.get('phone', '')
                item['Street Address'] = data.get('offices', '')
                item['Email'] = data.get('email', '')
                url = data.get('url', '')
                if url:
                    item['Detail_Url'] = self.base_url.format(url)
                item['Occupation'] = data.get('position', '')
                item['Source_URL'] = 'https://www.clarkhill.com/people'
                item['Lead_Source'] = 'clarkhill'
                item['Record_Type'] = 'Person'
                item['Meta_Description'] = "Clients count on more from our team members who leverage comprehensive " \
                                           "industry and policy knowledge to provide innovative solutions and " \
                                           "outstanding service worldwide."
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item

            current_page = json_data.get('page', '')
            total_page = json_data.get('totalPages', '')
            next_page = int(current_page) + 1
            if next_page <= int(total_page):
                yield scrapy.Request(url=self.request_api.format(next_page, letter_slug.get('letter', '')), callback=self.parse,
                                     headers=self.headers, meta={'letter': letter_slug})
        except Exception as ex:
            print('Error In Parse | ' + str(ex))

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
