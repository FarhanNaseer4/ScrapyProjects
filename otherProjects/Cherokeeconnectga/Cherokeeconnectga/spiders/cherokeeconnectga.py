import json
from datetime import datetime

import scrapy


class CherokeeconnectgaSpider(scrapy.Spider):
    name = 'cherokeeconnectga'
    request_api = 'https://api.membershipworks.com/v2/directory?_st&_rf=Members'
    base_url = 'https://cherokeeconnectga.com/directory/#!biz/id/{}'
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Origin': 'https://cherokeeconnectga.com',
        'Referer': 'https://cherokeeconnectga.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'X-Org': '24745',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    custom_settings = {
        'FEED_URI': 'cherokeeconnectga.csv',
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
        yield scrapy.Request(url=self.request_api, callback=self.parse, headers=self.headers)

    def parse(self, response):
        try:
            json_data = json.loads(response.body)
            for data in json_data.get('usr', []):
                item = dict()
                item['Business Name'] = data.get('nam', '')
                item['Street Address'] = data.get('adr', {}).get('ad1', '')
                item['State'] = data.get('adr', {}).get('sta', '')
                item['Zip'] = data.get('adr', {}).get('zip', '')
                loc = data.get('adr', {}).get('loc', [])
                if loc:
                    item['Latitude'] = loc[1]
                    item['Longitude'] = loc[0]
                phone = data.get('phn', [])
                if phone:
                    item['Phone Number'] = phone[0]
                uid = data.get('uid', '')
                if uid:
                    item['Detail_Url'] = self.base_url.format(uid)
                item['Source_URL'] = 'https://cherokeeconnectga.com/directory/'
                item['Occupation'] = 'Business Service'
                item['Lead_Source'] = 'cherokeeconnectga'
                item['Record_Type'] = 'Business'
                item['Meta_Description'] = ""
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item

        except Exception as ex:
            print('Error in Parser | ' + str(ex))
