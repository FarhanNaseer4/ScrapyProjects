import copy
import csv
import json
from datetime import datetime

import scrapy


class JimmyjohnsSpider(scrapy.Spider):
    name = 'jimmyjohns'
    request_api = 'https://www.jimmyjohns.com/webservices/Location/LocationServiceHandler.asmx/GetStoreAddressesByCityAndState'
    payload = {
        'city': 'Alabaster',
        'state': 'AL',
    }
    headers = {
        'authority': 'www.jimmyjohns.com',
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'content-type': 'application/json; charset=UTF-8',
        'cookie': '__cf_bm=epq.Hbu8BN_VbscmqdW8jqZyvcz.QGS1AOrgD_YhO6g-1671099694-0-AaddljJXn7QPtHwOyWzYf5tBYf+Bk8JwPtvjKT2YxSIUu30NWfe1t1wgogAk7Us5A71SoshsL92V/g2UkVcPe68=; __cflb=02DiuJ23HBgTA9eshrhN1qKL5cKCkdQQSvcgfFX7Zc3gz; _gcl_au=1.1.1264783293.1671099697; _mibhv=anon-1665985179304-7422757952_8473; initialTrafficSource=utmcsr=(direct)|utmcmd=(none)|utmccn=(not set); __utmzzses=1; _gid=GA1.2.2078243893.1671099700; _gat_UA-817850-1=1; _rdt_uuid=1671099700245.55042fb7-e318-4cf6-8b3f-77323b37444c; _uetsid=43aed3807c6211edb6aa5b0f2e0d9f32; _uetvid=1a6d2d804dde11edb77d1d85e2213c1b; _hjSessionUser_1753675=eyJpZCI6IjY5OTBlMTVlLTlkYmItNTA0Yy1iYWI5LWZhNmJiMTY0OTZmYSIsImNyZWF0ZWQiOjE2NzEwOTk3MDA3MzgsImV4aXN0aW5nIjpmYWxzZX0=; _hjFirstSeen=1; _hjIncludedInSessionSample=0; _hjSession_1753675=eyJpZCI6IjljNDkxMTM0LWVkMWItNDU3Mi04MTU5LWNjN2U2ZjAwOWUwNCIsImNyZWF0ZWQiOjE2NzEwOTk3MDE0MTUsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; _hjCachedUserAttributes=eyJhdHRyaWJ1dGVzIjp7IkV4cGVyaW1lbnRJRCI6bnVsbH0sInVzZXJJZCI6bnVsbH0=; _aeaid=8ce58689-739c-449b-bfcf-4c43cbf1c050; aelastsite=3hzjStq7fUQjvNFwAeK02exLBTC3nipL9vYFZpvPEc3VK51WLKNKepu6dzqxFwEa; aelreadersettings=%7B%22c_big%22%3A0%2C%22rg%22%3A0%2C%22memph%22%3A0%2C%22contrast_setting%22%3A0%2C%22colorshift_setting%22%3A0%2C%22text_size_setting%22%3A0%2C%22space_setting%22%3A0%2C%22font_setting%22%3A0%2C%22k%22%3A0%2C%22k_disable_default%22%3A0%2C%22hlt%22%3A0%2C%22disable_animations%22%3A0%2C%22display_alt_desc%22%3A0%7D; _ga=GA1.2.452215444.1671099700; cookieconsent_status=dismiss; _ga_4TBFC6C6LH=GS1.1.1671099700.1.1.1671099747.0.0.0; __cf_bm=xs42d5.N5kBexqx9SrwdQ9v34K35gbvokVn4vzf5f5M-1671099975-0-AajQC6oXijGxdnbaxkUrlM4lwa5vJ/YVSrh+RxEQ3I6yfwUCcnkL0gthaxlMGImjB4BtHB8abGDRTl2EYntFDfY=',
        'origin': 'https://www.jimmyjohns.com',
        'referer': 'https://www.jimmyjohns.com/find-a-jjs/',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    custom_settings = {
        'FEED_URI': 'jimmyjohns.csv',
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
            state = data.get('state_abbr')
            city = data.get('city')
            payload = copy.deepcopy(self.payload)
            payload['city'] = city
            payload['state'] = state

            yield scrapy.Request(url=self.request_api, callback=self.parse, headers=self.headers,
                                 method='POST', body=json.dumps(payload))

    def parse(self, response):
        json_data = json.loads(response.body)
        # print(json_data)
        if json_data:
            for data in json_data.get('d', []):
                item = dict()
                store_no = data.get('StoreNumber', '')
                item['Business Name'] = 'Jimmyjohns Store ' + str(store_no)
                item['Street Address'] = data.get('Address', '')
                item['State'] = data.get('State', '')
                item['Zip'] = data.get('ZipCode', '')
                item['Phone Number'] = data.get('Telephone', '')
                item['Latitude'] = data.get('Lat', '')
                item['Longitude'] = data.get('Lng', '')
                item['Source_URL'] = 'https://www.jimmyjohns.com/find-a-jjs/'
                item['Occupation'] = 'jimmyjohns Store'
                item['Lead_Source'] = 'jimmyjohns'
                item['Record_Type'] = 'Business'
                item['Meta_Description'] = "With 2,800 locations across 43 states in the U.S, Jimmy John's is focused on " \
                                           "making Freaky Fast sandwiches using only the freshest ingredients."
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item

