import copy
import json
from datetime import datetime

import scrapy


class TristarhealthSpider(scrapy.Spider):
    name = 'tristarhealth'
    request_api = "https://www.tristarhealth.com/fadmaa/provider/search/"
    base_url = 'https://www.tristarhealth.com/physicians/profile/{}'
    custom_settings = {'ROBOTSTXT_OBEY': False,
                       'CONCURRENT_REQUESTS_PER_DOMAIN': 5,
                       'RETRY_TIMES': 5,
                       'FEED_URI': 'tristarhealth.csv',
                       'FEED_FORMAT': 'csv',
                       'FEED_EXPORT_ENCODING': 'utf-8',
                       'FEED_EXPORT_FIELDS': ['Business Name', 'Full Name', 'First Name', 'Last Name', 'Street Address',
                                              'Valid To', 'State', 'Zip', 'Description', 'Phone Number',
                                              'Phone Number 1', 'Email',
                                              'Business_Site', 'Social_Media',
                                              'Category', 'Rating', 'Reviews', 'Source_URL', 'Detail_Url', 'Services',
                                              'Latitude', 'Longitude', 'Occupation',
                                              'Business_Type', 'Lead_Source', 'State_Abrv', 'State_TZ', 'State_Type',
                                              'SIC_Sectors', 'SIC_Categories',
                                              'SIC_Industries', 'NAICS_Code', 'Quick_Occupation', 'Record_Type',
                                              'Scraped_date', 'Meta_Description'],
                       # 'DOWNLOAD_DELAY': 1
                       }
    payload = {
        "coids": "09391;25070;27990;31767;34222;34223;34242;34293;34296;36243;36244",
        "program": "web",
        "keyword": None,
        "zip": "",
        "locationUrlTitle": None,
        "appointmentTypeId": None,
        "source": "dotcms",
        "siteType": "Hospital Operations",
        "startDate": "20221208",
        "numberDaysOut": 549,
        "gender": None,
        "languages": "English",
        "meta": {
            "page": 1,
            "pageSize": 10
        },
        "sortType": "Distance",
        "randomize": True,
        "randomizerSeed": ""
    }
    headers = {
        'authority': 'www.tristarhealth.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'access-control-allow-origin': 'http://localhost:8065',
        'content-type': 'application/json',
        'cookie': 'ASP.NET_SessionId=vjh5ion0512izfcxtzu0oqml; dtCookie=v_4_srv_-2D4_sn_V46T1AFR95JPJKOQTEJTSC4U2JCMQK25; rxVisitor=1670486525964K2Q7F3B5NJ728S83CH88068PH298T7A9; _gcl_au=1.1.1298466947.1670486534; tristar.hospitals#lang=en-US; sxa_site=service; fadmaa_oasSID=dfa05804053149ad9aeb310e0da502bf; _gid=GA1.2.1963190535.1670486536; dtSa=-; SC_ANALYTICS_GLOBAL_COOKIE=e6fb1c57d99a429cb1b00c91e2d26bc3|True; _ga=GA1.2.1146172451.1670486536; _dc_gtm_UA-60997798-42=1; website#lang=en-US; _gali=usr_location; dtLatC=2; _ga_3189V15LSY=GS1.1.1670486536.1.1.1670486695.23.0.0; rxvt=1670488495580|1670486525967; dtPC=-4$86536954_507h15vFBIJHVTUCQCDFIKLJHAARQPHHMSTQLQD-0e0',
        'origin': 'https://www.tristarhealth.com',
        'referer': 'https://www.tristarhealth.com/physicians/listing?zip=33131',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'x-dtpc': '-4$86536954_507h15vFBIJHVTUCQCDFIKLJHAARQPHHMSTQLQD-0e0'
    }

    def start_requests(self):
        page_no = {'page': 1}
        yield scrapy.Request(url=self.request_api, callback=self.parse, method='POST',
                             body=json.dumps(self.payload), headers=self.headers, meta={'page_no': page_no})

    def parse(self, response):
        try:
            current_p = response.meta['page_no']
            json_data = json.loads(response.body)
            for data in json_data.get('result', {}).get('providerList', []):
                item = dict()
                item['First Name'] = data.get('physicianFirstName', '')
                item['Last Name'] = data.get('physicianLastName', '')
                middle = data.get('physicianMiddleInitial', '')
                if middle:
                    item['Full Name'] = item['First Name'] + ' ' + middle + ' ' + item['Last Name']
                else:
                    item['Full Name'] = item['First Name'] + ' ' + item['Last Name']
                item['Street Address'] = data.get('displayLocation', {}).get('street', '')
                item['State'] = data.get('displayLocation', {}).get('state', '')
                item['Zip'] = data.get('displayLocation', {}).get('zip', '')
                item['Phone Number'] = data.get('displayLocation', {}).get('phone', '')
                item['Business Name'] = data.get('displayLocation', {}).get('name', '')
                item['Latitude'] = data.get('displayLocation', {}).get('latitude', '')
                item['Longitude'] = data.get('displayLocation', {}).get('longitude', '')
                detail_url = data.get('urlTitle', '')
                if detail_url:
                    item['Detail_Url'] = self.base_url.format(detail_url)
                item['Source_URL'] = 'https://www.tristarhealth.com/physicians'
                item['Lead_Source'] = 'tristarhealth'
                item['Meta_Description'] = ""
                item['Occupation'] = 'Doctor'
                item['Record_Type'] = 'Business'
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item

            old_page = current_p.get('page', '')
            next_page = old_page + 1
            total_records = json_data.get('result', {}).get('totalProvidersCount', '')
            total_pages = int(total_records)/10
            if next_page <= total_pages:
                current_p['page'] = next_page
                payload = copy.deepcopy(self.payload)
                payload['meta']['page'] = next_page
                yield scrapy.Request(url=self.request_api, callback=self.parse, method='POST',
                                     body=json.dumps(payload), headers=self.headers, meta={'page_no': current_p})
        except Exception as ex:
            print('Error while reading json | ' + str(ex))
