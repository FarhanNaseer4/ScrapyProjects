import json

import scrapy


class LasvegasmarketSpider(scrapy.Spider):
    name = 'lasvegasmarket'
    request_api = 'https://www.lasvegasmarket.com/imc-api/v2/exhibitors/search?sc_apikey=391D75C6-01EE-463C-8B51-47B2748F8ACD&f:Product%20Categories=203|270|204|205|206|185|207|208|209|328|210|21&page={}&pageSize=200&searchPage=cfa5a5c2-9bd7-4309-b356-1acdef98a65f'
    company_api = 'https://www.lasvegasmarket.com/imc-api/v2/exhibitors/OpenDetails?sc_apikey=391D75C6-01EE-463C-8B51-47B2748F8ACD&exhibitorIds={}&pageId=c263733f-2601-448b-b0cb-892922c3d945&'
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Channel': 'las-vegas-market',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': 'TiPMix=4.301785983102368; x-ms-routing-name=self; ASP.NET_SessionId=me5w5sz4rzw2o2rgxb4rgmdc; __RequestVerificationToken=gTZlck36y9C41x-agaoUWQlIX_pKy6BaPgSMCqCFUML1Q3F1iMpI9Y_jhnwgNmirzzses720klZp8MxvCXZ6nhwICPEaykFD5-ngXS0Q0VU1; sxa_site=las-vegas-market; ai_user=XewuU|2023-01-23T18:34:31.040Z; _gcl_au=1.1.1784548663.1674498871; _ga=GA1.2.1765189949.1674498872; _gid=GA1.2.872205918.1674498872; _uetsid=93c2b7e09b4c11ed8f616b2f269d1a9a; _uetvid=798d1a006b2311edb16ccf918eea4277; SC_ANALYTICS_GLOBAL_COOKIE=a5d28d26acb3481682d2fbe2723d1ab2|True; ln_or=eyIyMTk5NDgyIjoiZCJ9; sa-user-id=s%253A0-b217ac9f-b874-475b-6a35-23d6646b81a1.O8UsAyR3f7wWS62C83gQFDXg5kSEbbU9A%252F%252Fjdl%252FzMW8; sa-user-id-v2=s%253Ashesn7h0R1tqNSPWZGuBoTtnZhM.CjebuW%252BKMod17aKQFrGr5EJ%252Fsto2Fx99p99S1KWJHho; _pin_unauth=dWlkPU16UTBabU0xT0RZdFpHUmlaUzAwT1RJM0xXSXdaR0V0TVdGaE5EZGlPR1ZoWkdGbQ; _hjSessionUser_468859=eyJpZCI6IjJkOTkxYTY5LTI5NzMtNWU2Yy1hM2IyLTZhOTBkMGJmYzQwOCIsImNyZWF0ZWQiOjE2NzQ0OTg4NzIxODUsImV4aXN0aW5nIjpmYWxzZX0=; _hjFirstSeen=1; _hjIncludedInSessionSample=0; _hjSession_468859=eyJpZCI6IjA2MDFjNjllLTk3ZTEtNDk5NS04ODljLTQxYjA1ZTM1ZWE2MSIsImNyZWF0ZWQiOjE2NzQ0OTg4NzMwNTksImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; las-vegas-market#lang=en; _gat_UA-8853464-1=1; ai_session=G/fFI|1674498871834.5|1674498940444.7; _gat_UA-8853464-20=1',
        'Referer': 'https://www.lasvegasmarket.com/exhibitor/exhibitor-directory?exhibitor-directory=Product%2BCategories%3D203%257C270%257C204%257C205%257C206%257C185%257C207%257C208%257C209%257C328%257C210%257C21&page=2',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    custom_settings = {
        'FEED_URI': 'lasvegasmarket_V1.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }

    def start_requests(self):
        yield scrapy.Request(url=self.request_api.format(1), callback=self.parse, headers=self.headers)

    def parse(self, response):
        json_data = json.loads(response.body)
        if json_data:
            for data in json_data.get('data', []):
                exhibitor_id = data.get('exhibitorId', '')
                yield scrapy.Request(url=self.company_api.format(exhibitor_id), callback=self.parse_details,
                                     headers=self.headers)

    def parse_details(self, response):
        json_data = json.loads(response.body)
        if json_data:
            for data in json_data.get('data', []):
                item = dict()
                item['Company Name'] = data.get('companyDetails', {}).get('companyName', '')
                item['Company Address'] = data.get('directoryContactInfo', {}).get('address1', '')
                item['City'] = data.get('directoryContactInfo', {}).get('city', '')
                state_code = data.get('directoryContactInfo', {}).get('state', '')
                country_code = data.get('directoryContactInfo', {}).get('country', '')
                all_countries = data.get('directoryContactInfo', {}).get('countries', [])
                country_data = [d.get('state', []) for d in all_countries if int(d.get('code', '')) == int(country_code)]
                if any(country_data):
                    state_data = [d for d in country_data[0] if int(d.get('code', '')) == int(state_code)]
                    item['State'] = state_data[0].get('stateAbbreviation', '') if state_data else ''
                else:
                    item['State'] = ''
                item['Zip'] = data.get('directoryContactInfo', {}).get('zip', '')
                item['Phone number'] = data.get('directoryContactInfo', {}).get('primaryPhoneNo', '')
                item['Web Address'] = data.get('companyInformation', {}).get('companyWebsiteUrl', '')
                first_name = data.get('directoryContactInfo', {}).get('directoryContactFirstName', '')
                last_name = data.get('directoryContactInfo', {}).get('directoryContactLastName', '')
                if first_name and last_name:
                    item['Contact Name'] = first_name + ' ' + last_name
                item['Category'] = 'Home Textiles'
                yield item
