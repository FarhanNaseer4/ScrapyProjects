import json
from datetime import datetime

import scrapy


class OlolrmcSpider(scrapy.Spider):
    name = 'ololrmc'
    request_api = 'https://doctors.ololrmc.com/api/search?sort=networks&page={}'
    base_url = 'https://doctors.ololrmc.com/provider/{}/{}'
    custom_settings = {
        'FEED_URI': 'ololrmc.csv',
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
        'authority': 'doctors.ololrmc.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cookie': '__utma=207760844.2006644630.1669720382.1669720382.1669720382.1; __utmz=207760844.1669720382.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _gcl_au=1.1.774849929.1669720383; _gid=GA1.2.1310092354.1669720384; consumer_tracking_token=4d71b7ec-7d31-4311-9600-6bef51dacace; search_shuffle_token=c63721d5-72bf-4634-9e9a-b85ed86f6f1f; consumer_user_token=8e97928a-126f-43ea-84e7-534503a4b6ed; calltrk_referrer=https%3A//ololrmc.com/find-a-location/; calltrk_landing=https%3A//ololrmc.com/find-a-location/location-search-results%3FPostalCode%3D36104%26%26SearchPattern%3DContains%26LocationDescendants%3Dtrue; _ga_V55L1HQW9M=GS1.1.1669720383.1.1.1669720430.0.0.0; _ga_KDJNZY2BBD=GS1.1.1669793346.2.0.1669793346.0.0.0; _gat_kyruusTracker=1; _gat_UA-157894082-4=1; _ga=GA1.2.1100994148.1669720383; consumer_tracking_token=4d71b7ec-7d31-4311-9600-6bef51dacace; consumer_user_token=8e97928a-126f-43ea-84e7-534503a4b6ed; search_shuffle_token=c63721d5-72bf-4634-9e9a-b85ed86f6f1f',
        'referer': 'https://doctors.ololrmc.com/search?sort=networks&page=2',
        'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
        'x-csrf-header': 'fmolhs'
    }

    def start_requests(self):
        page = {'no': 1}
        yield scrapy.Request(url=self.request_api.format(1), callback=self.parse,
                             meta={'page': page}, headers=self.headers)

    def parse(self, response):
        try:
            page = response.meta['page']
            json_data = json.loads(response.body)
            for data in json_data.get('data', {}).get('providers', []):
                item = dict()
                item['Full Name'] = data.get('name', {}).get('full_name', '')
                item['First Name'] = data.get('name', {}).get('first_name', '')
                item['Last Name'] = data.get('name', {}).get('last_name', '')
                contact = data.get('contacts', [])
                if contact:
                    item['Phone Number'] = contact[0].get('value', '')
                address = data.get('locations', [])
                if address:
                    item['Street Address'] = address[0].get('street1', '')
                    item['State'] = address[0].get('state', '')
                    item['Zip'] = address[0].get('zip', '')
                    item['Business Name'] = address[0].get('name', '')
                item['Category'] = data.get('provider_type', '')
                p_id = data.get('id', '')
                name = data.get('name', {}).get('full_name', '')
                item['Detail_Url'] = self.base_url.format(name.replace(' ', '+'), p_id)
                item['Source_URL'] = 'https://doctors.ololrmc.com/search?sort=networks'
                item['Lead_Source'] = 'ololrmc'
                item['Meta_Description'] = ""
                item['Occupation'] = 'Doctor'
                item['Record_Type'] = 'Person'
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item

            total_pages = json_data.get('data', {}).get('total_pages', '')
            current_page = page.get('no', '')
            next_page = current_page + 1
            if next_page <= total_pages:
                page['no'] = next_page
                yield scrapy.Request(url=self.request_api.format(next_page), callback=self.parse,
                                     meta={'page': page}, headers=self.headers)
        except Exception as ex:
            print('Error From Parse | ' + str(ex))
