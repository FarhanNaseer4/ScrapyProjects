import json
from datetime import datetime

import scrapy


class ConehealthSpider(scrapy.Spider):
    name = 'conehealth'
    request_api = "https://www.conehealth.com/find-a-doctor/doctor-search-results/?searchId=ce8787b8-ff75-ed11-a856" \
                  "-000d3a61151d&sort=3&page={}&pageSize=10"
    base_url = 'https://www.conehealth.com{}'
    custom_settings = {
        'FEED_URI': 'conehealth.csv',
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
        'authority': 'www.conehealth.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'cookie': 'ASP.NET_SessionId=22tg1vee1pvupfoze0ysqacs; mobileview=web; cookiesession1=678A3E54PQRSTUVWXYZBCD012345BD25; _gcl_au=1.1.569487541.1670397634; _gid=GA1.2.1866261305.1670397636; LB_SessionId=141453322.1.2525811344.3275195392; _hjFirstSeen=1; _hjIncludedInSessionSample=0; _hjSession_1054698=eyJpZCI6IjM0ZDhlNjEzLTIzNWEtNDc2NC1iMzU0LWRiNjA1MjUzNGFkNSIsImNyZWF0ZWQiOjE2NzAzOTc2NDE1NDAsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; _RCRTX03=71fc08cf449211edaa0a0dc2c14dddfb5b4dc5319931429db843646c8b539589; _RCRTX03-samesite=71fc08cf449211edaa0a0dc2c14dddfb5b4dc5319931429db843646c8b539589; _hjSessionUser_1054698=eyJpZCI6ImRkZjcxYjcyLWJjMzItNWU5Ny1hMDA4LTUxYzdlNjZmNjBiNSIsImNyZWF0ZWQiOjE2NzAzOTc2NDEzMTUsImV4aXN0aW5nIjp0cnVlfQ==; __atuvc=5%7C49; __atuvs=63903ec1414f4c6d004; _ga=GA1.1.1831233010.1670397636; _ga_JNLB81E21L=GS1.1.1670397636.1.1.1670397736.32.0.0; _ga_QGNQZMZZGH=GS1.1.1670397636.1.1.1670397736.32.0.0; mobileview=web',
        'referer': 'https://www.conehealth.com/find-a-doctor/doctor-search-results/?filterTermId=35a8d535-9d2b-e311-8f58-2c768a4e1b84&sort=3',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    def start_requests(self):
        current = {'c_page': 1}
        yield scrapy.Request(url=self.request_api.format(1), callback=self.parse,
                             headers=self.headers, meta={'current': current})

    def parse(self, response):
        cur_page = response.meta['current']
        for data in response.css('div.DrNameTitleWrap a.ProfileAnchor'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield scrapy.Request(url=self.base_url.format(detail_url), callback=self.parse_detail,
                                     headers=self.headers)
        current_p = cur_page.get('c_page')
        total_page = response.css('select.SuperShort option:last-child::text').get('').strip()
        next_p = current_p + 1
        if next_p <= int(total_page):
            cur_page['c_page'] = next_p
            yield scrapy.Request(url=self.request_api.format(next_p), callback=self.parse,
                                 headers=self.headers, meta={'current': cur_page})

    def parse_detail(self, response):
        try:
            json_data = response.css('script[type="application/ld+json"]::text').get('').strip()
            if json_data:
                json_result = json.loads(json_data)
                item = dict()
                item['First Name'] = json_result.get('employee', {}).get('givenName', '')
                item['Last Name'] = json_result.get('employee', {}).get('familyName', '')
                middle = json_result.get('employee', {}).get('additionalName', '')
                if middle:
                    item['Full Name'] = item['First Name'] + ' ' + middle + ' ' + item['Last Name']
                else:
                    item['Full Name'] = item['First Name'] + ' ' + item['Last Name']
                item['Phone Number'] = json_result.get('telephone', '')
                location = json_result.get('location', [])
                if location:
                    item['Street Address'] = location[0].get('streetAddress', '')
                    item['State'] = location[0].get('addressRegion', '')
                    item['Zip'] = location[0].get('postalCode', '')
                item['Detail_Url'] = json_result.get('url', '')
                organization = json_result.get('parentOrganization', [])
                if organization:
                    item['Business Name'] = organization[0].get('name', '')
                item['Rating'] = json_result.get('aggregateRating', {}).get('ratingValue', '')
                item['Reviews'] = json_result.get('aggregateRating', {}).get('ratingCount', '')
                item['Phone Number 1'] = json_result.get('contactPoint', {}).get('telephone', '')
                item['Source_URL'] = 'https://www.conehealth.com/find-a-doctor/'
                item['Occupation'] = 'Doctor'
                item['Lead_Source'] = 'conehealth'
                item['Meta_Description'] = "Find a doctor for your healthcare needs at Cone Health. Search by name, " \
                                           "specialty, gender, or location."
                item['Record_Type'] = 'Person'
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item
        except Exception as ex:
            print('Error in parse_detail | ' + str(ex))
