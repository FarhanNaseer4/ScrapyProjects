import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class EdmondchamberSpider(scrapy.Spider):
    name = 'edmondchamber'
    start_urls = ['https://cca.edmondchamber.com/businesssearch.aspx']
    custom_settings = {
        'FEED_URI': 'edmondchamber.csv',
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
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/107.0.0.0 Safari/537.36 '
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse, headers=self.headers)

    def parse(self, response):
        for data in response.css('a.ccaCategoryLink'):
            cate_url = data.css('::attr(href)').get()
            if cate_url:
                yield response.follow(url=cate_url, callback=self.parse_listing, headers=self.headers)

    def parse_listing(self, response):
        for data in response.css('a.ccaMemName'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.parse_detail, headers=self.headers)

    def parse_detail(self, response):
        item = dict()
        item['Business Name'] = response.css('span.ccaMemName::text').get('').strip()
        item['Street Address'] = response.css('a.ccaLine1::text').get('').strip()
        contact_detail = response.css('a.ccaCityStateZip::text').get('').strip()
        try:
            states = re.findall(r'\b[A-Z]{2}\b', contact_detail)
            if len(states) == 2:
                state = states[-1]
            else:
                state = states[0]
        except:
            state = ''
        try:
            zip_code = re.search(r"(?!\A)\b\d{5}(?:-\d{4})?\b", contact_detail).group(0)
        except:
            zip_code = ''
        item['State'] = state
        item['Zip'] = zip_code
        item['Business_Site'] = response.css('a.ccaWebAddrLk::attr(href)').get('').strip()
        item['Phone Number'] = response.css('a.ccaPhone::text').get('').strip()
        fullname = self.get_name_parts(response.css('a.ccaRepName::text').get('').strip())
        item['Full Name'] = fullname.get('full_name', '')
        item['First Name'] = fullname.get('first_name', '')
        item['Last Name'] = fullname.get('last_name', '')
        item['Detail_Url'] = response.url
        item['Source_URL'] = 'https://cca.edmondchamber.com/businesssearch.aspx'
        item['Lead_Source'] = 'edmondchamber'
        item['Meta_Description'] = ""
        item['Occupation'] = response.css('a.ccaCategoryLink::text').get('').strip()
        item['Record_Type'] = 'Business'
        item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield item

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
