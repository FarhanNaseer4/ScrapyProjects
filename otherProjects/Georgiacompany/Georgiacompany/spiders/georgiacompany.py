import re
from datetime import datetime

import scrapy


class GeorgiacompanySpider(scrapy.Spider):
    name = 'georgiacompany'
    start_urls = ['https://www.georgiacompany.org/']
    zyte_key = '07a4b6f903574c1d8b088b55ff0265fc'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'georgiacompany.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORT_FIELDS': ['Business Name', 'Full Name', 'First Name', 'Last Name', 'Street Address',
                               'Valid To', 'State', 'Zip', 'Description', 'Phone Number', 'Phone Number 1', 'Email',
                               'Business_Site', 'Social_Media', 'Record_Type',
                               'Category', 'Rating', 'Reviews', 'Source_URL', 'Detail_Url', 'Services',
                               'Latitude', 'Longitude', 'Occupation',
                               'Business_Type', 'Lead_Source', 'State_Abrv', 'State_TZ', 'State_Type',
                               'SIC_Sectors', 'SIC_Categories',
                               'SIC_Industries', 'NAICS_Code', 'Quick_Occupation', 'Scraped_date', 'Meta_Description'],
        'HTTPERROR_ALLOW_ALL': True,
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/105.0.0.0 Safari/537.36 '
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse, headers=self.headers)

    def parse(self, response):
        for data in response.css('table.table td a'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.parse_detail, headers=self.headers)

        next_page = response.css('a.next::attr(href)').get()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse, headers=self.headers)

    def parse_detail(self, response):
        item = dict()
        item['Business Name'] = response.css('td[itemprop="legalName"] span::text').get('').strip()
        contact_detail = response.css('td[itemprop="address"]::text').get('').strip()
        try:
            states = re.findall(r'\b[A-Z]{2}\b', contact_detail)
            if len(states) == 2:
                state = states[-1]
            else:
                state = states[0]
        except:
            state = ''
        try:
            street = contact_detail.rsplit(state, 1)[0].strip().rstrip(',').strip()
        except:
            street = ''
        try:
            zip_code = re.search(r"(?!\A)\b\d{5}(?:-\d{4})?\b", contact_detail).group(0)
        except:
            zip_code = ''
        item['Street Address'] = street
        item['State'] = state
        item['Zip'] = zip_code
        item['Occupation'] = response.xpath('//td[contains(text(), "Company Type")]/following-sibling::td/text()').get('').strip()
        item['Detail_Url'] = response.url
        item['Source_URL'] = 'https://www.georgiacompany.org/'
        item['Lead_Source'] = 'georgiacompany'
        item['Record_Type'] = 'Business'
        item['Meta_Description'] = "www.georgiacompany.org is a free database collect business information about " \
                                   "Georgia Companies."
        item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield item


