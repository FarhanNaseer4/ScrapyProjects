import re
from datetime import datetime

import scrapy


class MyypSpider(scrapy.Spider):
    name = 'myyp'
    zyte_key = '07a4b6f903574c1d8b088b55ff0265fc'
    start_urls = ['https://www.myyp.com/allcategories/']
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'myyp.csv',
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
        for data in response.css('ul.col-list li a'):
            item = dict()
            item['Occupation'] = data.css('::text').get('').strip()
            cate_url = data.css('::attr(href)').get()
            if cate_url:
                yield response.follow(url=cate_url, callback=self.parse_city,
                                      headers=self.headers, meta={'item': item})

    def parse_city(self, response):
        item = response.meta['item']
        for data in response.css('ul.flex-list li a'):
            city_url = data.css('::attr(href)').get()
            if city_url:
                yield response.follow(url=city_url, callback=self.parse_listing,
                                      headers=self.headers, meta={'item': item})

    def parse_listing(self, response):
        item = response.meta['item']
        for data in response.css('div.listing-content h2 a'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.parse_details,
                                      headers=self.headers, meta={'item': item})

    def parse_details(self, response):
        item = response.meta['item']
        item['Business Name'] = response.css('h1[id="name"]::text').get('').strip()
        item['Street Address'] = response.css('span[itemprop="streetAddress"]::text').get('').strip()
        contact_detail = response.css('span[itemprop="addressLocality"]::text').get('').strip()
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
        item['Phone Number'] = response.css('span[itemprop="telephone"]::text').get('').strip()
        item['Social_Media'] = ', '.join(
            data.css('::attr(href)').get('') for data in response.css('div.profile-social-icons a'))
        item['Detail_url'] = response.url
        item['Source_URL'] = 'https://www.myyp.com/allcategories/'
        item['Lead_Source'] = 'myyp'
        item['Meta_Description'] = "BuyLocal - Search the yellow pages for phone numbers and addresses of businesses, " \
                                   "people, and more"
        item['Record_Type'] = 'Business'
        item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield item
