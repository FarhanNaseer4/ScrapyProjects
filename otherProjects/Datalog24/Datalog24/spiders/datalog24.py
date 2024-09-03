import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class Datalog24Spider(scrapy.Spider):
    name = 'datalog24'
    start_urls = ['https://datalog24.com/']
    custom_settings = {
        'FEED_URI': 'datalog24.csv',
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
        'authority': 'datalog24.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'cookie': '_ga=GA1.1.905058775.1669881293; __atuvc=9%7C48; __atuvs=63885dcd37953907008; _ga_WQGYJ366QF=GS1.1.1669881293.1.1.1669881367.0.0.0',
        'referer': 'https://datalog24.com/',
        'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
    }

    def parse(self, response):
        for data in response.css('div.description a[itemprop="url"]'):
            city_url = data.css('::attr(href)').get()
            if city_url:
                yield response.follow(url=city_url, callback=self.parse_city, headers=self.headers)

    def parse_city(self, response):
        for data in response.css('div.description a[itemprop="url"]'):
            cate_url = data.css('::attr(href)').get()
            item = {'Occupation': data.css('::text').get('').strip()}
            if cate_url:
                yield response.follow(url=cate_url, callback=self.parse_listing,
                                      headers=self.headers, meta={'item': item})

    def parse_listing(self, response):
        item = response.meta['item']
        for data in response.css('div.col-md-8 h3 a'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.parse_detail,
                                      headers=self.headers, meta={'item': item})

    def parse_detail(self, response):
        item = response.meta['item']
        item['Business Name'] = response.css('span[itemprop="name"]::text').get('').strip()
        contact_detail = response.css('span[itemprop="address"]::text').get('').strip()
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
        phone = response.css('span[itemprop="telephone"]::text').get('').strip()
        if len(phone) > 9:
            print('greater')
            item['Phone Number'] = phone
        else:
            print('less' + phone)
        item['Category'] = response.xpath('//th[contains(text(),"Speciality")]/following-sibling::td/text()').get('').strip()
        item['Detail_Url'] = response.url
        item['Source_URL'] = 'https://datalog24.com/'
        item['Lead_Source'] = 'datalog24'
        item['Meta_Description'] = ""
        item['Record_Type'] = 'Person'
        item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield item

