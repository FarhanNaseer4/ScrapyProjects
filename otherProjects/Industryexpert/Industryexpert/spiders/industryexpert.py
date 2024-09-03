from datetime import datetime

import scrapy


class IndustryexpertSpider(scrapy.Spider):
    name = 'industryexpert'
    start_urls = ['https://industryexpert.net/expert-directory/']
    custom_settings = {
        'FEED_URI': 'industryexpert.csv',
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
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/105.0.0.0 Safari/537.36 '
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse, headers=self.headers)

    def parse(self, response):
        for data in response.css('div.expert'):
            item = dict()
            item['First Name'] = data.css('span.given-name::text').get('').strip()
            item['Last Name'] = data.css('span.family-name::text').get('').strip()
            item['Full Name'] = item['First Name'] + ' ' + item['Last Name']
            item['Business Name'] = data.css('span.organization-name::text').get('').strip()
            item['Occupation'] = data.css('h6 span.title::text').get('').strip()
            item['Category'] = ', '.join(cate.css('::text').get('') for cate in data.css('span.cn-category-name'))
            item['Phone Number'] = data.css('span.cn-phone-number span.value::text').get('').strip()
            item['Email'] = ' | '.join(email.css('::text').get('') for email in data.css('span.email-address a'))
            item['Business_Site'] = data.css('span.website a.url::attr(href)').get('').strip()
            item['Street Address'] = data.css('span.street-address::text').get('').strip()
            item['State'] = data.css('span.region::text').get('').strip()
            item['Zip'] = data.css('span.postal-code::text').get('').strip()
            item['Detail_Url'] = data.css('div.name h4 a::attr(href)').get()
            item['Source_URL'] = 'https://industryexpert.net/expert-directory/'
            item['Lead_Source'] = 'industryexpert'
            item['Meta_Description'] = ""
            item['Record_Type'] = 'Person'
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield item

        next_page = response.css('a.cn-next-page::attr(href)').get()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse, headers=self.headers)


