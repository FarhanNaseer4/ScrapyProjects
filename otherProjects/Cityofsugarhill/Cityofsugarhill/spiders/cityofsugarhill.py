from datetime import datetime

import scrapy


class CityofsugarhillSpider(scrapy.Spider):
    name = 'cityofsugarhill'
    start_urls = ['https://cityofsugarhill.com/app/business-list/']

    custom_settings = {
        'FEED_URI': 'cityofsugarhill.csv',
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

    def parse(self, response):
        for data in response.xpath('//table//tr/following::tr'):
            item = dict()
            item['Business Name'] = data.css('td:nth-child(1)::text').get('').strip()
            item['Street Address'] = data.css('td:nth-child(2)::text').get('').strip()
            item['Phone Number'] = data.css('td:nth-child(3)::text').get('').strip()
            item['Occupation'] = data.css('td:nth-child(4)::text').get('').strip()
            item['Source_URL'] = 'https://cityofsugarhill.com/app/business-list/'
            item['Lead_Source'] = 'cityofsugarhill'
            item['Meta_Description'] = ""
            item['Record_Type'] = 'Business'
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield item

