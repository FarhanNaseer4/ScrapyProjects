from datetime import datetime

import scrapy


class StpeteSpider(scrapy.Spider):
    name = 'stpete'
    start_urls = ['https://business.stpete.com/memberdirectory']

    custom_settings = {
        'FEED_URI': 'stpete.csv',
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

    def parse(self, response):
        for data in response.css('div.gz-alphanumeric-btn a'):
            url = data.css('::attr(href)').get()
            item = dict()
            item['Meta_Description'] = response.xpath('//meta[@name="description" or '
                                                      '@property="og:description"]/@content').get('').strip()
            if url:
                yield response.follow(url=url, callback=self.parse_listing, headers=self.headers, meta={'item': item})

    def parse_listing(self, response):
        item = response.meta['item']
        for data in response.css('div.gz-directory-card-body'):
            item['Business Name'] = data.css('h5[itemprop="name"] a::text').get('').strip()
            item['Street Address'] = data.css('span[itemprop="streetAddress"]::text').get('').strip()
            item['State'] = data.css('span[itemprop="addressRegion"]::text').get('').strip()
            item['Zip'] = data.css('span[itemprop="postalCode"]::text').get('').strip()
            item['Phone Number'] = data.css('span[itemprop="telephone"]::text').get('').strip()
            item['Business_Site'] = data.css('li.gz-card-website a::attr(href)').get('').strip()
            item['Source_URL'] = 'https://business.stpete.com/memberdirectory'
            item['Lead_Source'] = 'stpete'
            item['Occupation'] = 'Business Service'
            item['Record_Type'] = 'Business'
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield item

