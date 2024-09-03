import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class AmarilloChamberSpider(scrapy.Spider):
    name = 'amarillo_chamber'
    start_urls = ['https://web.amarillo-chamber.org/allcategories']

    base_url = 'https://web.amarillo-chamber.org{}'
    custom_settings = {
        'FEED_URI': 'amarillo_chamber.csv',
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
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/106.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
    }

    def parse(self, response):
        for data in response.css('li.ListingCategories_AllCategories_CATEGORY a'):
            list_url = data.css('::attr(href)').get()
            if not list_url.startswith(self.base_url):
                list_url = self.base_url.format(list_url)
            yield scrapy.Request(url=list_url, callback=self.listing_page, headers=self.headers)

    def listing_page(self, response):
        for data in response.css('div.ListingResults_All_CONTAINER'):
            item = dict()
            detail_url = data.css('span[itemprop="name"] a::attr(href)').get()
            if detail_url:
                item['Detail_Url'] = self.base_url.format(detail_url)
            item['Business Name'] = data.css('span[itemprop="name"] a::text').get('').strip()
            fullname = self.get_name_parts(data.xpath('.//div[contains(@class,"MAINCONTACT")]//text()').get('').strip())
            item['Full Name'] = fullname.get('full_name', '')
            item['First Name'] = fullname.get('first_name', '')
            item['Last Name'] = fullname.get('last_name', '')
            item['Phone Number'] = data.xpath('.//div[contains(@class,"PHONE")]//text()').get('').strip()
            item['Street Address'] = data.css('span[itemprop="street-address"]::text').get('').strip()
            item['State'] = data.css('span[itemprop="region"]::text').get('').strip()
            item['Zip'] = data.css('span[itemprop="postal-code"]::text').get('').strip()
            item['Source_URL'] = 'https://web.amarillo-chamber.org/allcategories'
            item['Lead_Source'] = 'amarillo-chamber'
            item['Meta_Description'] = ""
            item['Occupation'] = response.css('h1[id="wc-pageTitle"]::text').get('').strip()
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
