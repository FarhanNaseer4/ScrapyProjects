import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class FarmershotlineSpider(scrapy.Spider):
    name = 'farmershotline'
    start_urls = ['https://www.farmershotline.com/business-directory']
    base_url = 'https://www.farmershotline.com{}'
    custom_settings = {
        'FEED_URI': 'farmershotline.csv',
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
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/106.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
    }

    def parse(self, response):
        for data in response.css('h3.field-content a'):
            cate_url = data.css('::attr(href)').get()
            if cate_url:
                yield response.follow(url=cate_url, callback=self.parse_sub_cate, headers=self.headers)

    def parse_sub_cate(self, response):
        for data in response.css('h3.field-content a'):
            sub_cate = data.css('::attr(href)').get()
            if sub_cate:
                yield response.follow(url=sub_cate, callback=self.parse_listing, headers=self.headers)

    def parse_listing(self, response):
        for data in response.xpath('//table[contains(@class,"views-table")]//tr/following::tr'):
            item = dict()
            item['Business Name'] = data.css('td.views-field-title a::text').get('').strip()
            item['Phone Number'] = data.css('td.views-field-field-fhl-phone-number-value::text').get('').strip()
            item['State'] = data.css('td.views-field-province::text').get('').strip()
            item['Street Address'] = data.css('td.views-field-city::text').get('').strip()
            detail_url = data.css('td.views-field-title a::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.parse_detail,
                                      headers=self.headers, meta={'item': item})
            else:
                item['Detail_Url'] = self.base_url.format(data.css('td.views-field-title a::attr(href)').get())
                item['Source_URL'] = 'https://www.farmershotline.com/business-directory'
                item['Lead_Source'] = 'farmershotline'
                item['Meta_Description'] = ""
                item['Occupation'] = 'Business Service'
                item['Record_Type'] = 'Business'
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if item['Business Name']:
                    yield item
        next_page = response.css('li.pager-next a::attr(href)').get()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse_listing, headers=self.headers)

    def parse_detail(self, response):
        item = response.meta['item']
        item['Business_Site'] = response.css('div.field-field-fhl-website a::attr(href)').get('').strip()
        name = response.css('div.field-field-contact-name div.odd::text').get('').strip()
        fullname = self.get_name_parts(name)
        item['Full Name'] = fullname.get('full_name', '')
        item['First Name'] = fullname.get('first_name', '')
        item['Last Name'] = fullname.get('last_name', '')
        item['Detail_Url'] = response.url
        item['Source_URL'] = 'https://www.farmershotline.com/business-directory'
        item['Lead_Source'] = 'farmershotline'
        item['Meta_Description'] = ""
        item['Occupation'] = 'Business Service'
        item['Record_Type'] = 'Business'
        item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if item['Business Name']:
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


