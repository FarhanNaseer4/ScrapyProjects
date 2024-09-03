import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class MontanabidsSpider(scrapy.Spider):
    name = 'montanabids'
    start_urls = ['https://www.montanabids.us/montana-contractors/category.htm']

    base_url = 'https://www.montanabids.us{}'
    custom_settings = {
        'FEED_URI': 'montanabids.csv',
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
        for data in response.xpath('//h2[contains(text(),"NAICS")]/following-sibling::div/ul/li/a'):
            listing_url = data.xpath('./@href').get()
            if not listing_url.startswith(self.base_url):
                listing_url = self.base_url.format(listing_url)
            yield scrapy.Request(url=listing_url, callback=self.listing_page, headers=self.headers)

    def listing_page(self, response):
        for data in response.css('div.innertube div.list-one'):
            item = dict()
            item['Business Name'] = data.css('div.lr-title a::text').get('').strip()
            detail_url = data.css('div.lr-title a::attr(href)').get()
            item['Business_Type'] = data.css('div.lr-biztype::text').get('').strip()
            item['Description'] = data.css('div.lr-summary::text').get('').strip()
            item['Phone Number'] = data.css('div.lr-address span::text').get('').strip()
            item['NAICS_Code'] = response.css('input[name="naics"]::attr(value)').get('').strip()
            address = data.xpath('.//div[@class="lr-address"]/text()').get('').strip()
            if address:
                if len(address.split(',')) > 1:
                    item['Street Address'] = address.split(',')[0].strip()
                    state = address.split(',')[-1].strip()
                    if len(state.split(' ')) == 2:
                        item['State_Abrv'] = state.split(' ')[0].strip()
                        item['Zip'] = state.split(' ')[-1].strip()
            if detail_url:
                yield scrapy.Request(url=self.base_url.format(detail_url), meta={'item': item},
                                     callback=self.detail_page, headers=self.headers)
            else:
                item['Source_URL'] = 'https://www.montanabids.us/montana-contractors/category.htm'
                item['Lead_Source'] = 'montanabids'
                item['Occupation'] = 'Contractor'
                item['Record_Type'] = 'Person'
                item['Meta_Description'] = "Contractors in Montana"
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item
        next_page = response.xpath('//a[contains(text(),"Next")]/@href').get()
        if next_page:
            yield scrapy.Request(url=self.base_url.format(next_page),
                                 callback=self.listing_page, headers=self.headers)

    def detail_page(self, response):
        item = response.meta['item']
        fullname = self.get_name_parts(
            response.xpath('//dt[contains(text(),"Contact")]/following-sibling::dd/text()').get('').strip())
        item['Full Name'] = fullname.get('full_name', '')
        item['First Name'] = fullname.get('first_name', '')
        item['Last Name'] = fullname.get('last_name', '')
        item['Detail_Url'] = response.url
        item['Source_URL'] = 'https://www.montanabids.us/montana-contractors/category.htm'
        item['Lead_Source'] = 'montanabids'
        item['Occupation'] = 'Contractor'
        item['Record_Type'] = 'Person'
        item['Meta_Description'] = "Contractors in Montana"
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
