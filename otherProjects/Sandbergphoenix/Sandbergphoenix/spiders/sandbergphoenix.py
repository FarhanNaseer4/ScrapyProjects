# -*- coding: utf-8 -*-
import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class SandbergphoenixSpider(scrapy.Spider):
    name = 'sandbergphoenix'
    start_urls = ['https://www.sandbergphoenix.com/locations']
    custom_settings = {
        'FEED_URI': 'sandbergphoenix.csv',
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
        'authority': 'www.sandbergphoenix.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/109.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse, headers=self.headers)

    def parse(self, response):
        for data in response.css('div.location-links a'):
            list_url = data.css('::attr(href)').get()
            if list_url:
                yield response.follow(url=list_url, callback=self.parse_listing, headers=self.headers)

    def parse_listing(self, response):
        for data in response.css('a.profile-name'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.parse_details, headers=self.headers)

    def parse_details(self, response):
        item = dict()
        name = response.css('div.bio-splash-title::text').get('').strip()
        if name:
            fullname = self.get_name_parts(name)
            item['Full Name'] = fullname.get('full_name')
            item['First Name'] = fullname.get('first_name')
            item['Last Name'] = fullname.get('last_name')
        item['Phone Number'] = response.css('[href*="tel"] span::text').get('').strip()
        address = response.css('div.bio-splash-position a::text').get('').strip()
        if len(address.split(',')) > 1:
            item['Street Address'] = address.split(',')[0].strip()
            item['State'] = address.split(',')[-1].strip()
        else:
            item['Street Address'] = address
        item['Email'] = response.css('[href*="mailto"] span.hidden-md-down::text').get('').strip()
        item['Description'] = response.css('div.bio-content-content p::text').get('').strip()
        item['Services'] = ' | '.join(service.css('::text').get('') for service in response.css('div.post-related-content a'))
        item['Source_URL'] = 'https://www.sandbergphoenix.com/locations'
        item['Lead_Source'] = 'sandbergphoenix'
        item['Detail_Url'] = response.url
        item['Occupation'] = 'Attorney'
        item['Record_Type'] = 'Person'
        item['Meta_Description'] = "Sandberg Phoenix &amp; Von Gontard P.C.is a law firm with a primary office " \
                                   "located in Downtown St. Louis, Missouri. SPVG is a St. Louis law firm with " \
                                   "experienced St. Louis attorneys and lawyers who provide excellent legal counsel."
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

