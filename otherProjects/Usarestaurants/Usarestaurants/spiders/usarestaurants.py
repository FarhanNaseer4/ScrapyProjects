import json
from datetime import datetime

import scrapy


class UsarestaurantsSpider(scrapy.Spider):
    name = 'usarestaurants'
    start_urls = ['https://usarestaurants.info/explore/united-states']
    custom_settings = {
        'FEED_URI': 'usarestaurants.csv',
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
        for data in response.css('span.localityLink a'):
            state_url = data.css('::attr(href)').get()
            if state_url:
                yield response.follow(url=state_url, callback=self.get_city, headers=self.headers)

    def get_city(self, response):
        for data in response.css('span.localityLink a'):
            city_url = data.css('::attr(href)').get()
            if city_url:
                yield response.follow(url=city_url, callback=self.get_listing, headers=self.headers)

    def get_listing(self, response):
        for data in response.css('div.places-list h3 a'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.get_details, headers=self.headers)

        next_page = response.css('a.next::attr(href)').get()
        if next_page:
            yield response.follow(url=next_page, callback=self.get_listing, headers=self.headers)

    def get_details(self, response):
        try:
            json_data = response.xpath('//script[@type="application/ld+json"][1]/text()').get()
            if json_data:
                json_result = json.loads(json_data)
                item = dict()
                item['Business Name'] = json_result.get('name', '')
                item['Phone Number'] = json_result.get('telephone', '')
                item['Street Address'] = json_result.get('address', {}).get('streetAddress', '')
                item['State'] = json_result.get('address', {}).get('addressRegion', '')
                item['Business_Site'] = response.xpath('//th[contains(text(), "Website")]/following-sibling::td/a/@href').get('').strip()
                item['Zip'] = json_result.get('address', {}).get('postalCode', '')
                item['Latitude'] = json_result.get('geo', {}).get('latitude', '')
                item['Longitude'] = json_result.get('geo', {}).get('longitude', '')
                item['Source_URL'] = 'https://usarestaurants.info/explore/united-states'
                item['Occupation'] = 'Restaurant'
                item['Lead_Source'] = 'usarestaurants'
                item['Meta_Description'] = "Explore businesses and places  in United states. Access business " \
                                           "information, phone numbers, and more - usarestaurants.info"
                item['Detail_Url'] = response.url
                item['Record_Type'] = 'Business'
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item

        except Exception as ex:
            print('Error in getting details | ' + str(ex))

