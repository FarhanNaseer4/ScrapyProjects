# -*- coding: utf-8 -*-
import scrapy


class MhraSpider(scrapy.Spider):
    name = 'mhra'
    start_urls = ['https://aic.mhra.gov.uk/era/pdr.nsf/name?openpage&start=1&count=200']
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/109.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
    }
    custom_settings = {
        'FEED_URI': 'mhra_gov.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse, headers=self.headers)

    def parse(self, response):
        for page in response.css('div.textbox a'):
            page_url = page.css('::attr(href)').get()
            if page_url:
                yield response.follow(url=page_url, callback=self.parse_data, headers=self.headers, dont_filter=True)

    def parse_data(self, response):
        for data in response.xpath('//table[@class="workspace"]//tr/following::tr'):
            item = dict()
            item['Manufacturer'] = data.xpath('./td[1]/text()').get('').strip()
            address = data.xpath('./td[2]/text()').getall()
            item['Email'] = ''.join(email for email in address if '@' in email)
            item['Address'] = ','.join(add for add in address if '@' not in add)

            item['Authorised Representative'] = data.xpath('./td[3]/text()').get('').strip()
            item['Relationship'] = data.xpath('./td[4]/text()').get('').strip()
            address_2 = data.xpath('./td[5]/text()').getall()
            check_address = ','.join(add for add in address_2 if add)
            if len(check_address.split(',')) > 2:
                item['Address 2'] = check_address
            else:
                item['Address 2'] = ''
            item['Date Registered'] = data.xpath('./td[6]/text()').get('').strip()
            item['MHRA Reference Number'] = data.xpath('./td[7]/text()').get('').strip()
            item['Devices'] = ' | '.join(device.xpath('./text()').get('').replace(':', '').strip() for device in data.xpath('./td[8]/a/ul/li'))
            yield item
