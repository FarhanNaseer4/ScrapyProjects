import re
from datetime import datetime

import scrapy


class BgchamberSpider(scrapy.Spider):
    name = 'bgchamber'
    start_urls = ['https://cca.bgchamber.com/businesssearch.aspx']

    custom_settings = {
        'FEED_URI': 'bgchamber.csv',
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
        for data in response.css('a.ccaCategoryLink'):
            url = data.css('::attr(href)').get()
            item = dict()
            item['Meta_Description'] = response.xpath('//meta[@name="description" or '
                                                      '@property="og:description"]/@content').get('').strip()
            if url:
                yield response.follow(url=url, callback=self.parse_listing, headers=self.headers, meta={'item': item})

    def parse_listing(self, response):
        item = response.meta['item']
        for data in response.css('div.ccaMemListing a.ccaMemProfileLnk'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.detail_page,
                                      headers=self.headers, meta={'item': item})
        next_page = response.css('a.ccaNext::attr(href)').get()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse_listing,
                                  headers=self.headers, meta={'item': item})

    def detail_page(self, response):
        item = response.meta['item']
        item['Business Name'] = response.css('p.ccaMemName::text').get('').strip()
        item['Business_Site'] = response.css('a.ccaWebAddrLk::text').get('').strip()
        item['Social_Media'] = ', '.join(data.css('::attr(href)').get('') for data in response.css('div.ccaOnlineLinks a'))
        address = response.css('p.ccaAddr::text').getall()
        if address:
            address_details = self.parse_address(' '.join(add.strip() for add in address))
            item['Street Address'] = address_details.get('Street', '')
            item['State'] = address_details.get('State', '')
            item['Zip'] = address_details.get('Zip', '')
        item['Phone Number'] = response.css('a.ccaAddrPhone::text').get('').strip()
        item['Source_URL'] = 'https://cca.bgchamber.com/businesssearch.aspx'
        item['Occupation'] = 'Business Service'
        item['Lead_Source'] = 'bgchamber'
        item['Detail_Url'] = response.url
        item['Record_Type'] = 'Business'
        item['Meta_Description'] = ""
        item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield item

    def parse_address(self, contact_detail):
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

        return {
            'Street': street,
            'State': state,
            'Zip': zip_code
        }

