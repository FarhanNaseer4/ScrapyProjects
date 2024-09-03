import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class KidderSpider(scrapy.Spider):
    name = 'Kidder'
    request_api = 'https://kidder.com/page/{}/?post_type=professional&s&location&specialty'
    custom_settings = {
        'FEED_URI': 'kidder.csv',
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
        cur_page = {'c_page': 1}
        yield scrapy.Request(url=self.request_api.format(1), callback=self.parse,
                             headers=self.headers, meta={'cur_page': cur_page})

    def parse(self, response):
        current_p = response.meta['cur_page']
        for data in response.css('a.card-pro__link'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield scrapy.Request(url=detail_url, callback=self.parse_detail, headers=self.headers)

        total_record = response.css('span.search-info__numb::text').get('').strip()
        current_page = current_p.get('c_page')
        next_page = current_page + 1
        total_pages = int(int(total_record) / 20) + 1
        if next_page <= int(total_pages):
            current_p['c_page'] = next_page
            yield scrapy.Request(url=self.request_api.format(next_page), callback=self.parse,
                                 headers=self.headers, meta={'cur_page': current_p})

    def parse_detail(self, response):
        item = dict()
        fullname = self.get_name_parts(response.css('h1.mast-pro__title::text').get('').strip())
        item['Full Name'] = fullname.get('full_name', '')
        item['First Name'] = fullname.get('first_name', '')
        item['Last Name'] = fullname.get('last_name', '')
        item['Phone Number'] = response.css('span.is-tel a::text').get('').strip()
        item['Email'] = response.css('a.is-email::text').get('').strip()
        address = response.xpath('//address[@class="intro-pro__address"]/text()').getall()
        contact_detail = ' '.join(add.strip() for add in address)
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
        item['Street Address'] = street
        item['State'] = state
        item['Zip'] = zip_code
        item['Source_URL'] = 'https://kidder.com/?post_type=professional&s=&location=&specialty=#search-anchor'
        occupy = response.css('span.mast-pro__subtitle::text').get('').strip()
        if occupy:
            item['Occupation'] = occupy
        else:
            item['Occupation'] = 'Business Service'
        item['Lead_Source'] = 'kidder'
        item['Detail_Url'] = response.url
        item['Record_Type'] = 'Person'
        item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item['Meta_Description'] = "Because a quality broker is critical to success, we hire, train and support the " \
                                   "best in the business. You get the in-depth knowledge, responsiveness and skills " \
                                   "you need to thrive in commercial real estate. Search our professionals to find " \
                                   "the right person for the job."
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
