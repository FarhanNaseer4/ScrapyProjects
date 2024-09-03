import re
from datetime import datetime

import scrapy


class RockfordprocesshingeSpider(scrapy.Spider):
    name = 'rockfordprocesshinge'
    start_urls = ['https://rockfordprocesshingesandhardware.com/business-directory/?wpbdp_view=all_listings']
    custom_settings = {
        'FEED_URI': 'rockfordprocesshinge.csv',
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
        for data in response.css('div.wpbdp-listing-excerpt'):
            item = dict()
            item['Business Name'] = data.css('div.listing-title h3 a::text').get('').strip()
            item['Detail_Url'] = data.css('div.listing-title h3 a::attr(href)').get('').strip()
            item['Phone Number'] = data.css('div.wpbdp-field-phone div.value::text').get('').strip()
            item['Business_Site'] = data.css('div.wpbdp-field-website div.value a::attr(href)').get('').strip()
            address = data.css('div.address-info div::text').getall()
            contact_detail = ','.join(add for add in address)
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
            item['Source_URL'] = 'https://rockfordprocesshingesandhardware.com/business-directory/?wpbdp_view' \
                                 '=all_listings'
            item['Lead_Source'] = 'rockfordprocesshingesandhardware'
            item['Meta_Description'] = "SALES REPS / DISTRIBUTORS Rockford Process Control, LLC2020 Seventh St, " \
                                       "Rockford, IL 61104815.966.2000 | sales@rockfordprocess.com LinkedIn Facebook " \
                                       "Contact Us Contact Us If you are human, leave this field blank. Name * " \
                                       "Business Name * Email * Phone I am interested in (Check all that apply) " \
                                       "Hinges Pulls Wire Doors Other Captcha Submit"
            item['Occupation'] = 'Business Service'
            item['Record_Type'] = 'Business'
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield item

        next_page = response.css('span.next a::attr(href)').get()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse, headers=self.headers)
