# -*- coding: utf-8 -*-
import copy
import csv
import json
import re
from datetime import datetime

import scrapy
from nameparser import HumanName


class FentonmochamberSpider(scrapy.Spider):
    name = 'fentonmochamber'
    request_api = 'https://www.chamberorganizer.com/members/directory/search_bootstrap_ajax.php?org_id=FENT'
    custom_settings = {
        'FEED_URI': 'fentonmochamber.csv',
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
    data = {
        'buttonNum': 'B1',
        'searchValues[bci]': '0',
        'searchValues[rtk]': '',
        'searchValues[order]': '',
        'searchValues[keyword]': '',
        'searchValues[business_name]': '',
        'searchValues[city_name]': '',
        'searchValues[zip_code]': '36117',
        'searchValues[zip_range]': '100',
        'searchValues[latitude]': '',
        'searchValues[longitude]': '',
        'searchValues[sgid]': '',
        'searchValues[mdc_vendor]': '',
        'searchValues[glac_welcome]': '',
        'searchValues[exp_type]': '',
        'searchValues[favis]': '',
        'searchValues[ataaCert]': '',
        'searchValues[busi]': '',
        'searchValues[pre]': '',
        'searchValues[nc]': '',
        'searchValues[ofc]': '',
        'searchValues[aoppspons]': '',
        'searchValues[texobroker]': '',
        'searchValues[istalabs]': '',
        'searchValues[aahs_legis]': '',
        'searchValues[aahs_trials]': '',
        'searchValues[aahs_research]': '',
        'searchValues[aahs_support]': '',
        'searchValues[adsm_mem_dir]': '',
        'searchValues[aliv_vep_dir]': '',
        'searchValues[brch_quincy]': '',
        'searchValues[iapr_mem_dir]': '',
        'searchValues[boma_mem_dir]': '',
        'searchValues[ensemble]': '0',
        'searchValues[ntrc_residential]': '',
        'searchValues[ntrc_commercial]': '',
        'searchValues[ntrc_associate]': '',
        'searchValues[txca_speaker]': '',
        'searchValues[txca_therapist]': '',
        'searchValues[nyed_pub]': '',
        'searchValues[wisp_mem_dir]': '',
        'searchValues[wisp_vend]': '',
        'searchValues[sfla_cert]': '',
        'searchValues[lcda_mem_dir]': '',
        'searchValues[gscc_mem_dir]': '',
        'searchValues[ohsa_county]': '',
        'searchValues[alp_mem_dir]': '',
        'searchValues[wspa_pro_bono]': '',
        'searchValues[sbpps_id]': '',
        'searchValues[view_all_override]': '',
        'two_column': 'X',
        'sameSearch': '1',
    }
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://www.chamberorganizer.com',
        'Referer': 'https://www.chamberorganizer.com/members/directory/search_bootstrap.php?twocol&org_id=FENT',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Cookie': 'PHPSESSID=d6c8137c522266f3e29e375020b996e1'
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_zip = self.get_search_zip()

    def get_search_zip(self):
        with open('33_states_cities_zip.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for zip_code in self.request_zip:
            payload = copy.deepcopy(self.data)
            search_data = {'search_zip': zip_code.get('zipcode')}
            payload['searchValues[zip_code]'] = zip_code.get('zipcode')
            yield scrapy.FormRequest(url=self.request_api, callback=self.parse,
                                     headers=self.headers, method='POST', formdata=payload,
                                     meta={'search': search_data})

    def parse(self, response):
        search_data = response.meta['search']
        for data in response.css('div.search-result div.member-info'):
            item = dict()
            item['Business Name'] = data.css('.name-plate::text').get('').strip()
            name = data.css('.second-line-name::text').get('').strip()
            if name:
                fullname = self.get_name_parts(name)
                item['Full Name'] = fullname.get('full_name')
                item['First Name'] = fullname.get('first_name')
                item['Last Name'] = fullname.get('last_name')
            contact_details = ','.join(add.css('::text').get('') for add in data.css('.address-block div'))
            if contact_details:
                address = self.get_address_parts(contact_details)
                item['Street Address'] = address.get('street')
                item['State'] = address.get('state')
                item['Zip'] = address.get('zip')
            item['Phone Number'] = data.css('.phone-number::text').get('').replace('Primary Phone:', '').strip()
            item['Business_Site'] = data.css('.more-info div a::attr(href)').get('').strip()
            item['Source_URL'] = 'https://www.fentonmochamber.com/directory.html'
            item['Lead_Source'] = 'fentonmochamber'
            item['Occupation'] = 'Business Service'
            item['Record_Type'] = 'Business'
            item['Meta_Description'] = "The Fenton Area Chamber of Commerce supports, promotes & encourages shopping " \
                                       "local and keeping business in Fenton. The organization also serves as a link " \
                                       "between our members and the community"
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield item

        next_button = response.css('div.next-page::attr(data-buttonnum)').get('').strip()
        if next_button:
            payload = copy.deepcopy(self.data)
            payload['searchValues[zip_code]'] = search_data.get('search_zip')
            payload['buttonNum'] = next_button
            yield scrapy.FormRequest(url=self.request_api, callback=self.parse,
                                     headers=self.headers, method='POST', formdata=payload,
                                     meta={'search': search_data})

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

    def get_address_parts(self, contact_detail):
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
            'street': street,
            'state': state,
            'zip': zip_code
        }
