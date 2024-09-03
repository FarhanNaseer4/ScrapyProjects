import json
from datetime import datetime

import scrapy


class HuntsvilleSpider(scrapy.Spider):
    name = 'huntsville'
    request_api = "https://www.huntsville.org/includes/rest_v2/plugins_listings_listings/find/?json=%7B%22filter%22%3A%7B%22filter_tags%22%3A%7B%22%24in%22%3A%5B%22site_primary_catid_5%22%5D%7D%7D%2C%22options%22%3A%7B%22limit%22%3A12%2C%22skip%22%3A{}%2C%22fields%22%3A%7B%22address1%22%3A1%2C%22amenities%22%3A1%2C%22categories%22%3A1%2C%22city%22%3A1%2C%22crmtracking%22%3A1%2C%22detailURL%22%3A1%2C%22dtn%22%3A1%2C%22isDTN%22%3A1%2C%22loc%22%3A1%2C%22latitude%22%3A1%2C%22longitude%22%3A1%2C%22phone%22%3A1%2C%22primary_category%22%3A1%2C%22primary_image%22%3A1%2C%22primary_image_url%22%3A1%2C%22rankid%22%3A1%2C%22rankorder%22%3A1%2C%22rankname%22%3A1%2C%22recid%22%3A1%2C%22regionid%22%3A1%2C%22state%22%3A1%2C%22social%22%3A1%2C%22title%22%3A1%2C%22zip%22%3A1%7D%2C%22count%22%3Atrue%2C%22sort%22%3A%7B%22rankorder%22%3A1%2C%22sortcompany%22%3A1%7D%7D%7D&token=d057104a28053acf7b46028aa8d4c904"
    custom_settings = {
        'FEED_URI': 'huntsville.csv',
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
        'authority': 'www.huntsville.org',
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cookie': 'GCLB=CMPXt6ePqYLxogE; _gcl_au=1.1.1881725126.1669970641; _gid=GA1.2.875772382.1669970643; __qca=P0-1416096483-1669970641235; __atuvc=3%7C48; __atuvs=6389bad3b022e4ae002; _ga=GA1.2.952275223.1669970643; _ga_Z9MN82YTMQ=GS1.1.1669970643.1.1.1669970696.0.0.0; _ga_FMD72K9TNS=GS1.1.1669970643.1.1.1669970696.7.0.0; _gat_UA-82747010-2=1; _ga_Q3027Z5YJW=GS1.1.1669970644.1.1.1669970707.0.0.0',
        'referer': 'https://www.huntsville.org/restaurants-breweries/all/?skip=24&sort=rankTitle',
        'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    def start_requests(self):
        skip = {'skip': 0}
        yield scrapy.Request(url=self.request_api.format(0), callback=self.parse,
                             headers=self.headers, meta={'skip': skip})

    def parse(self, response):
        skip_item = response.meta['skip']
        json_data = json.loads(response.body)
        for data in json_data.get('docs', {}).get('docs', []):
            item = dict()
            item['Business Name'] = data.get('title', '')
            item['Street Address'] = data.get('address1', '')
            item['State'] = data.get('state', '')
            item['Zip'] = data.get('zip', '')
            item['Phone Number'] = data.get('phone', '')
            detail_url = data.get('absolute_url', '')
            if detail_url:
                item['Detail_Url'] = detail_url
            item['Longitude'] = data.get('longitude', '')
            item['Latitude'] = data.get('latitude', '')
            item['Source_URL'] = 'https://www.huntsville.org/restaurants-breweries/all/'
            item['Lead_Source'] = 'huntsville'
            item['Occupation'] = 'Restaurant'
            item['Record_Type'] = 'Business'
            item['Meta_Description'] = ""
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield item

        total = json_data.get('docs', {}).get('count', '')
        old_skip = skip_item.get('skip', '')
        print(old_skip)
        new_skip = old_skip + 12
        if new_skip <= total:
            skip_item['skip'] = new_skip
            yield scrapy.Request(url=self.request_api.format(new_skip), callback=self.parse,
                                 headers=self.headers, meta={'skip': skip_item})

