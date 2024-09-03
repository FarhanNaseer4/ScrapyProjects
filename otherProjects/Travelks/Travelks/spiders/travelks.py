import json
from datetime import datetime

import scrapy


class TravelksSpider(scrapy.Spider):
    name = 'travelks'
    request_api = 'https://www.travelks.com/includes/rest_v2/plugins_listings_listings/find/?json=%7B%22filter%22%3A%7B%22%24and%22%3A%5B%7B%22filter_tags%22%3A%7B%22%24in%22%3A%5B%22site_primary_subcatid_{}%22%5D%7D%7D%5D%2C%22recid%22%3A%7B%22%24nin%22%3A%5B525%2C786%5D%7D%7D%2C%22options%22%3A%7B%22limit%22%3A12%2C%22skip%22%3A{}%2C%22count%22%3Atrue%2C%22castDocs%22%3Afalse%2C%22fields%22%3A%7B%22recid%22%3A1%2C%22title%22%3A1%2C%22primary_category%22%3A1%2C%22address1%22%3A1%2C%22city%22%3A1%2C%22url%22%3A1%2C%22isDTN%22%3A1%2C%22latitude%22%3A1%2C%22longitude%22%3A1%2C%22primary_image_url%22%3A1%2C%22qualityScore%22%3A1%2C%22weburl%22%3A1%2C%22dtn.rank%22%3A1%2C%22yelp.rating%22%3A1%2C%22yelp.url%22%3A1%2C%22yelp.review_count%22%3A1%2C%22yelp.price%22%3A1%7D%2C%22hooks%22%3A%5B%5D%2C%22sort%22%3A%7B%22qualityScore%22%3A-1%2C%22sortcompany%22%3A1%7D%7D%7D&token=4d8a02858dfad8e33048fb6ea48dd1a9'
    base_url = 'https://www.travelks.com{}'
    headers = {
        'authority': 'www.travelks.com',
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cookie': 'GCLB=CPfZke-b45yoLw; _scid=bce22cb8-d6c8-4a07-859f-42ba718a25f8; _gid=GA1.2.334634174.1670839132; _sctr=1|1670785200000; _hjFirstSeen=1; _hjIncludedInSessionSample=1; _hjSession_612140=eyJpZCI6ImI2MDBhMDkxLTA3ZDMtNDE4ZS1iMGI3LTExNjQwNGI2MmNkNCIsImNyZWF0ZWQiOjE2NzA4MzkxNDIzMzgsImluU2FtcGxlIjp0cnVlfQ==; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=0; _hjSessionUser_612140=eyJpZCI6IjNhZGE5Y2Y3LTZkY2MtNTlhYy1hMzg1LWY5Y2YxMTA5NDNiNCIsImNyZWF0ZWQiOjE2NzA4MzkxNDIyMDAsImV4aXN0aW5nIjp0cnVlfQ==; __atuvc=4%7C50; __atuvs=6396fb5cd0c1e0c3003; wp35590="WYYCTDDDDDDUMYUTTXC-XUZT-XIHH-BUTU-MACMVTCAUYVZDHYZWXBVB-HKMU-XLBX-HBCX-VYHYBTTKYIBHDmkHOLsrl_JhtDD"; _dc_gtm_UA-17881293-1=1; _uetsid=9523b5e07a0311eda18baf9e58d3a4ed; _uetvid=9523c9007a0311eda5d675af8372be4a; _gat_UA-82747010-2=1; _ga=GA1.2.610132479.1670839132; _ga_Z9MN82YTMQ=GS1.1.1670839131.1.1.1670840722.0.0.0; _ga_PVHFCZV1Z1=GS1.1.1670839131.1.1.1670840722.42.0.0; _ga_Q3027Z5YJW=GS1.1.1670839132.1.1.1670840722.0.0.0',
        'referer': 'https://www.travelks.com/places-to-stay/bed-and-breakfast/',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }
    custom_settings = {
        'FEED_URI': 'travelks.csv',
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

    def start_requests(self):
        for cate in range(1, 12):
            pre_skip = {'p_skip': 0,
                        'cate_id': cate}
            yield scrapy.Request(url=self.request_api.format(cate, 0), callback=self.parse,
                                 headers=self.headers, meta={'pre_skip': pre_skip})

    def parse(self, response):
        try:
            pr_skip = response.meta['pre_skip']
            json_data = json.loads(response.body)
            for data in json_data.get('docs', {}).get('docs', []):
                detail_url = data.get('url', '')
                if detail_url:
                    yield scrapy.Request(url=self.base_url.format(detail_url),
                                         callback=self.parse_detail, headers=self.headers)
            old_skip = pr_skip.get('p_skip', '')
            new_skip = old_skip + 12
            total_data = json_data.get('docs', {}).get('count', '')
            cate_id = pr_skip.get('cate_id', '')
            if new_skip <= int(total_data):
                pr_skip['p_skip'] = new_skip
                yield scrapy.Request(url=self.request_api.format(cate_id, new_skip), callback=self.parse,
                                     headers=self.headers, meta={'pre_skip': pr_skip})
        except Exception as ex:
            print('Error in parser | ' + str(ex))

    def parse_detail(self, response):
        try:
            json_data = json.loads(response.css('script[type="application/ld+json"]::text').get('').strip())
            item = dict()
            item['Business Name'] = json_data.get('name', '')
            item['Phone Number'] = json_data.get('telephone', '')
            item['Street Address'] = json_data.get('address', {}).get('streetAddress', '')
            item['State'] = json_data.get('address', {}).get('addressRegion', '')
            item['Zip'] = json_data.get('address', {}).get('postalCode', '')
            item['Latitude'] = json_data.get('geo', {}).get('latitude', '')
            item['Longitude'] = json_data.get('geo', {}).get('longitude', '')
            item['Source_URL'] = 'https://www.travelks.com/'
            item['Occupation'] = 'Business Service'
            item['Lead_Source'] = 'travelks'
            item['Detail_Url'] = response.url
            item['Record_Type'] = 'Business'
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            item['Meta_Description'] = "Find places to stay in Kansas. Browse listings for details on hotels, motels, " \
                                       "bed and breakfasts, cabins, RV sites, campsites and ranches for your trip."
            yield item
        except Exception as ex:
            print('Error in detail parser | ' + str(ex))
