import datetime
import json
import math

import scrapy


class RexallSpider(scrapy.Spider):
    name = "rexall"
    url = "https://careers.rexall.ca/api/4.3/companies/158405/career-site/jobs?page_size=50&page_number={}&keyword" \
          "={}&location=0-2&radius=15&locationDescription=Country&locationName=Canada&sort_by=distance" \
          "&sort_order=ASC&country=CA&distance_units=km&custom_categories=Pharmacy"
    keywords = ['Pharmacist', 'Technician and Assistant']
    custom_settings = {
        'FEED_URI': f'rexall_data.xlsx',
        'FEED_FORMAT': 'xlsx',
        'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
        'FEED_EXPORT_ENCODING': 'utf-8',
    }

    base_url = 'https://careers.rexall.ca{}'
    headers = {
        'authority': 'careers.rexall.ca',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'api-key': 'a5a42be583e281bca02ed0a30ef4bca3f046c46d',
        'api-timestamp': '2023-09-08T06:08:28',
        'api-token': 'b2cabf44210eb1f1107eef5589343b4ef81f34b5',
        'content-type': 'application/x-www-form-urlencoded',
        'cookie': 'visid_incap_2866025=tPbxtVvvTSe1uY84755NlBm6+mQAAAAAQUIPAAAAAAAPsPr3bYpZoB7bhhArHjLL; nlbi_2866025=+GfrfbBr5mUPHMLeHHiglwAAAADBQv6uN5dBbEQhMvQXIUtk; incap_ses_1290_2866025=ojKyPuAC8W/OBwDfAgHnERm6+mQAAAAAb+R0W6PI28+i23ePM9ILPQ==; R-GDPR-Applicable=false; R-GDPR-Status=1; GlobalUserGuid=3677fdac-be74-46b7-ae93-35e69bd93361; _ga=GA1.1.923902498.1694153279; _ga_WRMH761DNQ=GS1.1.1694153280.1.0.1694153307.0.0.0; _ga_YRV7ZYGTM6=GS1.1.1694153279.1.1.1694153349.0.0.0; _ga_WN7X9F92J3=GS1.1.1694153319.1.1.1694153350.0.0.0',
        'referer': 'https://careers.rexall.ca/pharmacy-careers?page_size=50&page_number=1&keyword=Technician%20and%20Assistant&location=0-2&radius=15&locationDescription=Country&locationName=Canada&sort_by=distance&sort_order=ASC&country=CA&distance_units=km&custom_categories=Pharmacy',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        for keyword in self.keywords:
            yield scrapy.Request(url=self.url.format(1, keyword), headers=self.headers,
                                 meta={'current': 1, 'keyword': keyword})

    def parse(self, response):
        current_page = response.meta['current']
        keyword = response.meta['keyword']
        json_data = json.loads(response.body)
        for jobs in json_data.get('jobs', []):
            item = dict()
            item['source'] = 'rexall'
            item['url'] = self.base_url.format(jobs.get('job_url', ''))
            item['title'] = jobs.get('headline', '')
            item['shortDescription'] = jobs.get('content_short', '').strip()
            item['postType'] = jobs.get('job_type_name', '')
            item['organization'] = jobs.get('job_details', {}).get('company_name', '')
            location = jobs.get('job_details', {}).get('locations', [])
            if location:
                item['streetAddress'] = location[0].get('street', {})
                item['city'] = location[0].get('city', {})
                item['state'] = location[0].get('state_name', {})
                item['postalCode'] = location[0].get('postalcode', {})
            else:
                item['streetAddress'] = ''
                item['city'] = ''
                item['state'] = ''
                item['postalCode'] = ''
            item['emails'] = ''
            item['phone'] = ''
            posted_date = jobs.get('original_posting_date', '')
            item['postedAt'] = posted_date
            item['dateStart'] = jobs.get('start_date', '')
            item['dateEnd'] = jobs.get('expiration_date', '')
            item['dateClosing'] = jobs.get('expiration_date', '')
            item['speciality'] = ''
            item['subSpeciality'] = ''
            item['jobId'] = jobs.get('job_source_id', '')
            item['applicationUrl'] = jobs.get('custom_apply_url', '')
            item['organizationType'] = jobs.get('industry_name', '')
            posted_in_time = self.is_job_posted_less_than_6_months(posted_date)
            if posted_in_time:
                yield scrapy.Request(url=self.base_url.format(jobs.get('job_url', '')), headers=self.headers,
                                     meta={'item': item}, callback=self.parse_description)

        total_records = json_data.get('numFound', '')
        total_page = math.ceil(total_records / 50)
        next_page = current_page + 1
        if next_page <= total_page:
            yield scrapy.Request(url=self.url.format(next_page, keyword), headers=self.headers,
                                 meta={'current': next_page, 'keyword': keyword})

    def is_job_posted_less_than_6_months(self, posted_date):
        today = datetime.datetime.now()
        posted_date = datetime.datetime.strptime(posted_date, "%Y-%m-%dT%H:%M:%S.%f")
        delta = today - posted_date
        return delta.days <= 180

    def parse_description(self, response):
        item = response.meta['item']
        item['fullDescription'] = ''.join(response.xpath(
            '//div[@class="job-description-content"]//p//text() | //div[@class="job-description-content"]//ul//text()').getall()).strip()
        yield item
