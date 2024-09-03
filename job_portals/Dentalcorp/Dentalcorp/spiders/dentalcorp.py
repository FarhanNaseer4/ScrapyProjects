import copy
import json

import scrapy
from scrapy import Selector


class DentalcorpSpider(scrapy.Spider):
    name = "dentalcorp"
    keyword = ['Dentist', 'Dental hygienist', 'Dental assistant']
    url = "https://dentalcorp.wd3.myworkdayjobs.com/wday/cxs/dentalcorp/dentalcorp/jobs"
    job_details = 'https://dentalcorp.wd3.myworkdayjobs.com/wday/cxs/dentalcorp/dentalcorp{}'
    payload = {
        "appliedFacets": {},
        "limit": 20,
        "offset": 0,
        "searchText": "Dentist"
    }
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'en-US',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Cookie': 'PLAY_SESSION=8a53b797b9e8472d766a45dde75460ac0416a765-dentalcorp_pSessionId=fptv3rtfb2e5rejclp6u0iqt8a&instance=wd3prvps0002f; wday_vps_cookie=2737214986.1075.0000; TS014c1515=01f629630493c11897e37f6b374534345a191fb8b87c5d14db0f2efe72069e9e53b298f940e20f985a1a5c0055fa4a942f565d7b15; timezoneOffset=-300; _ga=GA1.4.1927815395.1694583677; wd-browser-id=1736811e-24b6-4895-8c38-3eaab08118a9; CALYPSO_CSRF_TOKEN=495032bc-4355-4df9-9ef6-3df930210dbf; _ga_JZRRQMYMGN=GS1.4.1694583677.1.1.1694583908.0.0.0; PLAY_SESSION=8a53b797b9e8472d766a45dde75460ac0416a765-dentalcorp_pSessionId=fptv3rtfb2e5rejclp6u0iqt8a&instance=wd3prvps0002f; TS014c1515=01f62963043bb281490609e881d4a54eff8e9d24132e6a9e83b7074b733bf7341802a0eb25e7940c40482f4542880e6c9216f93746',
        'Origin': 'https://dentalcorp.wd3.myworkdayjobs.com',
        'Pragma': 'no-cache',
        'Referer': 'https://dentalcorp.wd3.myworkdayjobs.com/dentalcorp?q=Dentist',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'X-CALYPSO-CSRF-TOKEN': '495032bc-4355-4df9-9ef6-3df930210dbf',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    custom_settings = {
        'FEED_URI': f'dentalcorp_data.xlsx',
        'FEED_FORMAT': 'xlsx',
        'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
        'FEED_EXPORT_ENCODING': 'utf-8',
    }

    def start_requests(self):
        for keyword in self.keyword:
            payload = copy.deepcopy(self.payload)
            payload['searchText'] = keyword
            yield scrapy.Request(url=self.url, headers=self.headers, method='POST', body=json.dumps(payload),
                                 meta={'offset': 0,
                                       'keyword': keyword})

    def parse(self, response):
        jsonLoaded = json.loads(response.body)
        offset = response.meta['offset']
        keyword = response.meta['keyword']
        for data in jsonLoaded.get('jobPostings', []):
            job_slug = data.get('externalPath', '')
            yield scrapy.Request(url=self.job_details.format(job_slug), headers=self.headers,
                                 callback=self.parse_details)
        total_data = jsonLoaded.get('total', '')
        if total_data == 0:
            records = response.meta['total']
        else:
            records = total_data
        next_offset = offset + 20
        print(keyword)
        print(records)
        if next_offset <= records:
            payload = copy.deepcopy(self.payload)
            payload['searchText'] = keyword
            payload['offset'] = next_offset
            yield scrapy.Request(url=self.url, headers=self.headers, method='POST', body=json.dumps(payload),
                                 meta={'offset': next_offset,
                                       'keyword': keyword,
                                       'total': records}, callback=self.parse)

    def parse_details(self, response):
        json_loaded = json.loads(response.body)
        job_details = json_loaded.get('jobPostingInfo', {})
        item = dict()
        item['source'] = 'dentalcorp'
        item['Url'] = job_details.get('externalUrl', '')
        item['Title'] = job_details.get('title', '')
        item['postType'] = job_details.get('timeType', '')
        item['organization'] = json_loaded.get('hiringOrganization', {}).get('name', '')
        item['organization Url'] = json_loaded.get('hiringOrganization', {}).get('url', '')
        item['location'] = job_details.get('location', '')
        item['country'] = job_details.get('country', {}).get('descriptor', '')
        item['email'] = ''
        item['phone'] = ''
        item['postedAt'] = job_details.get('postedOn', '')
        item['startDate'] = job_details.get('startDate', '')
        item['endDate'] = ''
        item['jobID'] = job_details.get('jobReqId', '')
        item['canApply'] = job_details.get('canApply', '')
        item['posted'] = job_details.get('posted', '')
        item['applicationUrl'] = job_details.get('externalUrl', '')
        description = job_details.get('jobDescription', '')
        descript = Selector(text=description)
        item['Website'] = descript.xpath('//p/a/@href').get()
        item['Description'] = ''.join(descript.xpath('//p//text()').getall()).strip()
        yield item
