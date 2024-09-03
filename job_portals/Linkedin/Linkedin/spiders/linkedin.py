import scrapy


class LinkedinSpider(scrapy.Spider):
    name = 'linkedin'
    url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={" \
          "}&location=Canada&geoId=101174742&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&start" \
          "={}"

    previous_jobs = []

    search_terms = ['pharmacists', 'dentists', 'nurses']
    headers = {
        'authority': 'www.linkedin.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.7',
        'cookie': 'bcookie="v=2&a03d4788-be71-4d90-8f72-18325ca7750c"; bscookie="v=1&20230301081115ce701b0a-a840-4b12-866b-f5ffec67d639AQEcBFNJjVVPFrGUujsZoB8p90rHctdW"; fid=AQEY3FYD27aI0AAAAYraSnJ6OxQnipw2Yi4ZHMGeNIhtcO20sdq-I5QXQEk-zTxJ4cIURWwTCJv5uw; JSESSIONID=ajax:5721564687345672122; lang=v=2&lang=en-us; fcookie=AQHzeA43zokO-QAAAYraSsXpuXBfEOAVpL23Cldt-gtR6eGvVklFJAki1wz3D685irLQ-rYMMk3o_dBSmv9Vqg5FBg4z3ZpJGZFZmq-VSKnukbQQxb1yfD3wfHUGAQDaL-b7H_4ZZEmrW3eJTVqVml1rGJqTutosclBhHi2yaC8n9uPkqismSfc0toOuL-uhLGEp0606ZT3VkokBVWOK060iQe7ZzkIs-Uz2OXtkvYGRH4vnur0SLEGIjhJc8CP13N0SggJOzeUBq4TFM/tsOyBimOfzcFz4p78JRmppejk0tI+CQ82hxWJUaZ2f1V/anlbRmhv2mysx/Kaf0w70jRtIawRw==; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; _gcl_au=1.1.39737284.1695879462; lidc="b=TGST08:s=T:r=T:a=T:p=T:g=2602:u=1:x=1:i=1695879464:t=1695965864:v=2:sig=AQGhq9VWCBnBjU4bRuJxJ4cP3waXJ2Ui"; recent_history=AQGj1cwAGJu5VAAAAYrask0FYAEoYGxb7yBGytYUmt_jMTVuVc4gFpOAebhY6aTC09eQZ_OF-XUpqJKRIs-bcFwIlUH8F6-aZy6cj_A2bTTcewm0nUirrKqQiofEH-uZu3U3z49A5snG0_cWQxd3aIWNow8dh6ykFPE0ZVBGlCc7feriMF5ljueGoayuq_4H9yG34kpCSXb8yKgzflimbGxZ2iw8vyXIk7_b1Q0yBd0jbzxbHiXL37p6P-4FIBZ8e2RRVOONNm_JTU1vtukQzjPYtLvFAoucx0QjfXo4XEsc88lklFk2NQRuNyu_tXHirqjYGrI; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19629%7CMCMID%7C45303624366073032019110288314797580132%7CMCOPTOUT-1695893436s%7CNONE%7CvVersion%7C5.1.1; _uetsid=24bcbf905dc111eebaa255eef84cbde0; _uetvid=24bd17105dc111ee90699765bc454db7',
        'csrf-token': 'ajax:5721564687345672122',
        'referer': 'https://www.linkedin.com/jobs/search?keywords=pharmacists&location=Canada&geoId=101174742&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0',
        'sec-ch-ua': '"Brave";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    }
    detail_url = 'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{}?refId={}&trackingId={}'
    custom_settings = {
        # 'FEED_URI': f'output/Linkedin_jobs.csv',
        # 'FEED_FORMAT': 'csv',
        # 'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'ZYTE_SMARTPROXY_ENABLED': False,
        'ZYTE_SMARTPROXY_APIKEY': '8ccded533d7046f69a98663c2979ab8b',  # Todo: Add zyte proxy APIKEY here
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware': 610
        },
        # 'CONCURRENT_REQUESTS': 10,
        'HTTPERROR_ALLOW_ALL': False,
    }

    def start_requests(self):
        for keyword in self.search_terms:
            yield scrapy.Request(url=self.url.format(keyword, 0), headers=self.headers, meta={'keyword': keyword,
                                                                                              'skip': 0})

    def parse(self, response):
        # print(response.text)
        keyword = response.meta['keyword']
        current_skip = response.meta['skip']
        new_jobs = []
        for link in response.css('li div.base-card'):
            item = dict()
            tracking_id = link.css('::attr(data-tracking-id)').get('').strip()
            ref_id = link.css('::attr(data-search-id)').get('').strip()
            id_ = link.css('::attr(data-entity-urn)').get('').strip()
            job_id = id_.split('jobPosting:')[-1].strip()
            item['Job Url'] = link.css('a.base-card__full-link::attr(href)').get('')
            item['Job ID'] = job_id
            item['Tracking ID'] = tracking_id
            item['Ref ID'] = ref_id
            item['Job Title'] = link.css('a.base-card__full-link span.sr-only::text').get('').strip()
            item['Organization'] = link.css('div.base-search-card__info h4 a::text').get('').strip()
            item['Job Location'] = link.css('div div span.job-search-card__location::text').get('').strip()
            item['Posting Date'] = link.css('time.job-search-card__listdate::attr(datetime)').get('').strip()
            item['Start Date'] = link.css('time.job-search-card__listdate::attr(datetime)').get('').strip()
            new_jobs.append(item['Job Url'])
            yield scrapy.Request(url=self.detail_url.format(job_id, ref_id, tracking_id), headers=self.headers,
                                 callback=self.parse_details, meta={'item': item})
        next_skip = current_skip + 25
        if new_jobs not in self.previous_jobs:
            self.previous_jobs.extend(new_jobs)
            yield scrapy.Request(url=self.url.format(keyword, next_skip), headers=self.headers,
                                 meta={'keyword': keyword, 'skip': next_skip}, callback=self.parse)

    def parse_details(self, response):
        item = response.meta['item']
        item['Seniority level'] = response.xpath('//h3[contains(text(),"Seniority '
                                                 'level")]/following-sibling::span/text()').get('').strip()
        item['Employment type'] = response.xpath(
            '//h3[contains(text(),"Employment type")]/following-sibling::span/text()').get('').strip()
        item['Job function'] = response.xpath(
            '//h3[contains(text(),"Job function")]/following-sibling::span/text()').get('').strip()
        item['Industries'] = response.xpath(
            '//h3[contains(text(),"Industries")]/following-sibling::span/text()').get('').strip()
        item['Job Description'] = ''.join(
            response.xpath("//section[@class='show-more-less-html']/div//text()").getall()).strip()
        item['Detail Url'] = response.url
        yield item
