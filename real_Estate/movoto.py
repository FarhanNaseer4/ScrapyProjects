import csv
import json

import scrapy


class MovotoSpider(scrapy.Spider):
    name = 'movoto'
    url = "https://www.movoto.com/api/v/search/?path=address-{}&trigger=mvtHeader&includeAllAddress=true&newGeoSearch=true"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.7',
        'Connection': 'keep-alive',
        'Cookie': '_pxhd=LEQ5tBU0J9OBeNM/hQWN71htL/mnsZ/zT4Tn4jakL16xGcZ6y5SxP4QylTcqX8iEy4ciSvfgaTTHOvopwHkg3A==:kMpVp2b8VCQk6/TrJXsphRGFUtlJOj0Jaaxhf9olnLQVvAIckL05xPzyUzRDPVaevpVocN1/ZJUIvCjFE-702qrSKnWcnRAV3kffOImhym4=; RANDOMID=79; MOVOTODEVICEID=1eb0cbc2-70f0-48a9-b03e-6efb4dd6809d; LASTSEARCHVIEW=gridView; connect.sid=s%3AJPkJvoD_nEh6lOmKJiTJx7aOyA8fZlid.Yfic5b3x62WjGRGnYS6YAecYWOM2JKEOrE5tfYlyQFE; trackingGAID=undefined; pxcts=c40086fa-4236-11ee-b884-7a59686d4957; _pxvid=bf5e105e-4236-11ee-940b-5addb83f32d9; MOVOTOSESSIONID=2856bb99-c398-45d8-a8d4-f76d90a430e8; _px3=1ce623eb59790334f10be5ff67f0e217d804a6a451bc3acf8d351a5099d818f1:8txhFpb4LPkvDE/swCYnC+iQ/5tkYYKyFyISI5JwkS4E4zlMGM6hcLmT9l/1g+orYfBd5qcJNDt3piJol+t0zQ==:1000:pirdda7iHB5NEcumIxkLVvgQva71b30ZDJqsUxDINFXbyR/1/vslmcULT0laNY03ESBUvgUqwt+1NhLZ5h2bV07i+e5PmiZXgRdXWeWq3t7SsbJqGjqpG0amnHZXmvieQh1nbj6pC1UWJDILPRHv+EuwQTb8dB6nHyTWmLFvn59bdKLjuNcdddUKXeiZ+NwqyF6pfbY1VyqUbEHoTbehkg==; nearbyGeoPath=raleigh-nc%2F; _pxhd=LEQ5tBU0J9OBeNM/hQWN71htL/mnsZ/zT4Tn4jakL16xGcZ6y5SxP4QylTcqX8iEy4ciSvfgaTTHOvopwHkg3A==:kMpVp2b8VCQk6/TrJXsphRGFUtlJOj0Jaaxhf9olnLQVvAIckL05xPzyUzRDPVaevpVocN1/ZJUIvCjFE-702qrSKnWcnRAV3kffOImhym4=',
        'Referer': 'https://www.movoto.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-GPC': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Brave";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    base_url = 'https://www.movoto.com/{}'
    check_url = 'https://www.movoto.com/'
    custom_settings = {
        'FEED_URI': f'output/movoto_estimate_x2.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_keyword = self.get_search_keyword()

    def get_search_keyword(self):
        with open('input/x2_8660_leads_CLT-28269.csv', 'r', encoding='utf-8') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for keyword in self.request_keyword:
            address = keyword.get('Full Address', '').strip().replace(',', '').replace(' ', '-')
            yield scrapy.Request(url=self.url.format(address), headers=self.headers, meta={'item': keyword})

    def parse(self, response):
        json_data = json.loads(response.body)
        item = response.meta['item']
        listing = json_data.get('data', {}).get('listings', [])
        if listing:
            path = listing[0].get('path', '')
            if not path.startswith(self.check_url):
                path = self.base_url.format(path)
            yield scrapy.Request(url=path, headers=self.headers,
                                 callback=self.parse_estimate, meta={'item': item})
        else:
            yield item

    def parse_estimate(self, response):
        item = response.meta['item']
        script_json = response.xpath('//script[contains(text(),"window.__INITIAL_STATE__ = ")]/text()').get('').strip()
        if script_json:
            try:
                clean_json = script_json.split('window.__INITIAL_STATE__ = ')[-1].strip().split('window.startTime = new Date();')[0].strip()
                main_json = json.loads(clean_json.rsplit(';')[0])
                page_data = main_json.get('pageData', {}).get('nearbyEstPrice', {})
                item['Movoto'] = page_data.get('estPrice', '')
                yield item
            except Exception as ex:
                yield item
                print('error'+str(ex))
