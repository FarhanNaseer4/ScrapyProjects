import csv
import json
from datetime import date

import scrapy


class RedfinEstimateSpider(scrapy.Spider):
    name = 'redfin_estimate'
    url = "https://www.redfin.com/stingray/do/query-location?al=1&market=richmond&num_homes=1000&ooa=true&v=2" \
          "&location={}"
    base_url = 'https://www.redfin.com{}'
    check_url = 'https://www.redfin.com'
    headers = {
        'authority': 'www.redfin.com',
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        'cookie': 'RF_CORVAIR_LAST_VERSION=475.2.2; RF_BROWSER_ID=zPukQz4KTROyJOsjn1uxhw; RF_BROWSER_ID_GREAT_FIRST_VISIT_TIMESTAMP=2023-06-18T23%3A15%3A32.903427; RF_BID_UPDATED=1; usprivacy=1---; OneTrustWPCCPAGoogleOptOut=false; RF_BROWSER_CAPABILITIES=%7B%22screen-size%22%3A4%2C%22events-touch%22%3Afalse%2C%22ios-app-store%22%3Afalse%2C%22google-play-store%22%3Afalse%2C%22ios-web-view%22%3Afalse%2C%22android-web-view%22%3Afalse%7D; RF_LAST_NAV=1; audS=t; FEED_COUNT=%5B%22%22%2C%22f%22%5D; _gcl_au=1.1.30455625.1687155387; _rdt_uuid=1687155387504.d9d6f1c6-8e04-4069-8d4a-f43e6eb8d629; __pdst=45fc579d9ef24a8cba643b74a51d2160; _scid=5fc6ceba-91fe-4579-888f-a814462ce210; _tt_enable_cookie=1; _ttp=1JVFxCvstyHhqFjmOlJ8b19baip; AMP_TOKEN=%24NOT_FOUND; _gid=GA1.2.873626205.1687155390; _dc_gtm_UA-294985-1=1; _pin_unauth=dWlkPU16UTBabU0xT0RZdFpHUmlaUzAwT1RJM0xXSXdaR0V0TVdGaE5EZGlPR1ZoWkdGbQ; unifiedLastSearch=name%3D3124%2520Newington%2520Ct%26subName%3DNorth%2520Chesterfield%252C%2520VA%252C%2520USA%26url%3D%252FVA%252FNorth-Chesterfield%252F3124-Newington-Ct-23224%252Fhome%252F59549837%26id%3D1_59549837%26type%3D1%26unifiedSearchType%3D1%26isSavedSearch%3D%26countryCode%3DUS; RF_VISITED=true; RF_MARKET=richmond; RF_BUSINESS_MARKET=48; ki_t=1687155395528%3B1687155395528%3B1687155395528%3B1%3B1; ki_r=; RF_LISTING_VIEWS=112808258; RF_LAST_DP_SERVICE_REGION=9205; _ga_P8GPVZXD5S=GS1.1.1687155398.1.1.1687155419.0.0.0; OptanonAlertBoxClosed=2023-06-19T06:17:00.763Z; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Jun+19+2023+11%3A17%3A01+GMT%2B0500+(Pakistan+Standard+Time)&version=202304.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=b285bb84-d79f-4eeb-a692-d3442c870610&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CSPD_BG%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=PK%3BPB; _scid_r=5fc6ceba-91fe-4579-888f-a814462ce210; _uetsid=d33007900e6811ee8df1c350763410b5; _uetvid=f5fe92203ef211ed86648b975295c8e8; _ga=GA1.2.2085853198.1687155388; _ga_928P0PZ00X=GS1.1.1687155387.1.1.1687155435.12.0.0',
        'pragma': 'no-cache',
        'referer': 'https://www.redfin.com/',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    custom_settings = {
        'FEED_URI': f'output/redfin_estimate_v1.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'ZYTE_SMARTPROXY_ENABLED': False,
        'ZYTE_SMARTPROXY_APIKEY': '238915a8aafb45d08d7fa88e7d716284',  # Todo: Add zyte proxy APIKEY here
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware': 610
        },
        # 'CONCURRENT_REQUESTS': 10,
        'HTTPERROR_ALLOW_ALL': False,
    }
    address = '{} {},{} {}'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_keyword = self.get_search_keyword()

    def get_search_keyword(self):
        with open('output/realtor_estimate_v1.csv', 'r', encoding='utf-8') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_keyword:
            street = data.get('Full Property Address', '').strip()
            # city = data.get('PROP_CITY', '').strip()
            # state = data.get('PROP_STATE', '').strip()
            # zips = data.get('PROP_ZIP', '').strip()
            # address = self.address.format(street, city, state, zips)
            yield scrapy.Request(url=self.url.format(street), headers=self.headers,
                                 meta={'item': data})

    def parse(self, response):
        item = response.meta['item']
        json_data = json.loads(response.text.replace('{}&&', ''))
        exact_match = json_data.get('payload', {}).get('exactMatch', {})
        detail_url = exact_match.get('url', '')
        if not detail_url.startswith(self.check_url):
            detail_url = self.base_url.format(detail_url)
        yield scrapy.Request(url=detail_url, callback=self.parse_details, headers=self.headers,
                             meta={'item': item})

    def parse_details(self, response):
        print(response.text)
        item = response.meta['item']
        item['Redfin'] = response.css('div[data-rf-test-id="abp-price"] div.statsValue span::text').get('').strip()
        yield item
