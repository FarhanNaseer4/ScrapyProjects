import csv
import json

import scrapy
from scrapy.utils.response import open_in_browser


class ZillowZestimateSpider(scrapy.Spider):
    name = 'zillow_zestimate'
    url = "https://www.zillow.com/homes/{}_rb/"
    details = 'https://www.zillow.com/homedetails/{}/{}_zpid/'

    custom_settings = {
        'FEED_URI': f'output/zillow_estimate_v1.csv',
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

    payload = {}
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': 'x-amz-continuous-deployment-state'
                  '=AYABeOPfH7hajlNjEeclyqAXzrcAPgACAAFEAB1kM2Jsa2Q0azB3azlvai5jbG91ZGZyb250Lm5ldAABRwAVRzA3MjU1NjcyMVRZRFY4RDcyVlpWAAEAAkNEABpDb29raWUAAACAAAAADHLypHpCcfgIyQsuOAAwWtHhS6LMhH7Ry6xy9G4QeYwiiwBR3M8vLi+k9O2BEWNh39s0+dinNapPWTZvqJdwAgAAAAAMAAQAAAAAAAAAAAAAAAAAANL48PWhJ1KbRAPVfSuZ7ZL%2F%2F%2F%2F%2FAAAAAQAAAAAAAAAAAAAAAQAAAAwTz5eKp1iUeNGbdOK4oeUT8OnNnCcS5DY9GER0; zguid=24|%24b999c2ef-8374-4537-86a0-1a492da7862b; _ga=GA1.2.1385128810.1685465736; zjs_anonymous_id=%22b999c2ef-8374-4537-86a0-1a492da7862b%22; zjs_user_id=null; zg_anonymous_id=%2288a3be97-0202-4d23-9f8d-3a6c4184f1ee%22; _gcl_au=1.1.343059458.1685465738; _pxvid=ccb71b94-ff0a-11ed-b012-a83c3a4d90a6; __pdst=fcc02d6b4af14d469de6c206917a90fc; _pin_unauth=dWlkPU16UTBabU0xT0RZdFpHUmlaUzAwT1RJM0xXSXdaR0V0TVdGaE5EZGlPR1ZoWkdGbQ; _cs_c=0; _cs_id=2f1ed063-0405-a7f4-ce84-c0e122a94852.1685946621.1.1685947216.1685946621.1.1720110621790; g_state={"i_p":1689230938243,"i_l":1}; _hp2_id.1215457233=%7B%22userId%22%3A%221363316627635480%22%2C%22pageviewId%22%3A%225048238791896428%22%2C%22sessionId%22%3A%22964850994030659%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _clck=yqh3mq|2|fe3|0|1245; _uetvid=f97ff4707a0811ed9dcb0be38ac62a25; JSESSIONID=F62CC9D40038E950C97E4E5E4ACC2C03; zgsession=1|59865664-2ba6-457e-bedb-e3510a2fa6f6; x-amz-continuous-deployment-state=AYABeI+IcsmGF8voCaEvc0%2FRvnkAPgACAAFEAB1kM2Jsa2Q0azB3azlvai5jbG91ZGZyb250Lm5ldAABRwAVRzA3MjU1NjcyMVRZRFY4RDcyVlpWAAEAAkNEABpDb29raWUAAACAAAAADN64GEm1v%2FABt8+7LgAwNP6KE+3w876EzG%2FrwlpDoodugxE3Qv5UH6PrGjyOYIDN3vEbEe9njxH2fANuuFIOAgAAAAAMAAQAAAAAAAAAAAAAAAAAAJ+6X0ugxCjy5zlForNUQu%2F%2F%2F%2F%2F%2FAAAAAQAAAAAAAAAAAAAAAQAAAAzXJwK3JoqVhpkI0J5wBezGrPySqyKnQ4Cc7NPA; search=6|1695532767587%7Crect%3D35.53476472762745%252C-78.3532503247261%252C35.532232775957844%252C-78.35746675729752%26rid%3D27127%26disp%3Dmap%26mdm%3Dauto%26p%3D1%26sort%3Ddays%26z%3D1%26listPriceActive%3D1%26lt%3Dfsba%252Cfsbo%252Ccmsn%26price%3D200000-%26mp%3D1061-%26fs%3D1%26fr%3D0%26mmm%3D0%26rs%3D0%26ah%3D0%26singlestory%3D0%26housing-connector%3D0%26abo%3D0%26garage%3D0%26pool%3D0%26ac%3D0%26waterfront%3D0%26finished%3D0%26unfinished%3D0%26cityview%3D0%26mountainview%3D0%26parkview%3D0%26waterview%3D0%26hoadata%3D1%26zillow-owned%3D0%263dhome%3D0%26featuredMultiFamilyBuilding%3D0%26commuteMode%3Ddriving%26commuteTimeOfDay%3Dnow%09%0948913%09%7B%22isList%22%3Atrue%2C%22isMap%22%3Atrue%7D%09%09%09%09%09; AWSALB=KbO4CoHQ6TeKVtz7T23aUGGtbI4yk5NdhJyKyLzsi3awXCE4W/CF2vdhVbRAIWZS0y5/J114TahnnaKXbFtVloYOCrvaP4mMzm8eEecm2dwBZDAzN5goOFsjYqo8; AWSALBCORS=KbO4CoHQ6TeKVtz7T23aUGGtbI4yk5NdhJyKyLzsi3awXCE4W/CF2vdhVbRAIWZS0y5/J114TahnnaKXbFtVloYOCrvaP4mMzm8eEecm2dwBZDAzN5goOFsjYqo8',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/116.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    address = '{} {},{} {}'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_keyword = self.get_search_keyword()

    def get_search_keyword(self):
        with open('input/Raleigh_22_2.csv', 'r') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_keyword:
            # street = data.get('ADDRESS', '').strip()
            # city = data.get('CITYNAME', '').strip()
            # state = data.get('STATE', '').strip()
            # zips = data.get('ZIP', '').strip()
            # address = self.address.format(street, city, state, zips)
            address = data.get('Full Property Address', '')
            raw_add = address.replace(' ', '-').replace('#', '-')
            yield scrapy.Request(url=self.url.format(raw_add), meta={'item': data},
                                 dont_filter=True)

    def parse(self, response):
        item = response.meta['item']
        match = response.css('h5.kBlmUa::text').get('')
        # print(match)
        raw_json = response.css('script#__NEXT_DATA__::text').get('')
        # print(raw_json)
        if raw_json:
            json_loaded = json.loads(raw_json)
            data_json1 = json_loaded.get('props', {}).get('pageProps', {}).get('componentProps', {})
            # print(data_json1)
            if data_json1:
                zipd = data_json1.get('zpid', '')
                # print(zipd)
                req_url = response.css('meta[property="og:url"]::attr(content)').get('')
                id_zip = f'{zipd}_zpid'
                # print(id_zip)
                try:
                    data_loaded = data_json1.get('gdpClientCache', '')
                    data_loaded = json.loads(data_loaded)
                    key = 'NotForSaleShopperPlatformFullRenderQuery{"zpid":' + str(zipd) + '}'
                    print(key)
                    property_data = data_loaded[key].get('property', '')
                    # print(property_data)
                    item['Zillow'] = property_data.get('zestimate', '')
                    # item['Rent Zestimate'] = property_data.get('rentZestimate', '')
                    item['Zillow Sqft'] = property_data.get('livingArea', '')
                    item['Zillow Year Built'] = property_data.get('yearBuilt', '')
                    item['Zillow Last Sold Date'] = property_data.get('dateSoldString', '')
                    item['Zillow Sold Price'] = property_data.get('lastSoldPrice', '')
                    yield item
                except Exception as ex:
                    print(ex)
                    # item['Zillow Zestimate'] = ''
                    # item['Rent Zestimate'] = ''
                    yield item
            #     yield response.follow(url=req_url, callback=self.parse_details,
            #                           meta={'id': id_zip,
            #                                 'item': item}, dont_filter=True)
            # if 'No matching' not in match:
            #     data_json = json_loaded.get('props', {}).get('pageProps', {}).get('initialReduxState', {}).get('gdp',
            #                                                                                                    {})
            #     building = data_json.get('building', {})
            #     if building:
            #         ungrouped = building.get('ungroupedUnits', [])
            #         if any(ungrouped):
            #             url = ungrouped[0].get('hdpUrl', '')
            #             zp_id = ungrouped[0].get('zpid', '')
            #             yield response.follow(url=url, callback=self.parse_details,
            #                                   meta={'id': zp_id,
            #                                         'item': item},
            #                                   dont_filter=True)
            #         else:
            #             zpid = building.get('zpid', '')
            #             full_address = building.get('fullAddress', '')
            #             yield response.follow(url=self.details.format(full_address.replace(' ', '-'), zpid),
            #                                   callback=self.parse_details, meta={'id': zpid, 'item': item},
            #                                   dont_filter=True)
        else:
            yield item

    def parse_details(self, response):
        item = response.meta['item']
        zpid = response.meta['id']
        raw_json = response.css('script#__NEXT_DATA__::text').get('').strip()
        print(raw_json)
        # json_loaded = json.loads(raw_json)
        # data_json = json_loaded.get('props', {}).get('pageProps', {}).get('gdpClientCache', '')
        # data_loaded = json.loads(data_json)
        # try:
        #
        #     key = 'NotForSaleShopperPlatformFullRenderQuery{"zpid":' + str(zpid) + '}'
        #     property_data = data_loaded[key].get('property', '')
        #     # print(property_data)
        #     item['Zillow Zestimate'] = property_data.get('zestimate', '')
        #     # item['Rent Zestimate'] = property_data.get('rentZestimate', '')
        #     item['Zillow Sqft'] = property_data.get('livingArea', '')
        #     item['Zillow Year Built'] = property_data.get('yearBuilt', '')
        #     item['Zillow Last Sold Date'] = property_data.get('dateSoldString', '')
        #     item['Zillow Last Sold Price'] = property_data.get('lastSoldPrice', '')
        #     yield item
        # except Exception as ex:
        #     print(ex)
        #     # item['Zillow Zestimate'] = ''
        #     # item['Rent Zestimate'] = ''
        #     yield item
