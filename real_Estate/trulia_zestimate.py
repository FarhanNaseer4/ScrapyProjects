import copy
import csv
import json

import scrapy


class TruliaZestimateSpider(scrapy.Spider):
    name = 'trulia_zestimate'
    url = "https://www.trulia.com/graphql?operation_name=WEB_searchBoxAutosuggest&transactionId=client-cd0896d1-6668" \
          "-4830-8283-495505a45a47"
    headers = {
        'authority': 'www.trulia.com',
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'cookie': '_pxhd=Vs1MlYnKd3qS2e2Pi0HInlRn/urwFhKUE87ElMFE4Elzmm-9-5JOZDZRFWdKfRPMlkFD8YUqmNR-B1836Q7R7Q==:jGd32Ylr1RYxAoRTtNBUOKoacom-roKQMMd0q9v4-h1Pve4hNu-mDldoXY3-psabq86ZMEQEbIxPt2DbrCA890O2lNzGww3m4yt37guyY-U=; _csrfSecret=c4DzFSHbqqAIaoc9i%2B3zjy1q; tlftmusr=230927s1mobee4hjmupunj6zbj3sm567; tabc=%7B%221274%22%3A%22control%22%2C%221316%22%3A%22b%22%2C%221334%22%3A%22b%22%2C%221337%22%3A%22b%22%2C%221341%22%3A%22b%22%2C%221353%22%3A%22b%22%2C%221357%22%3A%22b%22%2C%221365%22%3A%22b%22%2C%221371%22%3A%22b%22%2C%221375%22%3A%22b%22%2C%221377%22%3A%22b%22%2C%221379%22%3A%22b%22%2C%221380%22%3A%22b%22%2C%221386%22%3A%22b%22%2C%221393%22%3A%22b%22%2C%221394%22%3A%22b%22%2C%221395%22%3A%22b%22%2C%221401%22%3A%22b%22%2C%221406%22%3A%22b%22%2C%221409%22%3A%22b%22%2C%221418%22%3A%22b%22%2C%221419%22%3A%22control%22%2C%221421%22%3A%22a%22%2C%221425%22%3A%22a%22%2C%221428%22%3A%22a%22%2C%221437%22%3A%22a%22%2C%221438%22%3A%22a%22%2C%221439%22%3A%22d%22%2C%221440%22%3A%22control%22%2C%221444%22%3A%22control%22%2C%221445%22%3A%22a%22%2C%221447%22%3A%22b%22%2C%221453%22%3A%22a%22%2C%221457%22%3A%22a%22%7D; pxcts=04934e1b-5cf2-11ee-83af-1a5c190989f7; _pxvid=fd586369-5cf1-11ee-b5bd-e99347c9e0b2; s_fid=3B8E76E4CE313B23-1A7498741218A3DB; s_cc=true; zjs_user_id=null; zg_anonymous_id=%22d732492f-9ce9-47d6-bd70-1e51c763d11b%22; zjs_anonymous_id=%22230927s1mobee4hjmupunj6zbj3sm567%22; s_vi=[CS]v1|3289DAD464200DB6-400003BA80048078[CE]; _pxff_cfp=1; trul_visitTimer=1695790502964_1695791068247; _px3=47f2b98b4ff5ec51049124d1fb092c7516ae88fea2f31750629b961859af919c:hIM9nWtXazEfgIu5YjnuaMutiTamNiON/wY1krrannwVOWK0qK5RrlY/Hj/32qOfyqctaqsgObOVtPhjG3OH5Q==:1000:RD5ZLv6E7kyY+m4NmE8excMYvraQPIR2lsBKlodAZ+D3ilVA8/rMlcBn/PubltDzKC+/Dinkx2DK3u0KrzYJKf/bXQV87jG0u35EXIB3o3+QdWEiaM5/ch+EExbgPaPL5QNVTqPud3hEv+Et+CrXE0zgRImkdDKiTYF7UKKXl/1DLXcw2XsZ5ZYHBwtX6XZFf1AZ+VPUJ3X5iH4RNDXnV+klDx+tIA0ak+luImQlFvo=; s_sq=truliacom%3D%2526c.%2526a.%2526activitymap.%2526page%253Dhome%25253Adiscovery%25253Abuy%2526link%253DSkip%252520main%252520navigation%252520Buy%252520Rent%252520Mortgage%252520Saved%252520Homes%252520Saved%252520Searches%252520Sign%252520up%252520or%252520Log%252520in%252520Discover%252520a%252520place%252520you%252527ll%252520love%252520to%252520live%252520Buy%252520Ren%2526region%253DBODY%2526pageIDType%253D1%2526.activitymap%2526.a%2526.c%2526pid%253Dhome%25253Adiscovery%25253Abuy%2526pidt%253D1%2526oid%253Dfunctionrg%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DDIV',
        'origin': 'https://www.trulia.com',
        'pragma': 'no-cache',
        'referer': 'https://www.trulia.com/',
        'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-autocomplete-v3-hood': '1',
        'x-region-id-city-search': '1',
        'x-request-id': 'client-e5b2024f-711d-497c-8ecb-3e3c7eb6e58d',
        'z-schools-enabled': '1'
    }
    json_data = {
        'operationName': 'WEB_searchBoxAutosuggest',
        'variables': {
            'query': '1018 Buckhorn Rd  Garner, NC 27529',
            'searchType': 'FOR_SALE',
            'mostRecentSearchLocations': [
                {
                    'cities': {
                        'city': 'Cary',
                        'state': 'NC',
                    },
                },
            ],
        },
        'query': 'query WEB_searchBoxAutosuggest($query: String!, $searchType: SEARCHAUTOCOMPLETE_SearchType, $mostRecentSearchLocations: [SEARCHDETAILS_LocationInput]) {\n  searchLocationSuggestionByQuery(query: $query, searchType: $searchType, mostRecentSearchLocations: $mostRecentSearchLocations) {\n    places {\n        __typename\n        ...on SEARCHAUTOCOMPLETE_Region{ \n          title\n          details\n          searchEncodedHash\n          searchLocation {\n            coordinates {\n              center {\n                latitude\n                longitude\n              }\n            }\n          }\n        }\n        ...on SEARCHAUTOCOMPLETE_Address { \n          title\n          details\n          searchEncodedHash\n          url\n          searchLocation {\n            coordinates {\n              center {\n                latitude\n                longitude\n              }\n            }\n          }\n      }\n    }\n    schools { \n            title\n            subtitle\n            details\n            searchEncodedHash,\n            searchLocation {\n              coordinates {\n                center {\n                  latitude\n                  longitude\n                }\n              }\n            }\n          }\n  }\n}',
    }
    base_url = 'https://www.trulia.com{}'
    custom_settings = {
        'FEED_URI': f'output/Trulia_estimate.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'ZYTE_SMARTPROXY_ENABLED': True,
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
        with open('output/zillow_estimate_v1.csv', 'r') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_keyword:
            street = data.get('Full Property Address', '').strip()
            # city = data.get('PROP_CITY', '').strip()
            # state = data.get('PROP_STATE', '').strip()
            # zips = data.get('PROP_ZIP', '').strip()
            address = street
            payload = copy.deepcopy(self.json_data)
            payload['variables']['query'] = address
            yield scrapy.Request(url=self.url, method='POST', headers=self.headers,
                                 body=json.dumps(payload), meta={'item': data})

    def parse(self, response):
        item = response.meta['item']
        try:
            json_data = json.loads(response.text)
            if json_data:
                json_place = json_data.get('data', {}).get('searchLocationSuggestionByQuery', {}).get('places', [])
                if json_place:
                    url = json_place[0].get('url', '')
                    if url:
                        yield scrapy.Request(url=self.base_url.format(url), headers=self.headers,
                                             callback=self.parse_price, meta={'item': item})
                    else:
                        yield item
                else:
                    yield item
            else:
                yield item
        except Exception as ex:
            print(ex)
            yield item

    def parse_price(self, response):
        item = response.meta['item']
        item['Trulia'] = response.css('div[data-testid="home-details-sm-lg-xl-price-details"] h3 div::text').get('').strip()
        yield item
