import csv
import json
import math
from collections import OrderedDict
import re
import os
from copy import deepcopy
from urllib.parse import urlsplit, parse_qs, urlencode, urlunsplit

from scrapy import Spider, Request
from datetime import datetime


class ZillowSpider(Spider):
    name = 'zillow'
    scraped_items = []

    custom_settings = {
        'FEED_URI': f'zillow_{datetime.today().date()}.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 2
    }
    previous_addresses = []
    new_addresses = []

    states = [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
        "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
        "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
        "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
        "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
        "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
        "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia",
        "Wisconsin", "Wyoming"
    ]

    headers = {
        'authority': 'www.zillow.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'cookie': 'zguid=24|%24f8607e0b-e0b3-4f2d-92e3-f504adce7781; _ga=GA1.2.697882016.1685127998; zjs_anonymous_id=%22f8607e0b-e0b3-4f2d-92e3-f504adce7781%22; zjs_user_id=null; zg_anonymous_id=%22069f3216-089e-4c11-9582-4e11bc283e4b%22; _pxvid=71a99240-fbf8-11ed-ab21-c66b6a91e1f6; _gcl_au=1.1.1666865800.1685128004; __pdst=4aadf64d7eb745b5a70c2ada343c3ae0; _fbp=fb.1.1685128006074.444767430; _pin_unauth=dWlkPU5EWm1aVEZoTkdNdFkySmlPQzAwTTJaaUxXSTJNekV0TWpJeVpqQTBNR1l3TXpCag; pxcts=e9532382-0551-11ee-ba4f-787145536349; _px3=a0c1114d9e2a74f7d567992f00260d1d7bfc28cab447c69d2ca9973a6866988b:ne48sLzcdgTRZLDYbWaUuFhcAgZfQ3L6h1iiJSZnFg45/g2TUhPKK47R36u8keI3jNKx+/kfIcEBTvAH0TWTKQ==:1000:Aw6WLj8IQTg9PF2OR4J4zGjNYgxYvCYI02F4sAilbdcX9sz17i7L70kxHlCYpCRmksAJFEeYjzKkSckaQYVdz5TiXZ7MA5KI0N8SG9SZvseNaEtHW6dXK85iVhB7K/rKuZ23Tx0zxR7aylvqeX9hpJ89i8R+1Khs3nfKYZ0w3ZADcaoHqM45YDPfEEWjRsDLlAcZU6VPSTrRtcK/AC5OvQ==; JSESSIONID=BF5DB861D7087676A6E9A3DE93F6EE9A; zgsession=1|37eeaeac-c329-43b9-9eb6-5a3bd5b89681; _gid=GA1.2.204789596.1686156013; DoubleClickSession=true; _uetsid=fe2aee30055111ee8e0479c640d0f931; _uetvid=756a3db0fbf811edaed9a9a3eab8c1ab; tfpsi=c4d3810e-7fdd-468d-8ead-60b1d1df57ef; _clck=1hf6yef|2|fc9|0|1241; g_state={"i_p":1686163227012,"i_l":1}; _gat=1; AWSALB=TXktDf/QzOxm/6XOsRqT/xLoXapzqcilG9qoWRaloMJjdw5l+k9u0C8SEs9wVCrQm1eKRd0hKuVS1Glu07N7u+NGIZGuELj5i+pMIkoFA7FGUxUYhSGrMjZ3raS6; AWSALBCORS=TXktDf/QzOxm/6XOsRqT/xLoXapzqcilG9qoWRaloMJjdw5l+k9u0C8SEs9wVCrQm1eKRd0hKuVS1Glu07N7u+NGIZGuELj5i+pMIkoFA7FGUxUYhSGrMjZ3raS6; search=6|1688748559189%7Crect%3D37.30676152825323%252C-79.913158375%252C27.651829593268545%252C-93.448314625%26rid%3D4%26disp%3Dmap%26mdm%3Dauto%26p%3D1%26z%3D1%26listPriceActive%3D1%26fs%3D1%26fr%3D0%26mmm%3D0%26rs%3D0%26ah%3D0%26singlestory%3D0%26housing-connector%3D0%26abo%3D0%26garage%3D0%26pool%3D0%26ac%3D0%26waterfront%3D0%26finished%3D0%26unfinished%3D0%26cityview%3D0%26mountainview%3D0%26parkview%3D0%26waterview%3D0%26hoadata%3D1%26zillow-owned%3D0%263dhome%3D0%26featuredMultiFamilyBuilding%3D0%26commuteMode%3Ddriving%26commuteTimeOfDay%3Dnow%09%094%09%09%09%09%09%09; __gads=ID=32d9f0fdfb3aabaa:T=1685128004:RT=1686156562:S=ALNI_MaEjFkYIhME91jf_5bv_7rGaNHgRw; __gpi=UID=00000c360c7f6f6c:T=1685128004:RT=1686156562:S=ALNI_Mad-V2nQtAivRwnOZMuR_1yU-9U7g; _clsk=1n6tk16|1686156564660|3|0|t.clarity.ms/collect; search=6|1688748892525%7Cregion%3Dal%26rb%3DAL%26rect%3D35.008028%252C-84.888246%252C30.144425%252C-88.473227%26disp%3Dmap%26mdm%3Dauto%26listPriceActive%3D1%26fs%3D1%26fr%3D0%26mmm%3D1%26rs%3D0%26ah%3D0%09%094%09%09%09%09%09%09; AWSALB=MAjfVX/Aqr6MguzYCnW2XCME9yoHASj3AsmBcSntT0zg0Jiq0sN2WqIWWI9BNKNV37vC4vCI/S22NT4Nr/dkWwZzw4/4hAVt0/fJoQHyjpPCfZPhhgzdAN9guVRJ; AWSALBCORS=MAjfVX/Aqr6MguzYCnW2XCME9yoHASj3AsmBcSntT0zg0Jiq0sN2WqIWWI9BNKNV37vC4vCI/S22NT4Nr/dkWwZzw4/4hAVt0/fJoQHyjpPCfZPhhgzdAN9guVRJ',
        # 'referer': 'https://www.zillow.com/homes/Alabama_rb/',
        'sec-ch-ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
    }

    filters = {
        "price": {"min": 150000, "max": 2000000},
        # "sort": {"value": "globalrelevanceex"},
        "fsba": {"value": False}, "fsbo": {"value": False},
        "nc": {"value": False}, "fore": {"value": False},
        "cmsn": {"value": False}, "auc": {"value": False},
        "mf": {"value": False},
        "manu": {"value": False},
        "land": {"value": False},
        "apa": {"value": False},
        "isCondo": {"value": True},
        "isTownhouse": {"value": True},
        "isRecentlySold": {"value": True},
        "isAllHomes": {"value": False},
        "isSingleFamily": {"value": True},
        "isNewConstruction": {"value": False},
        "doz": {"value": "7"}
    }

    def start_requests(self):
        self.get_address_hash()
        url = 'https://www.zillow.com/homes/{}_rb/'
        for price in range(150000, 2000000, 100000):
            start = price
            ends = price + 100000
            for state in self.states:
                yield Request(url.format(state), callback=self.parse, headers=self.headers, dont_filter=True,
                              meta={'state': state, 'start_price': start, 'end_price': ends})

    def parse(self, response, **kwargs):
        data1, data2 = '', ''
        data = response.css('script[data-zrr-shared-data-key="mobileSearchPageStore"]::text').get()
        if data:
            data = data[4:-3]
            data1 = json.loads(data)
            api_url = self.get_api_url(data1, response.meta['start_price'], response.meta['end_price'])
        else:
            data = response.css('script[id="__NEXT_DATA__"]::text').get()
            data2 = json.loads(data)
            api_url = self.get_products_url(data2, response.meta['start_price'], response.meta['end_price'])

        meta = {'page': 1,
                'data1': data1, 'data2': data2,
                'state': response.meta['state'],
                'start_price': response.meta['start_price'],
                'end_price': response.meta['end_price']}
        yield Request(api_url, callback=self.listing_page, headers=self.headers, meta=meta, dont_filter=True)

    def listing_page(self, response):
        data = response.json()
        listResults = data.get('cat1', {}).get('searchResults', {}).get('listResults', [])
        for result in listResults:
            sold_date = result.get('variableData', {})
            sold_date = sold_date.get('text', '') if sold_date else ''
            date = re.findall('\d+', sold_date)
            res = {
                'Address': result.get('address', '').split(',')[0],
                'City': result.get('addressCity', ''),
                'State': result.get('addressState', ''),
                'Zip': result.get('addressZipcode', ''),
                'Sold Price': result.get('soldPrice', ''),
                'Sold Date': '/'.join(date),
                'Source': 'Zillow',
                'Url': result.get('detailUrl', '')
            }
            isExist = self.check_already_scraped(res['Address'], res['City'], res['State'])
            if isExist:
                if res not in self.scraped_items:
                    yield res
                else:
                    self.scraped_items.append(res)

        total_pages = data.get('cat1', {}).get('searchList', {}).get('totalPages', '')
        total_records = data.get('cat1', {}).get('searchList', {}).get('totalResultCount', '')
        pages = math.ceil(int(total_records) / 40)
        meta = response.meta
        if data.get('cat1', {}).get('searchList', {}).get('pagination', {}).get('nextUrl'):
            page = response.meta['page'] + 1
            if page <= pages:
                api_url = ''
                if response.meta['data1']:
                    api_url = self.get_api_url(response.meta['data1'], response.meta['start_price'],
                                               response.meta['end_price'], page=str(page))
                elif response.meta['data2']:
                    api_url = self.get_products_url(response.meta['data2'], response.meta['start_price'],
                                                    response.meta['end_price'], page=str(page))
                meta['page'] = page
                yield Request(api_url, callback=self.listing_page, headers=self.headers,
                              meta=meta)

    def get_products_url(self, data, start_price, end_price, page=None):
        url = 'https://www.zillow.com/search/GetSearchPageState.htm'
        pagination_param = OrderedDict()
        pagination_param['usersSearchTerm'] = data['props']['pageProps']['searchPageState']['queryState'][
            'usersSearchTerm']
        if page:
            pagination_param['pagination'] = {"currentPage": page}
        pagination_param['mapBounds'] = data['props']['pageProps']['searchPageState']['queryState']['mapBounds']
        pagination_param['mapZoom'] = 10
        pagination_param['regionSelection'] = data['props']['pageProps']['searchPageState']['queryState'][
            'regionSelection']
        pagination_param['isMapVisible'] = True
        pagination_param['isListVisible'] = True
        filters = deepcopy(self.filters)
        filters['price'] = {'min': start_price, 'max': end_price}
        pagination_param['filterState'] = filters

        url_params = {'searchQueryState': json.dumps(pagination_param).replace(" ", ""),
                      'wants': '{"cat1":["listResults","mapResults"],"cat2":["total"]}'}

        api_search_url = self.update_query_params(url, url_params)

        return api_search_url

    def update_query_params(self, url, new_params):
        scheme, netloc, path, query_string, fragment = urlsplit(url)
        query_params = parse_qs(query_string)
        query_params.update(new_params)
        new_query_string = urlencode(query_params, doseq=True)
        return urlunsplit((scheme, netloc, path, new_query_string, fragment))

    def get_api_url(self, data, start_price, end_price, page=None):
        url = 'https://www.zillow.com/search/GetSearchPageState.htm'

        pagination_param = OrderedDict()
        pagination_param['usersSearchTerm'] = data.get('queryState', {}).get('usersSearchTerm', '')
        if page:
            pagination_param['pagination'] = {"currentPage": page}
        pagination_param['mapBounds'] = data.get('queryState', {}).get('mapBounds', {})
        pagination_param['mapZoom'] = 10
        pagination_param['regionSelection'] = data.get('queryState', {}).get('regionSelection', [])
        pagination_param['isMapVisible'] = True
        pagination_param['isListVisible'] = True
        filters = deepcopy(self.filters)
        filters['price'] = {'min': start_price, 'max': end_price}
        pagination_param['filterState'] = filters

        url_params = {'searchQueryState': json.dumps(pagination_param).replace(" ", ""),
                      'wants': '{"cat1":["listResults","mapResults"],"cat2":["total"]}'}

        api_search_url = self.update_query_params(url, url_params)

        return api_search_url

    def check_already_scraped(self, address1, city, state):
        address_hash = f'{address1.replace(" ", "_")}-{city}-{state}'
        if address_hash in self.previous_addresses:
            print(f'Address Already Exists: {address_hash}')
            return False
        else:
            self.new_addresses.append(address_hash)
            return True

    def get_address_hash(self):
        path = 'Scraped_Addresses.csv'
        if not os.path.exists(path):
            return
        # else:
        #     with open('Scraped_Addresses.csv','r') as file:
        #         for row in csv.reader(file):
        #             self.previous_addresses.append(row[0])

    def close(spider, reason):
        pass
        # with open('Scraped_Addresses.csv','a',newline='',encoding='utf-8') as file:
        #     writer = csv.writer(file)
        #     for address in spider.new_addresses:
        #         writer.writerow([address,'zillow'])

