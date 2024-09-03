# -*- coding: utf-8 -*-
import json

import scrapy


class PageLibrarySpider(scrapy.Spider):
    name = 'page_library'
    request_api = 'https://www.facebook.com/ads/library/async/search_ads/?q=social%20media&forward_cursor={' \
                  '}&collation_token=b975c229-5868-4a05-9979-13510fb33e23&count=20&active_status=all&ad_type=all' \
                  '&countries\[0\]=PK&media_type=all&search_type=keyword_unordered'
    first_forward = 'AQHR7AZv1UqifmUtE0uNNgfBVTlXD8_sl6LZUrSC5yyI1WmVD62YcmW_I1uzENhBx9Iy'
    payload = '__user=0&__a=1&__dyn=7xeUmxa3-Q8zo5ObwKBWobVo9E4a2i5U4e1FxebzEdF8aUuxa1ZzES2S2q0_EtxG4o3Bw5VCyU4a0OE2WxO2O1Vwooa8465o-cw5MKdwGwQwoE2LwBgao884y0Mo5Wm588Egze0z8-U6-3e4Ueo2sxOu2S2W2K7o725U4q0HUkyE9E11EbodEGdw46wbLwiU8U6C2-1qwNwAwQw&__csr=&__req=b&__hs=19399.BP%3ADEFAULT.2.0.0.0.0&dpr=1.5&__ccg=GOOD&__rev=1006948470&__s=leonqz%3A9to431%3Av0xbla&__hsi=7198790626268227176&__comet_req=0&lsd=AVpCwlZ3TFM&jazoest=2929&__spin_r=1006948470&__spin_b=trunk&__spin_t=1676099055&__jssesw=1'
    headers = {
        'authority': 'www.facebook.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.8',
        'content-type': 'application/x-www-form-urlencoded',
        'cookie': 'sb=rfmJY19wScTx1lphjemqE9E7; datr=rfmJY5vllGWouUFKgWK7Dbqp; m_ls=%7B%22c%22%3A%7B%221%22%3A%22HCwAABaOlQsW5sennAoTBBaWibCU88AtAA%22%2C%222%22%3A%22GSwVQBxMAAAWABa6j9G4DBYAABV-HEwAABYAFsyP0bgMFgAAFhoA%22%2C%225%22%3A%22GSwVNBxMAAAWABbOj9G4DBYAABWAARxMAAAWABbOj9G4DBYAABYaAA%22%2C%2216%22%3A%22FQQVCBmcFQQVDCbOj9G4DBYAABUEFTomzo_RuAwWAAAVBBU8Js6P0bgMFgAAFQQVUibOj9G4DBYAABUEFWAmzo_RuAwWAAAVBBViJs6P0bgMFgAAFQQVaibOj9G4DBYAABUEFW4mzo_RuAwWAAAVBBV0Js6P0bgMFgAAFtIJEQA%22%2C%2226%22%3A%22dummy_cursor%22%2C%2228%22%3A%221669997541%22%7D%2C%22d%22%3A%22ad70d545-f298-447a-a756-89019d81fd67%22%2C%22s%22%3A%220%22%7D; fr=0LgK4jzu4uEGxRdy6.AWX7gb6XQUURohJHhg2BNiLxXTw.Bjifmt.Gx.AAA.0.0.Bjikyk.AWXwiKanll4; dpr=1.25; usida=eyJ2ZXIiOjEsImlkIjoiQXJwd2x0c2JrcHVyaSIsInRpbWUiOjE2NzYwOTg0MzJ9; wd=1536x286',
        'origin': 'https://www.facebook.com',
        'referer': 'https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=PK&q=social%20media&search_type=keyword_unordered&media_type=all',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Brave";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'x-fb-lsd': 'AVpCwlZ3TFM'
    }

    def start_requests(self):
        yield scrapy.Request(url=self.request_api.format(self.first_forward),
                             callback=self.parse, method='POST', body=self.payload, headers=self.headers)

    def parse(self, response):
        json_data = '{' + response.text.split('{', 1)[-1]
        loaded_json = json.loads(json_data)
        print(loaded_json)
        pages_data = loaded_json.get('payload', {}).get('results', [])
        for page in pages_data:
            item = dict()
            # print(page)
        forward_cursor = loaded_json.get('payload', {}).get('forwardCursor', '')
        if forward_cursor:
            print(self.first_forward)
            yield scrapy.Request(url=self.request_api.format(forward_cursor),
                             callback=self.parse, method='POST', body=self.payload, headers=self.headers)
