import json

import scrapy


class MynextmattressSpider(scrapy.Spider):
    name = 'mynextmattress'
    start_urls = ['https://www.mynextmattress.co.uk/brands/spring-craft-beds?product_list_limit=all']
    custom_settings = {
        'FEED_URI': 'mynextmattress.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }
    headers = {
        'authority': 'www.mynextmattress.co.uk',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'cookie': 'wc_visitor=62059-e8847d60-ce0b-c2c6-eb0c-3168c83adf19; wc_client=direct+..+none+..++..++..++..++..+https%3A%2F%2Fwww.mynextmattress.co.uk%2Fspring-craft-venice-bed+..+62059-e8847d60-ce0b-c2c6-eb0c-3168c83adf19+..+; wc_client_current=direct+..+none+..++..++..++..++..+https%3A%2F%2Fwww.mynextmattress.co.uk%2Fspring-craft-venice-bed+..+62059-e8847d60-ce0b-c2c6-eb0c-3168c83adf19+..+; form_key=Vc3lJlY4ZtibRPNd; form_key=Vc3lJlY4ZtibRPNd; 94now_recently_product=MTY3NDU1MzYxOA%3D%3D; mage-cache-storage=%7B%7D; mage-cache-storage-section-invalidation=%7B%7D; mage-cache-sessid=true; hitachi-paybyfinance=%7B%7D; recently_viewed_product=%7B%7D; recently_viewed_product_previous=%7B%7D; recently_compared_product=%7B%7D; recently_compared_product_previous=%7B%7D; product_data_storage=%7B%7D; km_ai=Rjet4dG6D%2FdL7pg%2FA%2FOhdhoCMI8%3D; km_vs=1; checkout-cookie=check; mage-messages=; PHPSESSID=0631bsubidtstl6subr4i65o9e; __utma=214184459.359724906.1674553619.1674553619.1674553619.1; __utmc=214184459; __utmz=214184459.1674553619.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _hjFirstSeen=1; _hjIncludedInSessionSample=0; _hjSession_688236=eyJpZCI6IjJkNWJiOGQwLWQ2NzgtNGNjNi1hYjE1LTM3MTgyYTRmNzk4MiIsImNyZWF0ZWQiOjE2NzQ1NTM2MjEyNDcsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; _ga=GA1.3.359724906.1674553619; _gid=GA1.3.356205658.1674553621; _hjSessionUser_688236=eyJpZCI6IjMyYTI5YzViLWY5NDQtNWRlNS1hNjA1LWIzYjY5MTY4ODhlNSIsImNyZWF0ZWQiOjE2NzQ1NTM2MjExMjMsImV4aXN0aW5nIjp0cnVlfQ==; kvcd=1674554821671; km_lv=1674554822; _uetsid=0bb58d309bcc11ed8ca49771ad221f2a; _uetvid=0bb5d0d09bcc11ed8b4983d96ef07675; __utmb=214184459.15.10.1674553619; private_content_version=f46c91efbdb49ed3a7488669644df9e5; section_data_ids=%7B%7D',
        'referer': 'https://www.mynextmattress.co.uk/brands/spring-craft-beds?product_list_limit=all',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
    }

    def parse(self, response):
        for pro in response.css('a.product-item-photo')[:1]:
            product_url = pro.css('::attr(href)').get('')
            if product_url:
                yield scrapy.Request(url=product_url, callback=self.parse_details, headers=self.headers)

    def parse_details(self, response):
        image_json = response.xpath('//script[contains(text(),"magnifierOpts")]/text()').get('')
        if image_json:
            loaded_json = json.loads(image_json)
            images = loaded_json.get('[data-gallery-role=gallery-placeholder]', {}).get('mage/gallery/gallery', {})
            for data in images.get('data', []):
                print(data.get('full', ''))

