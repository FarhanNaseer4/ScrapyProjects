import csv
import json
from datetime import datetime

from scrapy_scrapingbee import ScrapingBeeSpider, ScrapingBeeRequest


class ReweDetailsSpider(ScrapingBeeSpider):
    name = 'rewe_details'
    base_url = 'http://shop.rewe.de{}'
    request_api = 'https://shop.rewe.de/api/products?objectsPerPage=40&page={' \
                  '}&search=%2A&sorting=RELEVANCE_DESC&categorySlug={}&serviceTypes=DELIVERY&market=320509&debug=false '
    custom_settings = {
        'FEED_URI': 'rewe_details.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
    }
    headers = {
        'authority': 'shop.rewe.de',
        'accept': 'application/vnd.rewe.productlist+json',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36 ',
    }

    def __init__(self, *args):
        super().__init__(*args)
        self.request_urls = self.get_search_urls()

    def get_search_urls(self):
        with open('rewe_categories.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_urls:
            cate_slug = data.get('Category_Url', '').split('/')[-2]
            item = {'category_url': data.get('Category_Url', '')}
            yield ScrapingBeeRequest(url=self.request_api.format('1', cate_slug), headers=self.headers,
                                     meta={'item': item}, callback=self.parse)

    def parse(self, response):
        result = json.loads(response.body)
        item = response.meta['item']
        rank = 1
        current_page = result.get('pagination', {}).get('page', '')
        for data in result.get('_embedded', {}).get('products', []):
            pro_url = data.get('_links', {}).get('detail', {}).get('href', '')
            if not pro_url.startswith(self.base_url):
                pro_url = self.base_url.format(pro_url)
            price_data = data.get('_embedded', {}).get('articles', [])
            yield {
                'Product_Id': data.get('id', ''),
                'Product_Name': data.get('productName', ''),
                'Brand_Name': data.get('brand', {}).get('name', ''),
                'Image_url': ', '.join(img.get('_links', {}).get('self', {}).get('href', '')
                                       for img in data.get('media', {}).get('images', [])),
                'Product_Weight': price_data[0].get('_embedded', {}).get('listing', {}).get('pricing', {}).get(
                    'grammage', '') if price_data[0] else '',
                'Current_price': price_data[0].get('_embedded', {}).get('listing', {}).get('pricing', {}).get(
                    'currentRetailPrice', '') if price_data[0] else '',
                'Base_Price': price_data[0].get('_embedded', {}).get('listing', {}).get('pricing', {}).get(
                    'basePrice','') if price_data[0] else '',
                'Product_url': pro_url,
                'Category_url': item.get('category_url', ''),
                'Product_Rank': rank,
                'Page_No': current_page,
                'TimeStamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            rank += 1
        total_page = result.get('pagination', {}).get('totalPages', '')
        next_page = current_page + 1
        if next_page <= total_page:
            yield ScrapingBeeRequest(self.request_api.format(next_page, item.get('category_url', '').split('/')[-2]),
                                     headers=self.headers, meta={'item': item}, callback=self.parse)


