import csv
import json

import scrapy


class StockxSpider(scrapy.Spider):
    name = 'stockx'
    start_urls = ['http://stockx.com/']
    search_url = "https://stockx.com/search?s={}"
    custom_settings = {
        'ZYTE_SMARTPROXY_ENABLED': True,
        'ZYTE_SMARTPROXY_APIKEY': 'Zyte_key',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware': 610
        },
        'FEED_URI': f'stockx_output.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'FEED_EXPORT_FIELDS': ['product_url', 'product_title', 'brand', 'categories', 'condition', 'description',
                               'image_url', 'Style', 'Colorway', 'Retail Price', 'Release Date',
                               'Included Accessories', 'Featured', 'Source', 'Uploaded By', 'size', 'price',
                               'discount', 'discount_percent', 'UPC', 'search_keyword']
    }

    def start_requests(self):
        input_data = self.get_input_from_file()
        for input_criteria in input_data:
            keyword = input_criteria.get('search_keyword')
            yield scrapy.Request(url=self.search_url.format(keyword), callback=self.parse,
                                 meta={'keyword': keyword})

    def parse(self, response):
        keyword = response.meta.get('keyword')
        products_links = response.css('#browse-grid [data-testid="RouterSwitcherLink"]::attr(href)').getall()
        for each_product in products_links:
            yield response.follow(url=each_product, callback=self.parse_products, meta={'keyword': keyword})
        next = response.css('a[aria-label="Next"]::attr(href)').get('')
        if next:
            yield response.follow(url=next, callback=self.parse, meta={'keyword': keyword})

    def parse_products(self, response):
        keyword = response.meta.get('keyword')
        script = json.loads(response.css('script#__NEXT_DATA__::text').get(''))
        product_data = script.get('props').get('pageProps').get('req').get('appContext').get('states').get('query').get('value').get('queries')
        product_dict = {'data': get_data.get('state').get('data').get('product') for get_data in product_data if 'GetProduct' in json.loads(get_data.get('queryHash'))[0]}
        item = dict()
        item['product_url'] = response.url
        item['product_title'] = product_dict.get('data').get('title')
        item['brand'] = product_dict.get('data').get('brand')
        item['categories'] = ' / '.join([i.get('name') for i in product_dict.get('data').get('breadcrumbs', [])])
        item['condition'] = product_dict.get('data').get('condition')
        item['description'] = product_dict.get('data').get('description')
        item['image_url'] = product_dict.get('data').get('media').get('imageUrl')
        for trait in product_dict.get('data').get('traits'):
            item[trait.get('name')] = trait.get('value')
        for variant in product_dict.get('data').get('variants'):
            item['size'] = f"{variant.get('sizeChart').get('baseType').upper()} {variant.get('sizeChart').get('baseSize')}"
            item['price'] = variant.get('market').get('statistics').get('lastSale').get('amount')
            item['discount'] = variant.get('market').get('statistics').get('lastSale').get('changeValue')
            item['discount_percent'] = variant.get('market').get('statistics').get('lastSale').get('changePercentage') * 100 \
                if variant.get('market').get('statistics').get('lastSale').get('changePercentage') else ''
            if variant.get('gtins'):
                for i in variant.get('gtins'):
                    item[i.get('type')] = i.get('identifier')
            item['search_keyword'] = keyword
            yield item

    def get_input_from_file(self):
        with open(file='stockx_search_input.csv', mode='r', encoding='utf-8') as input_file:
            return list(csv.DictReader(input_file))





