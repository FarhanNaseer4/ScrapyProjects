import csv
import scrapy


class PlusDetailsSpider(scrapy.Spider):
    name = 'plus_details'
    req_url = '{}?PageNumber={}&PageSize=12'
    base_url = 'https://www.plus.nl{}'
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'plus_details.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }
    headers = {
        'authority': 'www.plus.nl',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36 '
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_urls = self.get_search_urls()

    def get_search_urls(self):
        with open('plus_categories.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_urls:
            cate_url = data.get('Category_Url', '')
            item = {'category_url': cate_url,
                    'page_no': 1}
            yield scrapy.Request(url=self.req_url.format(cate_url, '1'), callback=self.parse,
                                 meta={'item': item}, headers=self.headers)

    def parse(self, response):
        item = response.meta['item']
        rank = 1
        for data in response.xpath("//div/li[@class='ish-productList-item']/div"):
            tag_info = data.xpath("./a/div[2]/div[2]//img/@src").get()
            yield {
                'Product_Id': data.xpath("./@data-id").get(),
                'Product_Name': data.xpath("./@data-name").get(),
                'Brand_Name': data.xpath("./@data-brand").get(),
                'Price': data.xpath("./@data-price").get(),
                'Product_Category': data.xpath("./@data-category").get(),
                'Product_url': data.xpath("./a/@href").get(),
                'Image_url': self.base_url.format(data.xpath("./a/div/div[@class='nonbanner-tile_img']/img/@src").get()),
                'Tag': tag_info.split('/')[-1].replace('.png', '') if tag_info else '',
                'Product_Rank': rank,
                'Page_number': item.get('page_no', ''),
                'Page_Url': response.url,
                'Category_url': item.get('category_url', '')
            }
            rank += 1
        next_page = item.get('page_no', '') + 1
        total_product = response.xpath("//div/span/div[@class='total-items-found']/text()").get('0')
        total_page = int(total_product)/12
        if next_page <= total_page:
            item['page_no'] = next_page
            yield scrapy.Request(url=self.req_url.format(item.get('category_url', ''), next_page), callback=self.parse,
                                 meta={'item': item}, headers=self.headers)
