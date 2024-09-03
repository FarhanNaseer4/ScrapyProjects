from scrapy_scrapingbee import ScrapingBeeSpider, ScrapingBeeRequest


class ReweCategorySpider(ScrapingBeeSpider):
    name = 'rewe_category'
    request_url = 'https://shop.rewe.de/'
    base_url = 'https://shop.rewe.de{}'
    custom_settings = {
        'FEED_URI': 'rewe_categories.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
    }
    headers = {
        'authority': 'shop.rewe.de',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36 ',

    }

    def start_requests(self):
        yield ScrapingBeeRequest(self.request_url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        for data in response.css(".home-page-categories div a"):
            category_url = data.css("::attr(href)").get()
            if not category_url.startswith(self.base_url):
                category_url = self.base_url.format(category_url)
            item = {'cate_main': category_url}
            yield ScrapingBeeRequest(category_url, headers=self.headers,
                                     meta={'item': item}, callback=self.scrap_subcategories)

    def scrap_subcategories(self, response):
        item = response.meta['item']
        for data in response.css(".NavFacetGroup_navFacetGroupList__vreDw ul li a"):
            yield {
                'Category_Name': data.css("::attr(title)").get(),
                'Category_Url': data.css('::attr(href)').get(),
                'Main_Category': item.get('cate_main', '')
            }
