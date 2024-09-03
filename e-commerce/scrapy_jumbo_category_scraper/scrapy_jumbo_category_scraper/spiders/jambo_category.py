import scrapy


class JamboCategorySpider(scrapy.Spider):
    name = 'jambo_category'
    start_urls = ['https://www.jumbo.com/producten/']
    base_url = 'https://www.jumbo.com{}'
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'jambo_categories.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }
    headers = {
        'authority': 'www.jumbo.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36 ',
        'X-Crawlera-Profile': 'pass',
    }

    def parse(self, response):
        for data in response.xpath("//div[@class='filter-dimension']/ul[@analytics-tag='Categories']/li/a"):
            category_url = data.xpath("./@href").get()
            if not category_url.startswith(self.base_url):
                category_url = self.base_url.format(category_url)
            yield scrapy.Request(url=category_url, callback=self.sub_categories, headers=self.headers)

    def sub_categories(self, response):
        for data in response.xpath("//div[@class='filter-dimension']/ul[@analytics-tag='Categories']/li/a"):
            yield {
                'Category_Name': data.xpath("./text()").get(),
                'Category_url': data.xpath("./@href").get(),
                'Main_category_url': response.url
            }
