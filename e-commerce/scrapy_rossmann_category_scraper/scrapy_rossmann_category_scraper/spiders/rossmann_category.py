import scrapy


class RossmannCategorySpider(scrapy.Spider):
    name = 'rossmann_category'
    start_urls = ['https://www.rossmann.de/de/marken/c/brands']
    base_url = 'https://www.rossmann.de{}'
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'rossmann_categories.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }

    def parse(self, response):
        i = 1
        for data in response.css(".rm-brand__container-letter ul li a"):
            pro_url = data.css("::attr(href)").get()
            if not pro_url.startswith(self.base_url):
                pro_url = self.base_url.format(pro_url)
            yield {
                'Category_Name': data.css("::text").get(),
                'Category_Url': pro_url
            }
