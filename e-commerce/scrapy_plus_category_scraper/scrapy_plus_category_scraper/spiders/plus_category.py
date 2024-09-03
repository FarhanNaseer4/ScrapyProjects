import scrapy


class PlusCategorySpider(scrapy.Spider):
    name = 'plus_category'
    start_urls = ['https://www.plus.nl/']
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'plus_categories.csv',
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

    def parse(self, response):
        for data in response.xpath("//div[@class='js-assortiment-block-container']/div[contains(@class,"
                                   "'col-xs-12')]/following-sibling::div[contains(@class,'col-xs-12')]/a"):
            cate_url = data.xpath("./@href").get()
            yield scrapy.Request(url=cate_url,
                                 callback=self.scrap_categories, headers=self.headers)

    def scrap_categories(self, response):
        for data in response.xpath("//div[contains(@class,'enter stickyMenuContent')]/ul/li/a"):
            yield {
                'Category_Name': data.xpath("./text()").get(),
                'Category_Url': data.xpath("./@href").get(),
                'Main_Category': response.url
            }
