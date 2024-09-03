import scrapy


class CoopCategorySpider(scrapy.Spider):
    name = 'coop_category'
    start_urls = ['https://www.coop.nl/categorie/boodschappen']
    base_url = 'https://www.coop.nl{}'
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'coop_categories.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }

    headers = {
        'authority': 'www.coop.nl',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36 '
    }

    def parse(self, response):
        for data in response.css(".category-list__item a"):
            cate_url = data.css("::attr(href)").get()
            if not cate_url.startswith(self.base_url):
                cate_url = self.base_url.format(cate_url)
            yield scrapy.Request(url=cate_url, callback=self.scrap_subcategories, headers=self.headers)

    def scrap_subcategories(self, response):
        for data in response.css(".category-list__item a.category-tile"):
            cate_url = data.css("::attr(href)").get()
            if not cate_url.startswith(self.base_url):
                cate_url = self.base_url.format(cate_url)
            yield {
                'Category_Name': data.css(" div.category-tile__name::text").get(),
                'Category_url': cate_url,
                'Main_Category': response.url
            }
