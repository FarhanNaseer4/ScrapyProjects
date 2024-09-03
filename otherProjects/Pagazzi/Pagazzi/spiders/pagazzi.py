import re
from urllib.parse import urljoin

import scrapy
from colour import Color


class PagazziSpider(scrapy.Spider):
    name = 'pagazzi'
    url = 'https://www.pagazzi.com/mirrors-clocks-wall-art/wall-art'
    today = f'output/{name}.csv'
    custom_settings = {
        'FEED_URI': today,
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'ITEM_PIPELINES': {'Pagazzi.pipelines.CustomImagesPipeline': 1},
        'IMAGES_STORE': f'{name}_images',
        'RETRY_TIMES': 5,
        'HTTPERROR_ALLOW_ALL': True,
        'ROBOTSTXT_OBEY': False,
    }

    headers = {
        'authority': 'www.pagazzi.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/'
                  'avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                      ' (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse_listing_page, headers=self.headers)

    # def parse_categories(self, response):
    #     all_categories = response.xpath(
    #         './/div[@class="page-wrapper"]/header//div[@class="content-header"]//nav//li/a/@href').getall()
    #     for url in all_categories:
    #         yield scrapy.Request(url=urljoin(self.url, url), callback=self.parse_listing_page, headers=self.headers)

    def parse_listing_page(self, response):
        detail_urls = response.xpath('.//ol[contains(@class,"product-items")]/li//a[@onclick]/@href').getall()
        for url in detail_urls:
            yield scrapy.Request(url=urljoin(self.url, url), callback=self.parse_detail_page, headers=self.headers)

        next_page = response.xpath('.//span[text()="Next"]/../@href').get()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse_listing_page, headers=self.headers)

    def parse_detail_page(self, response):
        item = dict()
        item['URL'] = response.url
        item['Name'] = response.xpath('.//h1[@class="page-title"]/span/text()').get('').strip()
        item['Sku'] = response.xpath('.//div[@class="product attribute sku"]/div/text()').get('').strip()
        item['Description'] = '\n\n'.join([' '.join(re.sub('\s+', ' ', detail)).strip() for detail in response.xpath(
            './/div[contains(@class,"description")]/div/*//text()').getall() if re.sub(
            '\s+', '', detail).strip() != ''])
        item['Colour Options'] = ' | '.join([color for color in item.get('Name').split() if self.check_color(color)])
        item['Price'] = response.xpath(
            './/*[@data-price-type="finalPrice"]/span/text()').get('').strip()
        item['Old Price'] = response.xpath(
            './/*[@data-price-type="oldPrice"]/span/text()').get('').strip()
        item['Measurements'] = '\n'.join(
            [f'{data.xpath(".//th/text()").get("")}: {data.xpath(".//td/text()").get("")}' for data in
             response.xpath('.//table[@id="product-attribute-specs-table"]//tr') if re.findall(
                '\d+', data.xpath(".//td/text()").get(""))])
        item['Category'] = ' > '.join([cat for cat in response.xpath(
            './/div[@class="breadcrumbs"]/ul/li//text()').getall() if re.sub('\s', '', cat).strip() != '']).strip()
        for i in range(500):
            item[f'Image {i + 1}'] = ''
        for index, image_url in enumerate(
                list(set(response.xpath('.//div[@class="MagicToolboxSelectorsContainer"]//a/@href').getall()))):
            item[f'Image {index + 1}'] = image_url
        yield item

    @staticmethod
    def check_color(color):
        try:
            Color(color)
            return True
        except ValueError:
            return False
