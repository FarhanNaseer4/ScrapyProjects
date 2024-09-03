# -*- coding: utf-8 -*-
import scrapy


class ApspolskaSpider(scrapy.Spider):
    name = 'apspolska'
    start_urls = ['https://apspolska.pl/']
    custom_settings = {
        'FEED_URI': 'apspolska_v2.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORT_FIELDS': ['Product URL', 'Product image URL', 'Product name', 'Amount & unit', 'Price',
                               'Kod/Product code', 'Product Description']
    }
    headers = {
        'authority': 'apspolska.pl',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/109.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse, headers=self.headers)

    def parse(self, response):
        for data in response.css('li.nav_desktop a.js--main_category'):
            main_cate = data.css('::attr(href)').get()
            if main_cate:
                yield scrapy.Request(url=main_cate, callback=self.parse_sub_cate, headers=self.headers)

    def parse_sub_cate(self, response):
        for sub_cate in response.css('a.categories__list--item_link'):
            sub_url = sub_cate.css('::attr(href)').get()
            if sub_url:
                yield scrapy.Request(url=sub_url, callback=self.parse_listing, headers=self.headers)

    def parse_listing(self, response):
        for data in response.xpath('//div[@class="g--container"]//div[@class="product_left_section"]/a'):
            detail_url = data.xpath('./@href').get()
            if detail_url:
                yield scrapy.Request(url=detail_url, callback=self.parse_details, headers=self.headers)

        next_page = response.css('a.next::attr(href)').get()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse_listing, headers=self.headers)

    def parse_details(self, response):
        item = dict()
        units = response.xpath('//div[@class="product__desc__content"]/p/text()').getall()
        item['Amount & unit'] = ''.join(
            value.replace('Pojemność:', '').strip() for value in units if 'Pojemność' in value)
        item['Product URL'] = response.url
        item['Product image URL'] = response.css('li a img[itemprop="image"]::attr(src)').get('').strip()
        item['Product name'] = response.css('h1[itemprop="name"]::text').get('').strip()
        item['Price'] = response.css('meta[itemprop="price"]::attr(content)').get('').strip()
        item['Kod/Product code'] = response.css('span[itemprop="mpn"]::text').get('').strip()
        item['Product Description'] = ''.join(response.css('div[itemprop="description"] p::text').getall()).strip()
        yield item
