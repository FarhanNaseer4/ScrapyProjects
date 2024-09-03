import csv
import os.path
from datetime import datetime
import scrapy


class AmazonSpiderSpider(scrapy.Spider):
    name = 'amazon_spider'
    today = f'output/AmazonReview.csv'
    custom_settings = {
        'FEED_URI': today,
        'FEED_FORMAT': 'csv',
    }
    base_request = 'http://api.scraperapi.com/?api_key=e1e890dfabff60329a9e07a3fe08e1b2&url={}'
    base_url = 'http://www.amazon.co.uk/{}'
    headers = {
        'authority': 'www.amazon.co.uk',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        # this function initialize the variables
        super().__init__(**kwargs)
        self.request_url = self.get_search_urls()
        self.dict_name, self.dict_text = self.get_review_dict()

    def get_search_urls(self):
        # this function reads the text file to get start and end date range for calendar
        with open('input/search_input.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_url:
            url = data.get('url', '')
            yield scrapy.Request(url=self.base_request.format(url),
                                 callback=self.parse, headers=self.headers)

    def get_review_dict(self):
        file_exists = os.path.exists('output/AmazonReview.csv')
        if file_exists:
            with open('output/AmazonReview.csv', 'r', encoding='utf-8-sig') as reader:
                review_name = []
                review_text = []
                for data in csv.DictReader(reader):
                    review_name.append(data['Reviewer_Name'])
                    review_text.append(data['Review_Text'])
                return review_name, review_text
        else:
            return [], []

    def scrap_detail(self, data, response):
        product_name = response.xpath('//div[@class="a-row product-title"]/h1/a/text()').get()
        brand_name = response.xpath('//div[@data-hook="cr-product-byline"]/span/a/text()').get()
        total_rating = response.xpath('//div[@data-hook="total-review-count"]/span/text()').get()
        avg_rating_stars = response.xpath('//div[@class="a-row"]/span/text()').get()
        review_text = data.xpath('.//*/div[contains(@class,"spacing-small")]/span/span//text()').getall()
        reviewer_name = data.xpath('.//*/div[contains(@class,"a-profile-content")]/span/text()').get()
        print(review_text)
        item = {
            'Product_Name': product_name,
            'Brand_Name': brand_name,
            'Total_Ratings': total_rating,
            'Avg_Rating_Stars': avg_rating_stars,
            'Reviewer_Name': reviewer_name,
            'Review_star': data.xpath('.//*/div[contains(@class, "a-row")]//i[contains(@data-hook, '
                                      '"review-star-rating")]/span/text()').get(),
            'Review_Title': data.xpath('.//*/div[@class="a-row"]/a[contains(@data-hook,'
                                       '"review-title")]/span/text()').get(),
            'Review_date': data.xpath('.//*/span[contains(@data-hook,"review-date")]/text()').get(),
            'Review_details': data.xpath('.//*/a/span[contains(@data-hook,"avp-badge")]/text()').get(),
            'Review_Text': review_text,
            'Helpful_for': data.xpath('.//*/span[@class="cr-vote"]/div/span/text()').get(),
            'TimeStamp': datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        }
        return item

    def parse(self, response):
        if self.dict_text:
            for data in response.xpath('//div[@data-hook="review"]'):
                check = True
                review_text = data.xpath('.//*/div[contains(@class,"spacing-small")]/span/span').get()
                reviewer_name = data.xpath('.//*/div[contains(@class,"a-profile-content")]/span/text()').get()
                review_data = review_text.replace('<span>', '').replace('<br>', '').replace('</span>',
                                                                                            '') if review_text else ''
                for index in range(len(self.dict_name)):
                    if (reviewer_name == self.dict_name[index]) and (review_data == self.dict_text[index]):
                        check = False
                        break
                if check:
                    item = self.scrap_detail(data, response)
                    yield item
        else:
            for data in response.xpath('//div[@data-hook="review"]'):
                item = self.scrap_detail(data, response)
                yield item
        next_page = response.xpath('//ul[@class="a-pagination"]/li[@class="a-last"]/a/@href').get()
        if next_page:
            yield response.follow(url=self.base_request.format(self.base_url.format(next_page)),
                                  callback=self.parse, headers=self.headers)
