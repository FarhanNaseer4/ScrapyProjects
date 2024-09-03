import scrapy
from scrapy.utils.response import open_in_browser


# why this

# def get_headers(s, sep=': ', strip_cookie=True, strip_cl=True, strip_headers: list = []) -> dict():
#     d = dict()
#     for kv in s.split('\n'):
#         kv = kv.strip()
#         if kv and sep in kv:
#             v=''
#             k = kv.split(sep)[0]
#             if len(kv.split(sep)) == 1:
#                 v = ''
#             else:
#                 v = kv.split(sep)[1]
#             if v == '\'\'':
#                 v =''
#             # v = kv.split(sep)[1]
#             if strip_cookie and k.lower() == 'cookie': continue
#             if strip_cl and k.lower() == 'content-length': continue
#             if k in strip_headers: continue
#             d[k] = v
#     return d


class BusinessSpider(scrapy.Spider):
    name = 'business'
    start_urls = ['https://chamber.saratoga.org/list']

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/105.0.0.0 Safari/537.36 '
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], headers=self.headers, callback=self.parse)

    def parse(self, response):
        for data in response.css('div.gz-alphanumeric-btn a'):
            url = data.css('::attr(href)').get()
            print(url)
            # yield scrapy.Request(url=url, callback=self.parse_listing, headers=self.headers)
