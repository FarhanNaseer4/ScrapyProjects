import json

import scrapy


class ChewyCategorySpider(scrapy.Spider):
    name = 'chewy_category'
    start_urls = ['https://www.chewy.com/']
    custom_settings = {
        'FEED_URI': 'chewy_category_v2.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }
    headers = {
        'authority': 'www.chewy.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        # 'cookie': 'pid=hp2r_3ubSoGDUDAJ-FM59Q_; ajs_anonymous_id=a4d14d8d-4adc-4319-96db-ff70fd95c7a3; _gcl_au=1.1.230926452.1683405239; _scid=7242a8d9-984d-44ea-8c77-5c823e28ab02; _mibhv=anon-1683405239957-9706766622_6593; _tt_enable_cookie=1; _ttp=Y18_XGN1e0S8u6j-cl2sKeYONXo; FPID=FPID1.2.9LSECmdJoL%2FNJRgbIdoqjmeSb9yR9H3y%2BGBXZsS6zqk%3D.1683405239; _pin_unauth=dWlkPU16UTBabU0xT0RZdFpHUmlaUzAwT1RJM0xXSXdaR0V0TVdGaE5EZGlPR1ZoWkdGbQ; _hjSessionUser_1727423=eyJpZCI6ImNmOWJiNTI5LTNmY2EtNTM1NC05OTdlLTdhOWVjYTIyYmFjMSIsImNyZWF0ZWQiOjE2ODM0MDUyNDAzNzUsImV4aXN0aW5nIjp0cnVlfQ==; addshoppers.com=2%7C1%3A0%7C10%3A1683405257%7C15%3Aaddshoppers.com%7C44%3AZmZkM2JiM2ViYWY1NDkxN2JjNDhjOGQ1NDEzZTk1MmQ%3D%7C06acd9cda406d01f1c1a032c57534dd378e902ed2e2b19179f195c9a5d0a5829; experiment_=; AKA_A2=A; _abck=3A7C853A781C32091034C61D3BA9509B~0~YAAQj9nARXME3+CHAQAARHh4+wkIKCpm2sD7DT1g6ypbHa3Zz4nrcRIQioAGrH9UUxWRa3bWasRRFaZBayb9a6jgAKcU93MTtIAD4a+EGGFLGIrZ7viBYe0OdWX29nBVJCfLI90uec2YcdHfGeXMzT3prMrcF6bh3P7ZqydgCgkhtmyltIw4/e8cQxs1P3vKto4eP15nE6QstgHNCABTWRVqhKoOA7dQhejZiZrFuy30gauqrwJNMfmXq4LDOxBANFWaYlPl+dZ8pVBWh3H9PiKJf2ylZUhP6n3Tp8zn4W85JXjuZUsyEgfs9FIXl8IRqe+Hh2ggI5L8xqpV985KB7GBfJ+q23vVnqOI1ddA9qk8qmbWgAlypdFDpv1fSqJlojrScu4BEl8aOk30aYiTy17NHkUaTtY=~-1~-1~-1; bm_sz=F3F6D4FFF3BFAFDFD0DFF2CFFEE35BC2~YAAQj9nARXYE3+CHAQAARHh4+xMwV4nxEqooen9O3ZOrcf0vQwxY5BN0ASCwjiOkxVYbRHrbYoFL69Y/UvMr2fk82j3xvfkqxpzehB29vU4m0v6+J7NMwKCoo/sGjiH04dvBKCeqrgOijYd6FfUr6PcocUZgwu40XTrkgg7lk0iyHmfRgMfl7SYeFNG2TFXjRQRGaSvP6o6bOL0sfaWoZEM7FXECS5HaKuWdNAyqehaJp3EorImA8ZY2Bu+0oypWDbIIKT0yJNTDRf8NFXNu1onR+u6PJAjSJg/HrwhkkuC31A==~4536630~4474161; experiment_2022_09_HEADER_SITEWIDE_BANNER=VARIANT; experiment_2022_10_NAV_INGRESS_SUPERLATIVE=CONTROL; experiment_2023_03_HEADER_SIGN_IN_REDIRECT=VAR_2; experiment_2023_02_NAV_IA_TEST=CONTROL; abTestingAnonymousPID=hp2r_3ubSoGDUDAJ-FM59Q_; ak_bmsc=640E2DE4F5E56E6C1B6A0803ECA7EBCA~000000000000000000000000000000~YAAQj9nAReoE3+CHAQAAA454+xOYvGAg58a3mTprxP3R+rW+izSkS1oP6LD0GGNQOxdR0ltCQiyWko6VqUo7orPbwPxNdtQq+Suf9byFUfY84hW+UVFObGrSg9SWOki+61jf12rY3KW7PnZWxioGBBss3CuId9N8WOHvYLa5TVWcit7B/c+EhVnpusAO7vO8GThSWXKOj0abTOEBUr+duuhLy6fC4tZaU8zgY0+iHfH2yyBH3y4dJNSjFrzqnI7ZRwW3J5XTqs40upPSs/J9H0rV3cV4rHrC6OF3tCG21AFKScxdm9kzT9XWbYHW4zZ29I7SMLhVnwA4h7WwWPBAVxPFavNU2+5mDKJThfuZsafVEwy7ialygtFslYUmFMpfXwxd6Obx9UeoUcHPHmtHN+iEiFqo8aNTnTvfklfrOk9Z5DQcCV3tLyaJ8oDgCed78ogKr/OyetHsXqzAcMXeBfWeKYZQfIjMZMxrNwBZtbx9mYRm7jl3QdFDCn2XHzOkHehTLibJL3neSzig9DW6klXD; _gid=GA1.2.787073954.1683551197; _hjIncludedInSessionSample_1727423=0; _hjSession_1727423=eyJpZCI6ImY0ZWEwZDFmLTdlNjQtNGQ1Yi1hMjZmLTc0MjM2Y2M4ZDdlMiIsImNyZWF0ZWQiOjE2ODM1NTExOTg0MDYsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; _hjHasCachedUserAttributes=true; FPLC=PrD8T%2Bstfe5xantOVIiFwux%2FvOmQxs%2FIO1dGUYCxQN%2Bu6KL2r%2B%2BokyMYXKynBo8ROWiIvhnGCRWaFG5jmGp0aySBnbegKNGgxya6KMbACnw9YfIOKab3bLrTPVr2Zg%3D%3D; sid=0c11d88c-343c-40bb-a612-3f1352fcc328; AWSALB=Q/MqxkMcYHJPuAe8hS1rUbIaeVwdbJDEG0VRsQuA2idOz+E2tKH9mdYFZS7T6mZLQhXKAo4gVTP/OKQklLipDm3yh4VmCV8k4dP7f+so9FMIDzG6HJ29DsLINSM8; pageviewCount=33; _scid_r=7242a8d9-984d-44ea-8c77-5c823e28ab02; _uetsid=2a633c40eda111edbdf83f40685aecab; _uetvid=54e2b1e0ec4d11ed82e123b3c43a07cf; cto_bundle=WZbpt19DQyUyQmxLM0thYUZ2bUFRR2xXR2NMdVd0TnhFYkZEcGR4b1k3OEN6SWpJSFdJcU5YdjVGbVltSWlhejBEQVdQdVZqd1VFSVZRZTN0cmxDcGtwdWJ4akZhRlFsaDBwVXNiSXRVREVQV1QzNklrTmh5MUJSeGhCNDB1JTJCdXJQVVFGMGp2Qm95RFJIbEo0eHY4ODh1SzE2eExRJTNEJTNE; _ga=GA1.2.1455861625.1683405239; OptanonConsent=isGpcEnabled=0&datestamp=Mon+May+08+2023+18%3A52%3A13+GMT%2B0500+(Pakistan+Standard+Time)&version=202303.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=BG15%3A1%2CC0004%3A1%2CC0010%3A1%2CC0011%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1&AwaitingReconsent=false; RT="z=1&dm=www.chewy.com&si=fa46c427-b89c-4093-94cd-c94ad1b00c0c&ss=lheuwqi0&sl=6&tt=jzy&bcn=%2F%2F02179911.akstat.io%2F&obo=3&rl=1&nu=3e3ez51&cl=1nbe4"; bm_sv=E5399EF40DF3186C5C0163956DE024B4~YAAQhdnARYjkE9aHAQAAkLWi+xMdZ6608aQJbSzYlm/ToiBbxe+a/togitnQx1wnx04d94mNtqHh6VvMEOdAcpSu4gvg+zFqts7vZiJdOBlSH0/Ho9uC2Jl20Gv4wHrt3Yb71dpSOg56fxiewSi+G2CFPlQDpGGXxWuZwhLzq+gj1q8OME4jLqTc/A2UjZs0QGiDPf3XB2wjwpwSgp84Sm0P3Js/We0QdAhQViw74x+N6hHnhjoO4o5dXHjedjLZjg==~1; akavpau_defaultvp=1683554259~id=7c349ceafce65d6ccabfae357cab879f; akaalb_chewy_ALB=1683554559~op=prd_chewy_lando:www-prd-lando-use1|prd_chewy_plp:www-prd-plp-use2|prd_chewy_plp_sp:searchpilot-prd-use2|chewy_com_ALB:www-chewy-use1|~rv=99~m=www-prd-lando-use1:0|www-prd-plp-use2:0|searchpilot-prd-use2:0|www-chewy-use1:0|~os=43a06daff4514d805d02d3b6b5e79808~id=4b4e0d6e226a7cd9b1b724cb88c73dd3; _ga_GM4GWYGVKP=GS1.1.1683551197.2.1.1683553960.24.0.0',
        'pragma': 'no-cache',
        # 'referer': 'https://www.google.com/',
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    }
    main_list = ['/b/dog-288', '/b/cat-325', '/b/small-pet-977', '/b/bird-941', '/b/fish-885', '/b/reptile-1025',
                 '/b/horse-1663', '/b/farm-animal-8403', '/b/pet-parents-15439']
    base_url = 'https://www.chewy.com{}'

    def start_requests(self):
        for cate in self.main_list:
            cate_ = {'main': self.base_url.format(cate)}
            yield scrapy.Request(url=self.base_url.format(cate), headers=self.headers, meta={'category': cate_})

    def parse(self, response):
        check = response.css('a.CategoryEntry_categoryLabel__YwTzx::attr(href)').getall()
        if any(check):
            category = response.meta['category']
            for url in check:
                category['sub_category 1'] = self.base_url.format(url)
                yield response.follow(url=self.base_url.format(url), callback=self.sub_category1, headers=self.headers,
                                      meta={'category': category})

    def sub_category1(self, response):
        check = response.css('a.CategoryEntry_categoryLabel__YwTzx::attr(href)').getall()
        if any(check):
            category = response.meta['category']
            for url in check:
                category['sub_category 2'] = self.base_url.format(url)
                yield response.follow(url=self.base_url.format(url), callback=self.sub_category2, headers=self.headers,
                                      meta={'category': category})

    def sub_category2(self, response):
        check = response.css('a.CategoryEntry_categoryLabel__YwTzx::attr(href)').getall()
        if any(check):
            category = response.meta['category']
            for url in check:
                category['sub_category 3'] = self.base_url.format(url)
                yield response.follow(url=self.base_url.format(url), callback=self.sub_category3, headers=self.headers,
                                      meta={'category': category})
        else:
            category = response.meta['category']
            for url in check:
                category['working category'] = self.base_url.format(url)
                yield category

    def sub_category3(self, response):
        category = response.meta['category']
        category['working category'] = response.url
        yield category
