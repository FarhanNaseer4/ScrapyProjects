import copy
import csv
import json
from datetime import datetime

import scrapy


class JamboDetailsSpider(scrapy.Spider):
    name = 'jambo_details'
    graphql = 'https://www.jumbo.com/api/graphql'
    base_url = 'https://www.jumbo.com{}'
    zyte_key = 'Enter_YOUR_KEY'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'jambo_details.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }
    payload = {
        "operation": "searchResult",
        "variables": {
            "searchTerms": "",
            "sortOption": "",
            "showMoreIds": "",
            "offSet": 0,
            "pageSize": 24,
            "categoryUrl": ''
        },
        "query": "\n  fragment productFields on Product {\n    id: sku\n    brand\n    badgeDescription\n    "
                 "category\n    subtitle: packSizeDisplay\n    title\n    image\n    inAssortment\n    availability {"
                 "\n      availability\n      isAvailable\n      label\n    }\n    isAvailable\n    isSponsored\n    "
                 "link\n    status\n    retailSet\n    prices: price {\n      price\n      promoPrice\n      "
                 "pricePerUnit {\n        price\n        unit\n      }\n    }\n    crossSellSkus\n    quantityDetails "
                 "{\n      maxAmount\n      minAmount\n      stepAmount\n      defaultAmount\n      unit\n    }\n    "
                 "quantityOptions {\n      maxAmount\n      minAmount\n      stepAmount\n      unit\n    }\n    "
                 "primaryBadge: primaryBadges {\n      alt\n      image\n    }\n    secondaryBadges {\n      alt\n    "
                 "  image\n    }\n    promotions {\n      id\n      group\n      isKiesAndMix\n      image\n      "
                 "tags {\n        text\n        inverse\n      }\n      start {\n        dayShort\n        date\n     "
                 "   monthShort\n      }\n      end {\n        dayShort\n        date\n        monthShort\n      }\n  "
                 "    attachments{\n        type\n        path\n      }\n    }\n  }\n\n  query searchResult(\n    "
                 "$searchTerms: String\n    $filters: String\n    $offSet: Int\n    $showMoreIds: String\n    "
                 "$sortOption: String\n    $pageSize: Int\n    $categoryUrl: String\n  ) {\n    searchResult(\n      "
                 "searchTerms: $searchTerms\n      filters: $filters\n      offSet: $offSet\n      showMoreIds: "
                 "$showMoreIds\n      sortOption: $sortOption\n      pageSize: $pageSize\n      categoryUrl: "
                 "$categoryUrl\n    ) {\n      canonicalRelativePath\n      categoryIdPath\n      categoryTiles {\n   "
                 "     id\n        label\n        imageLink\n        navigationState\n        siteRootPath\n      }\n "
                 "     urlState\n      newUrl\n      redirectUrl\n      shelfDescription\n      removeAllAction\n     "
                 " powerFilters {\n        displayName\n        navigationState\n        siteRootPath\n      }\n      "
                 "metaData {\n        title\n        description\n      }\n      headerContent {\n        "
                 "headerText\n        count\n      }\n      helperText {\n        show\n        shortBody\n        "
                 "longBody\n        header\n        linkText\n        targetUrl\n        messageType\n      }\n      "
                 "recipeLink {\n        linkText\n        targetUrl\n        textIsRich\n      }\n      "
                 "guidedNavigation {\n        ancestors {\n          label\n        }\n        displayName\n        "
                 "dimensionName\n        groupName\n        name\n        multiSelect\n        moreLink {\n          "
                 "label\n          navigationState\n        }\n        lessLink {\n          label\n          "
                 "navigationState\n        }\n        refinements {\n          label\n          count\n          "
                 "multiSelect\n          navigationState\n          siteRootPath\n        }\n      }\n      "
                 "selectedRefinements {\n        refinementCrumbs {\n          label\n          count\n          "
                 "multiSelect\n          dimensionName\n          ancestors {\n            label\n            "
                 "navigationState\n          }\n          removeAction {\n            navigationState\n          }\n  "
                 "      }\n        searchCrumbs {\n         terms\n         removeAction {\n          "
                 "navigationState\n         }\n        }\n        removeAllAction {\n         navigationState\n       "
                 " }\n      }\n      socialLists {\n        title\n        totalNumRecs\n        lists {\n          "
                 "id\n          title\n          followers\n          productImages\n          thumbnail\n          "
                 "author\n          labels\n          isAuthorVerified\n        }\n      }\n      mainContent {\n     "
                 "   searchWarning\n        searchAdjustments {\n          originalTerms\n          adjustedSearches "
                 "{\n            key\n            terms {\n              autoPhrased\n              adjustedTerms\n   "
                 "           spellCorrected\n            }\n          }\n        }\n      }\n      productsResultList "
                 "{\n        pagingActionTemplate {\n          navigationState\n        }\n        lastRecNum\n       "
                 " totalNumRecs\n        sortOptions {\n          navigationState\n          label\n          "
                 "selected\n        }\n        products {\n          ...productFields\n          retailSetProducts {"
                 "\n            ...productFields\n          }\n        }\n      }\n    }\n  }\n "
    }
    headers = {
        'accept': 'application/json',
        'content-type': 'application/json',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36',
        'X-Crawlera-Profile': 'pass',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_urls = self.get_search_urls()

    def get_search_urls(self):
        with open('jambo_categories.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for data in self.request_urls:
            payload = copy.deepcopy(self.payload)
            payload['categoryUrl'] = data.get('Category_url', '')
            item = {'cate_url': data.get('Category_url', ''),
                    'Page_no': 1,
                    'Category_url': self.base_url.format(data.get('Category_url', ''))}
            yield scrapy.Request(url=self.graphql, method='POST', callback=self.parse,
                                 body=json.dumps(payload), meta={'item': item},
                                 headers=self.headers, dont_filter=True)

    def parse(self, response):
        result = json.loads(response.body)
        item = response.meta['item']
        rank = 1
        for data in result.get('data', {}).get('searchResult', {}).get('productsResultList', {}).get('products', []):
            url = data.get('link', '')
            if not url.startswith(self.base_url):
                url = self.base_url.format(url)
            yield {
                'Product_Id': data.get('id', ''),
                'Product_Name': data.get('title', ''),
                'Brand_Name': data.get('brand', ''),
                'Sub_title': data.get('subtitle', ''),
                'Image': data.get('image', ''),
                'Category': data.get('category', ''),
                'Price': data.get('prices', {}).get('price', ''),
                'Promo_Price': data.get('prices', {}).get('promoPrice', ''),
                'Price_Per_Unit': data.get('prices', {}).get('pricePerUnit', {}).get('price', ''),
                'Primary_Badge': ', '.join(badge.get('alt', '') for badge in data.get('primaryBadge', [])),
                'Secondary_Badge': ', '.join(badge.get('alt', '') for badge in data.get('secondaryBadges', [])),
                'Product_Rank': rank,
                'Page_No': item.get('Page_no', ''),
                'Product_url': url,
                'Category_url': item.get('Category_url', ''),
                'TimeStamp': datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            }
            rank += 1
        next_offset = result.get('data', {}).get('searchResult', {}).get('productsResultList', {}).get('lastRecNum', '')
        if next_offset:
            payload = copy.deepcopy(self.payload)
            payload['categoryUrl'] = item.get('cate_url', '')
            payload['offset'] = next_offset
            item['Page_no'] = item.get('Page_no', '') + 1
            yield scrapy.Request(url=self.graphql, method='POST', callback=self.parse,
                                 body=json.dumps(payload), meta={'item': item},
                                 headers=self.headers, dont_filter=True)
