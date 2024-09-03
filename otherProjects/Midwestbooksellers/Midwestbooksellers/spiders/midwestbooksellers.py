# -*- coding: utf-8 -*-
import copy
import json
from math import ceil

import scrapy


class MidwestbooksellersSpider(scrapy.Spider):
    name = 'midwestbooksellers'
    request_api = 'https://www.midwestbooksellers.org/members/directory-customer-list'
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': 'ASP.NET_SessionId=3mnow0z45t2kxjbqz1h2nfvh; ARRAffinity=47ed4454503496bc155d9516df80033e0e5b75aa1e700298afc6a2941eb48a75; ARRAffinitySameSite=47ed4454503496bc155d9516df80033e0e5b75aa1e700298afc6a2941eb48a75; _ga=GA1.1.1050730545.1675942539; OneTimeAlert=1613233258; _ga_98BKPXKF18=GS1.1.1675942538.1.1.1675942989.0.0.0',
        'Origin': 'https://www.midwestbooksellers.org',
        'Referer': 'https://www.midwestbooksellers.org/independent-bookstore-directory',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    custom_settings = {
        'FEED_URI': 'midwestbooksellers.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }
    data = {
        'directoryID': '144',
        'pageNumber': '1',
        'searchText': '',
        'memberTypeIDs': '',
        'specialOffer': '',
        'city': '',
        'state': '',
        'zip': '',
        'county': '',
        'country': '',
        'mapView': 'false',
        'latitude': '',
        'longitude': '',
        'radius': '50',
    }

    def start_requests(self):
        page = {'page_no': 1}
        yield scrapy.FormRequest(url=self.request_api, method='POST', meta={'page': page},
                                 formdata=self.data, headers=self.headers)

    def parse(self, response):
        page = response.meta['page']
        json_data = json.loads(response.body)
        members = json_data.get('Members', [])
        for member in members:
            item = dict()
            item['name'] = member.get('Name', '')
            item['address'] = member.get('ShippingAddress1', '')
            item['city'] = member.get('ShippingCity', '')
            item['state'] = member.get('ShippingState', '')
            item['zip'] = member.get('ShippingZip', '')
            item['phone'] = member.get('Phone', '')
            item['Facebook'] = member.get('FacebookUrl', '')
            item['Instagram'] = member.get('InstagramHandle', '')
            item['Twitter'] = member.get('TwitterHandle', '')
            item['Email'] = member.get('Email', '')
            yield item

        current_page = page.get('page_no', 0)
        total_count = json_data.get('TotalCount', '')
        total_page = ceil(int(total_count)/12)
        next_page = current_page + 1
        if next_page <= total_page:
            page['page_no'] = next_page
            payload = copy.deepcopy(self.data)
            payload['pageNumber'] = str(next_page)
            yield scrapy.FormRequest(url=self.request_api, method='POST', meta={'page': page},
                                     formdata=payload, headers=self.headers, callback=self.parse)