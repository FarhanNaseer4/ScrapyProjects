import csv
import os
import re
import pandas as pd
import scrapy


class Scraper1Spider(scrapy.Spider):
    name = 'scraper_1'
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {'scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware': 610},
        'ZYTE_SMARTPROXY_ENABLED': True,
        'ZYTE_SMARTPROXY_APIKEY': 'bbe5eb726dcb4efba66324c3e4dce259',
        'X-Crawlera-Region': 'US',
    }
    excel_file = 'Scraper_1_Data.xlsx'
    headers = {
        'authority': 'www.indeed.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        'cookie': 'CTK=1h0i28tquholp800; PPID=""; __cf_bm=FYPHoEXPPQM8SNO2FqIrFUtB1Jp41AWjFzg4UdJCA5E-1688963664-0-AS4ZEv0JuUCjzkMHwje18vCEMCMDvMIsBqrDtj6dJanbsptciUEHDtpOk4wSkg7jMJ9j2QYf5UF0MzXOWHBEIKI=; _cfuvid=v3njtQOddonyfx7nkmSmmVVo_NFHrqyo5_T_N6ZA.XA-1688963664497-0-604800000; CSRF=4McAOrk3SI6uhCSR2gdjL6FlfgkhTzQ1; INDEED_CSRF_TOKEN=QPJocs4dbht54MU1qsogcjqI3te1kh4H; LV="LA=1688963665:CV=1688963665:TS=1688963665"; hpnode=1; _gid=GA1.2.226577698.1688963670; SURF=Nk7IE2hBkNVgIWdoZyj9hs0PArNX9xHN; CO=PK; RF="TFTzyBUJoNr6YttPP3kyivpZ6-9J49o-Uk3iY6QNQqKE2fh7FyVgtZT-hVucuZ8SOEqLW4lcgPA="; CTK=1h0i28tquholp800; _gcl_au=1.1.393825838.1688963694; _mkto_trk=id:699-SXJ-715&token:_mch-indeed.com-1688963695130-98485; NCR=1; _ga_5KTMMETCF4=GS1.1.1688963694.1.0.1688963699.0.0.0; indeed_rcc="PREF:LV:CTK:CO:UD"; _ga=GA1.2.25336558.1688963670; gonetap=3; g_state={"i_p":1688970905764,"i_l":1}; PREF="TM=1688963737977:LD=en:L=Wilmington%2C"; jaSerpCount=1; UD="LA=1688963737:CV=1688963737:TS=1688963737:SG=3d4b743d5bb805eed54d0a02b8824e44"; RQ="q=nu&l=Wilmington%2C+&ts=1688963738016"; JSESSIONID=4A4F7CFAD892D518A95FCFBA34FC4AAC; ac=PGgX8B7bEe6aZa11oSnPnQ#PG4LYB7bEe6aZa11oSnPnQ; _gat=1; _gali=jobsearch; PTK=tk=1h4v1aqbqlekg800&type=jobsearch&subtype=topsearch; __cf_bm=A_1FgycCjrAVJ5H6rPJt.ImMASk0K7eDYJKlOxdRmDI-1688964707-0-AabPzHlymNZ5K4HBfA7GjIWUgRELet3O4U3t9QMAsPJpGprXz1iX4jsM09WNf3afB7lcOzWpDGEzkxFeB97Zrys=',
        'pragma': 'no-cache',
        'referer': 'https://www.indeed.com/jobs?q=nu&l=Wilmington%2C%20&from=searchOnHP',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    url = "https://www.indeed.com/jobs?q={}&l={}&vjk=51e4c487a3d8ca57"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.serial = self.get_serials()

    def get_serials(self):
        with open(f'input/scraper_1_input.csv', 'r') as csv_file:
            return list(csv.DictReader(csv_file))

    def start_requests(self):
        for terms in self.serial[3:]:
            keyword = terms.get('Keyword', '').strip().replace(' ', '+')
            city = terms.get('City', '').replace(',', '%2C').strip().replace(' ', '+')
            print(terms.get('City', ''))
            yield scrapy.Request(url=self.url.format(keyword, city), headers=self.headers, dont_filter=True,
                                 meta={'search': terms})

    def parse(self, response):
        search = response.meta['search']
        filters_data = []
        for data in response.xpath('//div[button[@id="filter-radius"]]/following-sibling::div'):
            item = dict()
            item['Sheet_Name'] = data.xpath('.//div[@class="yosegi-FilterPill-pillLabel"]/text()').get('').strip()
            item['Keyword'] = search.get('Keyword', '')
            item['City'] = search.get('City', '')
            for records in data.xpath('./ul/li/a'):
                options = records.xpath('./text()').get('').strip()
                if options:
                    key_name = options.split('(')[0].strip()
                    item[key_name] = options.split('(')[-1].replace(')', '').strip()
            filters_data.append(item)
        # print(filters_data)
        if any(filters_data):
            self.create_excel_append_data(filters_data)

    def create_excel_append_data(self, filters):
        if os.path.exists(self.excel_file):
            excel_data = pd.read_excel(self.excel_file, sheet_name=None, engine='openpyxl')
        else:
            excel_data = {}
        for data_dict in filters:
            sheet_name = data_dict['Sheet_Name']
            valid_sheet_name = self.get_valid_sheet_name(sheet_name)
            data_dict.pop('Sheet_Name')
            if valid_sheet_name in excel_data:
                df = pd.DataFrame([data_dict])
                excel_data[valid_sheet_name] = pd.concat([excel_data[valid_sheet_name], df], ignore_index=True)
            else:
                excel_data[valid_sheet_name] = pd.DataFrame([data_dict])
        with pd.ExcelWriter(self.excel_file, engine='openpyxl') as writer:
            for sheet_name, sheet_data in excel_data.items():
                sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)

            writer.save()

    def get_valid_sheet_name(self, sheet_name):
        valid_sheet_name = re.sub(f'[\/\\\*\[\]:\?]', '', sheet_name)
        if not valid_sheet_name or len(valid_sheet_name) > 31:
            valid_sheet_name = 'Sheet'
        return valid_sheet_name
