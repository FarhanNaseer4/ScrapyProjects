import csv
import re
from datetime import datetime

import scrapy


class TripadvisorSpider(scrapy.Spider):
    name = 'tripadvisor'
    request_api = 'https://www.tripadvisor.co.nz/{}.html'
    custom_settings = {
        'FEED_URI': 'tripadvisor.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORT_FIELDS': ['Business Name', 'Full Name', 'First Name', 'Last Name', 'Street Address',
                               'Valid To', 'State', 'Zip', 'Description', 'Phone Number', 'Phone Number 1', 'Email',
                               'Business_Site', 'Social_Media', 'Record_Type',
                               'Category', 'Rating', 'Reviews', 'Source_URL', 'Detail_Url', 'Services',
                               'Latitude', 'Longitude', 'Occupation',
                               'Business_Type', 'Lead_Source', 'State_Abrv', 'State_TZ', 'State_Type',
                               'SIC_Sectors', 'SIC_Categories',
                               'SIC_Industries', 'NAICS_Code', 'Quick_Occupation', 'Scraped_date', 'Meta_Description']
    }
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cookie': 'TADCID=cBbhORTae_Mii6rCABQCFdpBzzOuRA-9xvCxaMyI13HboXWrSj5_aC2JzfKdPWQH0H7GmWNhm3yXyp4YQxTqiTLxvgukUFHhQt4; TAUnique=%1%enc%3AE%2F6xEZjZtWMvGPLw0ZSe%2BMsd48DVHPOh6ESb%2B8F%2FQXc1QsIvfKZfHg%3D%3D; TASSK=enc%3AAN7%2FdyRhOuyRmyg7OGAceFoI7DYCHDiC6OeEL7Y58B1w73Tw1WiX0ja%2FwVJ%2Fsm1gd0sE10c1%2FyaBKZYJDXz1YalzIk5xDN8%2Fyi4G9q%2BnS9%2B5prJ%2BGoIVjY%2FSdVktXn5Hnw%3D%3D; TATrkConsent=eyJvdXQiOiIiLCJpbiI6IkFMTCJ9; TART=%1%enc%3AX4ktmoNE87OVYa2RCjDC41U8EPyj9ppyFOSQ9Ly10d572yOK1SgSI8Z0ZQaUi%2F%2BiRsNr41gHGgI%3D; TATravelInfo=V2*AY.2022*AM.12*AD.11*DY.2022*DM.12*DD.12*A.2*MG.-1*HP.2*FL.3*DSM.1669721701663*RS.1; TASID=E3CA3BBE51F3F99060389C395E961A35; __vt=CNH71bJqJ9KIeeAqABQCIf6-ytF7QiW7ovfhqc-AvSG-FJe703zIhX7DoBcm7AgeXVWFXueSZ8S2DnpKP17XP4U7ylb7csHjwMJIDFcVl7Bnud9fvIVBRJRfwd1c9RMMrFEf5ZuMg-qKONKyVsTvz7JGVg; ak_bmsc=56FDFD32A5E48C867736673D858F0803~000000000000000000000000000000~YAAQrCg0F9EswLqEAQAAoZGTxxGSVzPvsevTJcGaabrYNRN7PnuIdmkRCo3ejfzyQozVO807a13k2OdAfU9hUbUzZkfdRtVvyLnFZ4DWqE37Y3LLOOEBys9nVyS+HYU0nd9Qi0f48yIDUTS8C+XMBmRnXj3Eh+kFc8VZTyXP2l+AbjcTgV5kTemD1hGFIL/Kk/z/Yx59vnjniUC8fpsr7XWHsv5eq0TimHuhzPtWT8Npe3y+bt5ylZ78ZTUSmmgq8vQ7eVGme5dLhvjyha5q8PGo13Hl3VRTBk6mLAr+GEBUMASYhbsSISnOsMgCekVmx6EVNBjZZSQl91b8toBATAFmfsN0LCFzKERrbE1h7AzFscKNhphehocb2XGJ9EPxUGUuuN5MjLlB/58WXx7Ykw==; PAC=ADUbskyOnlZ65yylAn-YGRmgqLuuy8SNjZKO8zpJamwaXxYczc2jO2LwQS6ErdEpo41BUgV-gKefBG1JG7KJXvxp1m9f9d6LXDQ8lS2_taXC6g-SFW2-gmkgQcRR0KpPXQ%3D%3D; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Nov+30+2022+13%3A07%3A30+GMT%2B0500+(Pakistan+Standard+Time)&version=202209.1.0&isIABGlobal=false&hosts=&consentId=d848fe95-f87d-4393-8414-794dd9feaaf6&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false; ServerPool=A; PMC=V2*MS.13*MD.20221129*LD.20221130; SRT=TART_SYNC; TASession=V2ID.E3CA3BBE51F3F99060389C395E961A35*SQ.4*LS.PageMoniker*HS.recommended*ES.popularity*DS.5*SAS.popularity*FPS.oldFirst*FA.1*DF.0*TRA.true*EAU._; TAUD=LA-1669721578755-1*RDD-1-2022_11_29*HDD-122831-2022_12_11.2022_12_12*LD-74115544-2022.12.11.2022.12.12*LG-74115546-2.1.F.; bm_sv=81E33AF3D1B09F8C551A28BB5BDA5071~YAAQrCg0F0E2wLqEAQAAzEiUxxFAyLGi9l2Bv3UJvWTmhupS0rL4d+FgE9YIv/cAFI6T3hTHTEkYdP5Em7cLnACBC9LUAxoSZdI8gfVhEeoa4eau9oRczftijjU1RoZYG2VjHAnKFUBhdP0Tp0QbPt+H84bx6uxuqtBvv4zh5T8fXpF96MLAs8Al6IY4YLlC13+HLdzRJA7DZ3YQqGro3UNom8nfPkAGwHP77y465158xawASf7qMA3Ng4rs6koCf1TM5Cj7cLs=~1; CM=%1%PremiumMobSess%2C%2C-1%7Ct4b-pc%2C%2C-1%7CRestAds%2FRPers%2C%2C-1%7CRCPers%2C%2C-1%7CWShadeSeen%2C%2C-1%7CTheForkMCCPers%2C%2C-1%7CHomeASess%2C%2C-1%7CPremiumMCSess%2C%2C-1%7CCrisisSess%2C%2C-1%7CUVOwnersSess%2C%2C-1%7CRestPremRSess%2C%2C-1%7CRepTarMCSess%2C%2C-1%7CCCSess%2C%2C-1%7CCYLSess%2C%2C-1%7CPremRetPers%2C%2C-1%7CViatorMCPers%2C%2C-1%7Csesssticker%2C%2C-1%7CPremiumORSess%2C%2C-1%7Ct4b-sc%2C%2C-1%7CRestAdsPers%2C%2C-1%7CMC_IB_UPSELL_IB_LOGOS2%2C%2C-1%7CTSMCPers%2C%2C-1%7Cb2bmcpers%2C%2C-1%7CPremMCBtmSess%2C%2C-1%7CMC_IB_UPSELL_IB_LOGOS%2C%2C-1%7CLaFourchette+Banners%2C%2C-1%7Csess_rev%2C%2C-1%7Csessamex%2C%2C-1%7CPremiumRRSess%2C%2C-1%7CTADORSess%2C%2C-1%7CAdsRetPers%2C%2C-1%7CListMCSess%2C%2C-1%7CTARSWBPers%2C%2C-1%7CSPMCSess%2C%2C-1%7CTheForkORSess%2C%2C-1%7CTheForkRRSess%2C%2C-1%7Cpers_rev%2C%2C-1%7CRBAPers%2C%2C-1%7CRestAds%2FRSess%2C%2C-1%7CHomeAPers%2C%2C-1%7CPremiumMobPers%2C%2C-1%7CRCSess%2C%2C-1%7CLaFourchette+MC+Banners%2C%2C-1%7CRestAdsCCSess%2C%2C-1%7CRestPremRPers%2C%2C-1%7CRevHubRMPers%2C%2C-1%7CUVOwnersPers%2C%2C-1%7Csh%2C%2C-1%7Cpssamex%2C%2C-1%7CTheForkMCCSess%2C%2C-1%7CCrisisPers%2C%2C-1%7CCYLPers%2C%2C-1%7CCCPers%2C%2C-1%7CRepTarMCPers%2C%2C-1%7Cb2bmcsess%2C%2C-1%7CTSMCSess%2C%2C-1%7CSPMCPers%2C%2C-1%7CRevHubRMSess%2C%2C-1%7CPremRetSess%2C%2C-1%7CViatorMCSess%2C%2C-1%7CPremiumMCPers%2C%2C-1%7CAdsRetSess%2C%2C-1%7CPremiumRRPers%2C%2C-1%7CRestAdsCCPers%2C%2C-1%7CTADORPers%2C%2C-1%7CTheForkORPers%2C%2C-1%7CPremMCBtmPers%2C%2C-1%7CTheForkRRPers%2C%2C-1%7CTARSWBSess%2C%2C-1%7CPremiumORPers%2C%2C-1%7CRestAdsSess%2C%2C-1%7CRBASess%2C%2C-1%7CSPORPers%2C%2C-1%7Cperssticker%2C%2C-1%7CListMCPers%2C%2C-1%7C; TAReturnTo=%1%%2FRestaurants-g28922-Alabama.html; TASession=V2ID.E3CA3BBE51F3F99060389C395E961A35*SQ.5*LS.Restaurants*HS.recommended*ES.popularity*DS.5*SAS.popularity*FPS.oldFirst*FA.1*DF.0*RT.0*TRA.true*LD.28922*EAU._; TAUD=LA-1669721578755-1*RDD-1-2022_11_29*HDD-122831-2022_12_11.2022_12_12*LD-75648614-2022.12.11.2022.12.12*LG-75648616-2.1.F.; TASID=E3CA3BBE51F3F99060389C395E961A35',
        'referer': 'https://www.tripadvisor.co.nz/',
        'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_keyword = self.get_search_keyword()

    def get_search_keyword(self):
        try:
            with open('search_keyword.csv', 'r', encoding='utf-8-sig') as reader:
                return list(csv.DictReader(reader))
        except Exception as ex:
            print('Error While reading file | ' + str(ex))

    def start_requests(self):
        try:
            for keyword in self.request_keyword:
                yield scrapy.Request(url=self.request_api.format(keyword.get('Keywords', '')),
                                     callback=self.parse, headers=self.headers)
        except Exception as ex:
            print('Error From Start Request | ' + str(ex))

    def parse(self, response):
        try:
            for data in response.css('div.geo_name a'):
                city_url = data.css('::attr(href)').get()
                if city_url:
                    yield response.follow(url=city_url,
                                          callback=self.parse_listing, headers=self.headers)

            next_page = response.xpath('//a[contains(text(),"Next")]/@href').get()
            if next_page:
                yield response.follow(url=next_page, callback=self.parse, headers=self.headers)
        except Exception as ex:
            print('Error From Parse | ' + str(ex))

    def parse_listing(self, response):
        try:
            for data in response.css('div a.Lwqic'):
                detail_url = data.css('::attr(href)').get()
                if detail_url:
                    yield response.follow(url=detail_url,
                                          callback=self.parse_details, headers=self.headers)

            next_page = response.xpath('//a[contains(text(),"Next")]/@href').get()
            if next_page:
                yield response.follow(url=next_page, callback=self.parse_listing, headers=self.headers)
        except Exception as ex:
            print('Error From Parse_listing | ' + str(ex))

    def parse_details(self, response):
        try:
            item = dict()
            item['Business Name'] = response.css('div.acKDw h1::text').get('').strip()
            contact_detail = response.css('span.DsyBj span a.AYHFM::text').get('').strip()
            try:
                states = re.findall(r'\b[A-Z]{2}\b', contact_detail)
                if len(states) == 2:
                    state = states[-1]
                else:
                    state = states[0]
            except:
                state = ''
            try:
                street = contact_detail.rsplit(state, 1)[0].strip().rstrip(',').strip()
            except:
                street = ''
            try:
                zip_code = re.search(r"(?!\A)\b\d{5}(?:-\d{4})?\b", contact_detail).group(0)
            except:
                zip_code = ''
            item['Street Address'] = street
            item['State'] = state
            item['Zip'] = zip_code
            item['Phone Number'] = response.css('span.AYHFM a.BMQDV::text').get('').strip()
            item['Business_Site'] = response.css('span.DsyBj a.YnKZo::attr(href)').get('').strip()
            item['Detail_Url'] = response.url
            item['Source_URL'] = 'https://www.tripadvisor.co.nz/'
            item['Lead_Source'] = 'tripadvisor'
            item['Meta_Description'] = ""
            item['Occupation'] = 'Restaurant'
            item['Record_Type'] = 'Business'
            item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield item

        except Exception as ex:
            print('Error From Parse_details | ' + str(ex))
