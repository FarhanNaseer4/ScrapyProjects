from datetime import datetime

import scrapy


class JaredSpider(scrapy.Spider):
    name = 'jared'
    start_urls = ['https://www.jared.com/store-finder/view-all-states']
    custom_settings = {
        'FEED_URI': 'jared.csv',
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
        'authority': 'www.jared.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'cookie': 's_ecid=MCMID%7C83134814953341369902987713296879003737; _dy_c_exps=; kndctr_700CFDC5570CBFE67F000101_AdobeOrg_identity=CiY4MzEzNDgxNDk1MzM0MTM2OTkwMjk4NzcxMzI5Njg3OTAwMzczN1IPCPPF84PRMBgBKgRJUkwx8AHzxfOD0TA=; _dy_c_att_exps=; _dycnst=dg; _dyid=36029661974792586; _dycst=dk.w.c.ws.; _dy_geo=PK.AS.PK_PB.PK_PB_Gujrat; _dy_df_geo=Pakistan..Gujrat; 65558=false; _rdt_uuid=1671018906488.96f7b257-9124-4f8c-a030-4a06cb4c93ba; _gid=GA1.2.114912336.1671018907; _gcl_au=1.1.1991822380.1671018907; _scid=839d9a07-b9f0-44c6-8237-8d8120782b30; _mibhv=anon-1671018907872-8077487227_4965; usi_return_visitor=Wed%20Dec%2014%202022%2016%3A55%3A10%20GMT%2B0500%20(Pakistan%20Standard%20Time); __attentive_id=d98eb1e79190438a9dcdad7804f073be; _attn_=eyJ1Ijoie1wiY29cIjoxNjcxMDE4OTE0ODg4LFwidW9cIjoxNjcxMDE4OTE0ODg4LFwibWFcIjoyMTkwMCxcImluXCI6ZmFsc2UsXCJ2YWxcIjpcImQ5OGViMWU3OTE5MDQzOGE5ZGNkYWQ3ODA0ZjA3M2JlXCJ9In0=; __attentive_cco=1671018914892; mdLogger=false; kampyle_userid=71c4-5bf1-9f6c-07fd-3e76-f733-0e64-977a; crl8.fpcuid=5babb708-adb7-4913-ba3e-a09b4fae5330; syte_uuid=2c278b10-7ba6-11ed-9c15-410cf3198dc4; notice_behavior=none; __attentive_dv=1; _pin_unauth=dWlkPU16UTBabU0xT0RZdFpHUmlaUzAwT1RJM0xXSXdaR0V0TVdGaE5EZGlPR1ZoWkdGbQ; jared-cart=c999e50e-522f-4735-b4c1-c998e382214d; _dyid_server=36029661974792586; jared_compareProductIds=; jared_compareProducts=; currentMoveToBagCount=0; _sctr=1|1670958000000; LPVID=Y5MjM3YTlhNTVhMWM3ODdm; btIdentify=cb90d18e-e210-4757-d123-fbaf554f75bd; G_ENABLED_IDPS=google; JSESSIONID=B90B7B5F1F5DA72321CB571FEEC9CB68.accstorefront-578c96fcfc-xcvnd; ROUTE=.accstorefront-578c96fcfc-xcvnd; akaalb_prod-jared=1671093590~op=:~rv=85~m=~os=1f2f3e33771e07b3045bd0ccbe2c164e~id=46d4433fb460831b3fd2225bdb11e14e; _abck=A4A4181A5A77235A89EA315D5F816F45~0~YAAQlr3XF8Q9ehGFAQAAHay5FAlfQRiX5LNI55gkGCOhPrSs6ZwOvbU/koYEuPYzOd/2T31mDbtNdN8NxDBn+l9fGnLszxTZr6QQ7GLExJ3/udJJrT+VJXeb17qSb8QK5Iv9Ti/tAUL/AK6wCsVEeUNN+eEMLQORvJxGRB7Tth5hGQRhTrxHUDC89nYNVW7/NM04XcaLX+pfnO9b9V2BvPdJPfpYN/AdrGBll/lQWdP+nRajLfhjV/rWXZ6unN3UhY3kxWTwmY1vPNZtJ2eJYveVaULzZ7gaMSqnTzCyp0FpoJJqgGruEbfBDAiVSiMGucn5KSVwg8+8KqO5DfiT0s3f1uMxYmty+vSSpmQarQoSDVW/Lo96LDGI0x+XGv++E3EephIZkcsT/R0jsJPNPWVBI+ja4MI=~-1~-1~-1; bm_sz=56259F0EA3C1E165A68820F1F54F1C98~YAAQlr3XF8c9ehGFAQAAHay5FBLU5XDVHDyOOKSAZI4yWdJvQ7JQayBTopzMydA55PNUGoPFldB4fd384aMJ0DQPbgFxa2dSl9SxxPwG0mSq1qZoQP1Z3srTTz6t9sic9qOe1WUirnaSllG+6AN5ifM584sT8rIK0cbYS1ee0lUtFo3Qo1n71nPlipoUFOxQ+YbcTXG1f86APsVZrOCt3B8KLyotyPQ7qh2Syb8xmDIBl1nIWspl2pynXnEn3oebv9m93axzuSwt2nnwLJPVfFPYCWsvgxxyRRfl/ekY/Ynw/w==~3356726~3158582; ak_bmsc=D9B92ED3F7F4B75BD9A96C28E270F362~000000000000000000000000000000~YAAQlr3XF8U/ehGFAQAAFdW5FBIqdMFu1iOGQlg+qjXYezKt1jIBtNHmYFtlc1d+h+HCXAAF1kdVh45nIhhx3OskKIR6VUOcCpQponkfFUHReigUBdOhUvfefNFvBr5xzW+djGqqyp//CvSmrfGgT2LeUAe/MmOOqNZfXR+ISsHnudcYeMzL4x/098MR3pUo1UqGEWQAVCTLKximbH6MkN/hikNzC/QdBfqLV+koGSURErp10exzOrdhkdNrSE8v75syK0ElK4WTO4GhjdUlEHci7nRtvN6wx/hg4n0n7OcQjUQyRxsyUKKCLKCRCULi1QI3ZjNxGVv0AVbjdy6s1iPP/ZQ/D8JRT3emT12qRFL2YxoMXXpBsgS6aWu7sqUOn6j2souvgpxD3LmulEjS1tGNCPG02avwgWQaztEiS0XxBBKrmMAwixyhzretH2a6bRvnIySCNcUBaPWR5DraWUy+YPON8U5jP5pCSQnahw/2dRy84JoJ; _dy_ses_load_seq=23250%3A1671090002209; _dy_csc_ses=t; _dyjsession=f65238cf25eb6d563c9ad5f99ea61bd3; dy_fs_page=www.jared.com%2Fstore-finder%2Fview-all-states; _dy_lu_ses=f65238cf25eb6d563c9ad5f99ea61bd3%3A1671090004130; _dy_toffset=-2; kndctr_700CFDC5570CBFE67F000101_AdobeOrg_cluster=irl1; _dy_soct=562868.1087015.1671090002*609638.1177432.1671090002*658189.1265811.1671090002*531209.1016160.1671090002*542389.1044426.1671090002*543783.1047291.1671090002*631761.1220731.1671090002*540585.1040630.1671090005; AMCVS_700CFDC5570CBFE67F000101%40AdobeOrg=1; AMCV_700CFDC5570CBFE67F000101%40AdobeOrg=690614123%7CMCIDTS%7C19341%7CMCMID%7C83134814953341369902987713296879003737%7CMCAAMLH-1671694806%7C3%7CMCAAMB-1671694806%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1671097207s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C3.1.0; gpv_pn=View%20All%20Store%20%7C%20Jared%20%7C%20Jared; s_cc=true; rowId=; da_sid=398418168E33AE886E85AA13B142C1E89E.0|4|0|3; da_lid=FCDA38579A72EA13C162BB99F3444F3F42|0|0|0; da_intState=; s_gnr=1671090011954-Repeat; stimgs={%22sessionId%22:54777183%2C%22didReportCameraImpression%22:true%2C%22newUser%22:false}; sailthru_pageviews=1; _ga_J5F638VNBH=GS1.1.1671090166.2.0.1671090166.60.0.0; kampyleUserSession=1671090167426; kampyleUserSessionsCount=2; kampyleSessionPageCounter=1; sailthru_content=7c804d99d6217cb821a272c385510d9342e0b5019e5826bbfab35658a6e1690e72fe94ecd8af4210fa8bb33c509897b3; sailthru_visitor=4e4cc17a-cf2b-4011-bdf7-5612acb6ec9f; _uetsid=2c45abc07ba611edbcc88fbd59eec33b; _uetvid=2c45fb107ba611eda28af7b4580b478c; _ga=GA1.2.654511325.1671018907; _gat_gtag_UA_28633046_5=1; _bts=c77b249d-af7c-4692-953a-bc22a459e3b4; smtrrmkr=638066869681790931%5E0185107d-4053-4ed0-b169-7668399f9328%5E018514bc-6173-41b1-a408-d7db39cbc3e7%5E0%5E59.103.102.9; tpc_a=ef2f25fd675249f7ab7c1e29c0d9ece7.1671018914.qTx.1671090168; _bti=%7B%22app_id%22%3A%22jared%22%2C%22bsin%22%3A%223FjBokySmRJLpgjDaFjwlf4iM6orhPZGDjN3Rw%2BYMkKrmYuCxHV9Jc1Jdi0tdDuDlPlhJLjF2T6%2BZOsDKOngaQ%3D%3D%22%2C%22is_identified%22%3Afalse%7D; __attentive_pv=1; __attentive_ss_referrer="ORGANIC"; akavpau_prod_jared_vp=1671090472~id=5178b405c45fc73fc9ca29b88e41c33d; bm_sv=AC6DACA84D495CF95AA5768BBFFEB050~YAAQlr3XF6pxehGFAQAAl3O8FBIqT4I4MPO8bBB00L7taSc/jgDQIwORAmoFGy1scEMI0Nny8sYbfrNopX/haSPgmvWN8GrkPXLaPlyrMLmAQGBhzNTpxpxCHtY+CvCssfoBcGFnMErObASJScxEa7YWa60tCf2cE0Iq0YCZagWucND98ExhSFGn34WNSh9tboHF8DiE/5xs2gXDueaddQVrwF4pNhXeLQsg/7T8lzoBCDf2ByAIxWeVL2QfM90=~1',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse, headers=self.headers)

    def parse(self, response):
        for data in response.css('div.col-lg-3 div a'):
            list_url = data.css('::attr(href)').get()
            if list_url:
                yield response.follow(url=list_url, callback=self.listing_page, headers=self.headers)

    def listing_page(self, response):
        for data in response.css('div.viewstoreslist a'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.parse_detail, headers=self.headers)

    def parse_detail(self, response):
        item = dict()
        item['Business Name'] = response.css('h1[itemprop="name"]::text').get('').strip()
        item['Street Address'] = response.css('span[itemprop="streetAddress"]::text').get('').strip()
        item['State'] = response.css('span[itemprop="addressRegion"]::text').get('').strip()
        item['Zip'] = response.css('span[itemprop="postalCode"]::text').get('').strip()
        item['Phone Number'] = response.css('span[itemprop="telephone"] a::text').get('').strip()
        item['Source_URL'] = 'https://www.jared.com/store-finder/view-all-states'
        item['Occupation'] = 'Jared Store'
        item['Lead_Source'] = 'jared'
        item['Detail_Url'] = response.url
        item['Record_Type'] = 'Business'
        item['Meta_Description'] = ""
        item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield item

