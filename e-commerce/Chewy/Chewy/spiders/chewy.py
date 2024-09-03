import csv
import json

import scrapy


class ChewySpider(scrapy.Spider):
    name = 'chewy'
    allowed_domains = ['chewy.com']
    listing_api = "https://www.chewy.com/plp/api/search?groupResults=true&count=36&include=items&fields%5B0%5D=STORE_DETAILS_WITH_MERCH_ASSOCIATIONS&omitNullEntries=true&catalogId=1004&from={}&sort=byRelevance&groupId={}"
    listing_headers = {
        '2022_04_lv_poc_holdout': 'ALL_ADS',
        '2023_02_aggregate_ratings': 'CONTROL',
        '2023_02_lv_one_plus_n_poc_ads': 'ONE_PLUS_N_CONTROL',
        '2023_02_lv_poc_ads_deals': 'ALL_ADS',
        '2023_03_related_searches': 'CONTROL',
        '2023_04_anonymous_pid_attribution': 'CONTROL',
        '2023_04_personalization_id_attribution': 'CONTROL',
        '2023_04_product_grid_experiment_1': 'VARIANT_01',
        '2023_04_vsn': 'VARIANT',
        'authority': 'www.chewy.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'adjustboostvalues': 'CONTROL',
        'adjustboostvalueswithnewfields': 'CONTROL',
        'autoredirecth1': 'CONTROL',
        'cache-control': 'no-cache',
        # 'cookie': 'pid=hp2r_3ubSoGDUDAJ-FM59Q_; ajs_anonymous_id=a4d14d8d-4adc-4319-96db-ff70fd95c7a3; _gcl_au=1.1.230926452.1683405239; _scid=7242a8d9-984d-44ea-8c77-5c823e28ab02; _mibhv=anon-1683405239957-9706766622_6593; _tt_enable_cookie=1; _ttp=Y18_XGN1e0S8u6j-cl2sKeYONXo; FPID=FPID1.2.9LSECmdJoL%2FNJRgbIdoqjmeSb9yR9H3y%2BGBXZsS6zqk%3D.1683405239; _pin_unauth=dWlkPU16UTBabU0xT0RZdFpHUmlaUzAwT1RJM0xXSXdaR0V0TVdGaE5EZGlPR1ZoWkdGbQ; _hjSessionUser_1727423=eyJpZCI6ImNmOWJiNTI5LTNmY2EtNTM1NC05OTdlLTdhOWVjYTIyYmFjMSIsImNyZWF0ZWQiOjE2ODM0MDUyNDAzNzUsImV4aXN0aW5nIjp0cnVlfQ==; addshoppers.com=2%7C1%3A0%7C10%3A1683405257%7C15%3Aaddshoppers.com%7C44%3AZmZkM2JiM2ViYWY1NDkxN2JjNDhjOGQ1NDEzZTk1MmQ%3D%7C06acd9cda406d01f1c1a032c57534dd378e902ed2e2b19179f195c9a5d0a5829; _gid=GA1.2.787073954.1683551197; FPLC=mGnTdGonshlR7FN90PjwxbY7IpzutQmlqxoXz9J21vYFXaVOee0PYRpX4m1pj9dBxVrhbOQItK17FrW7yjg3ghX%2Bw7Tx%2BZUKhkd0OxViP16KNq7xd2amuiGBPrifzQ%3D%3D; ajs_anonymous_id=a4d14d8d-4adc-4319-96db-ff70fd95c7a3; sid=2c233e60-1436-4353-bd46-0dd275fd8392; x-feature-preview=false; _abck=3A7C853A781C32091034C61D3BA9509B~0~YAAQlm8/Fw8DC/iHAQAASNYYBAnv9bhR238PIACgsxPFNfCVRL9LGH+MT4zX0X4WY3aIg10YfhsSCbsc7ZPwLxltRr1VzjF/HooTc6tmw0FzqGCm09sdbWyzo8wYuvERBtpDiPCEwIpOzhMJkzGAtfNcIdthETPUqwtyg6UCRC5OA2RFcuVZxrvdkUb0anr5R9l5ZA31nhTKnPpijiGt7fPjkGSia96bgC9hefgCpXl3LvdqvtewNxyXEMO9Ix3AAuJF0s5AFPAd0mgocJTWRR8CDaNAeLMLSS1SY5BEg4RRulsLcJSdwiDovIYmq3WiBpQ97r5hVu4Rck1S3DOvoaBD5Pi9/jE6b2eUGpKa8HN9YigIYMqq+lT6GUpn63h1qSrlWMtqmQXnEMFOl+CdU85ORqZaaAk=~-1~-1~-1; bm_sz=B0A324BD3ACB845D7591AEC227AC97BE~YAAQlm8/FxIDC/iHAQAASNYYBBMxtvDBNOwU52S+iDBAXXhF57ovYgi83IB2nWCoaPBPJwtrt2PPqIt9EBHQ+1eTYRnBW1L2Od1L0C1KuF84Osw+FZvZyFbZVRYLDvBuFGH1XReLul7giuHz4WIJxFhN/EjIwXX1YxbWhpweRdXVCicN6AhH+O1A1aVx2shVmcNjQVmTJpAUbDc/0JKWxMPglyq/6TW7D+hDUcNUPBt8F/UyDqtRL21jlUpNbLg06aRag919MBL+G5hCSrMhsAL7AdFbliStO1abCM3bC2QRmQ==~3356980~4605490; experiment_2022_09_HEADER_SITEWIDE_BANNER=VARIANT; experiment_2022_10_NAV_INGRESS_SUPERLATIVE=CONTROL; experiment_2023_03_HEADER_SIGN_IN_REDIRECT=VAR_2; experiment_2023_02_NAV_IA_TEST=CONTROL; abTestingAnonymousPID=hp2r_3ubSoGDUDAJ-FM59Q_; ak_bmsc=A754D822858E7F5F58103B69E67836E2~000000000000000000000000000000~YAAQlm8/FzUDC/iHAQAA2uQYBBPrbFNOANa2bkry2Tb4o3czayQhBT1P5Ji4vNz707WukkUR9JkDVKOvTnUQnN6hbzbFGuHWkMRPLKVIJG/WQ+SflMBWMxn68oXIsab+4Erq502oKJOWaIJgJD9XlbnjHTLpxVnOy2EkF8ZJK5PxvfU5lV4HPr+JRqQid8Opnys/Cq0+I7n+MIROaxDYABh1/XaVdQuNV+SqP5aP+2oyrQ0Dw82JOrI8DaPFiy3ESBxWQ5q7LqfIgdyW9+YaC9LlDKv5EP9KvUCOF3FbFAtlxoTjuS5lEEWy8Q1Po9ighjH7l/hAcvZBmWDV3hHL39/vW2UCoaaqa5ueh97urvojoPuVLTtkbXpuxC74yZd3TJojk/y8q2FsE9W6uEXn3SfPtHmV8NYgS1JgckINxY9IYKrgjQ/qp6dXW63L1ZjUdz8riu4wRdRrKeuXz7jLkQREyvFBYQadhCle2x+nvU75WoFO4yhPayC/rYDCnLP9dqn4aEcItlfGrC4AQJVp+lB1SBc=; experiment_=; AKA_A2=A; sbsd=00000000009ca8f7f4ce57ddad5be41f037d2f33cbaa4549297f792449dc5511c61931d23e93ee0844-5c00-49ab-a4b1-9c4df16f24671683700023; _hjIncludedInSessionSample_1727423=0; _hjSession_1727423=eyJpZCI6ImZkMzlhMDA3LTdiMzUtNGRjNi04ZTIyLWVjOWJiNzdmMjZjYiIsImNyZWF0ZWQiOjE2ODM2OTU5MjgxMjksImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; _hjHasCachedUserAttributes=true; AWSALB=1Sd9Xb5Vjn01lkCUEh+xLZLOfj4SqnNVgpJ+T9mhlkh6GI889rGJKcjWxkByKUKKpu229c+SF8G751G+h+LBEQmNkd/S8zIAhjQgooV8jerwqy7IZIkwPGMnJXaw; pageviewCount=101; bm_sv=09B648FFD37696FA091CA669CC495773~YAAQlm8/F/EHC/iHAQAA9gEaBBPs9/0YoPYdLsqF/0DFc+XjxWeVpcQdtj2TdA/oW2xENYC/9RjYHLfAV2AjfGrrIDZbksB2bIdmP5A3SMfu0KBP1lXGFxfXuwbtAfvdbyWa2wCIr0hXCzrgE3lCieVRWD4oE5fxhcwvA42bihabLmRHxVBaTgEUIr1MvdBPn5SkZeapEkKfOzpkhq50wlT+/wMyWrodYU9imvU7u5QQAYnTzRXSDxahi1FF1ngz~1; _scid_r=7242a8d9-984d-44ea-8c77-5c823e28ab02; _ga_GM4GWYGVKP=GS1.1.1683695924.11.1.1683695998.49.0.0; _uetsid=2a633c40eda111edbdf83f40685aecab; _uetvid=54e2b1e0ec4d11ed82e123b3c43a07cf; _dc_gtm_UA-23355215-1=1; _ga=GA1.2.1455861625.1683405239; _dc_gtm_UA-12345-6=1; _dc_gtm_UA-23355215-27=1; OptanonConsent=isGpcEnabled=0&datestamp=Wed+May+10+2023+10%3A20%3A02+GMT%2B0500+(Pakistan+Standard+Time)&version=202303.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=BG15%3A1%2CC0004%3A1%2CC0010%3A1%2CC0011%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1&AwaitingReconsent=false; cto_bundle=bVFjDF9DQyUyQmxLM0thYUZ2bUFRR2xXR2NMdWFaMEYlMkJDTjVMVmNtU0RkRXJKJTJGUmRRMkthWjY3a3c2YmtIRXFlYmFWWUZHaXpjUkxxb1lDM1lwazZodzdyNTdPbWl3dnE3ZXl4aE9oQUg4MENTJTJCV01KczhBRVp4TzBoOWtiMWh6eWRENFpVRlFUbTEwalV0Y01pVnJIYkttaGNNUSUzRCUzRA; akavpau_defaultvp=1683696312~id=36318e1c791193fa91f024f51f07d214; akaalb_chewy_ALB=1683696612~op=prd_chewy_plp:www-prd-plp-use1|prd_chewy_plp_sp:searchpilot-prd-use1|prd_chewy_lando:www-prd-lando-use1|chewy_com_ALB:www-chewy-use2|~rv=12~m=www-prd-plp-use1:0|searchpilot-prd-use1:0|www-prd-lando-use1:0|www-chewy-use2:0|~os=43a06daff4514d805d02d3b6b5e79808~id=9aa8664ba9a2fbc3a5d0574d936f9caa; RT="z=1&dm=www.chewy.com&si=fa46c427-b89c-4093-94cd-c94ad1b00c0c&ss=lhh92prw&sl=1&tt=1ike&rl=1&nu=25qz0e5m&cl=25fp"; _dd_s=rum=0&expire=1683696914342',
        'expanded_redirect_v2': 'VARIANT',
        'itemboost': 'VARIANT',
        'knnv2': 'CONTROL',
        'learntorank': 'CONTROL',
        'no_results_search_term_purchases': 'CONTROL',
        'pragma': 'no-cache',
        'productperformancev2': 'CONTROL',
        'queryexp2303': 'CONTROL',
        'referer': 'https://www.chewy.com/b/food-treats-1026',
        'removingunpublishedpages': 'CONTROL',
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'vectorsearch': 'CONTROL',
    }
    detail_url = "https://www.chewy.com/_next/data/chewy-pdp-ui-dTDWyslnu8a9/en-US/{}/dp/{}.json?id={}&slug={}"
    detail_headers = {
        'authority': 'www.chewy.com',
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        # 'cookie': 'pid=hp2r_3ubSoGDUDAJ-FM59Q_; ajs_anonymous_id=a4d14d8d-4adc-4319-96db-ff70fd95c7a3; _gcl_au=1.1.230926452.1683405239; _scid=7242a8d9-984d-44ea-8c77-5c823e28ab02; _mibhv=anon-1683405239957-9706766622_6593; _tt_enable_cookie=1; _ttp=Y18_XGN1e0S8u6j-cl2sKeYONXo; FPID=FPID1.2.9LSECmdJoL%2FNJRgbIdoqjmeSb9yR9H3y%2BGBXZsS6zqk%3D.1683405239; _pin_unauth=dWlkPU16UTBabU0xT0RZdFpHUmlaUzAwT1RJM0xXSXdaR0V0TVdGaE5EZGlPR1ZoWkdGbQ; _hjSessionUser_1727423=eyJpZCI6ImNmOWJiNTI5LTNmY2EtNTM1NC05OTdlLTdhOWVjYTIyYmFjMSIsImNyZWF0ZWQiOjE2ODM0MDUyNDAzNzUsImV4aXN0aW5nIjp0cnVlfQ==; addshoppers.com=2%7C1%3A0%7C10%3A1683405257%7C15%3Aaddshoppers.com%7C44%3AZmZkM2JiM2ViYWY1NDkxN2JjNDhjOGQ1NDEzZTk1MmQ%3D%7C06acd9cda406d01f1c1a032c57534dd378e902ed2e2b19179f195c9a5d0a5829; _gid=GA1.2.787073954.1683551197; FPLC=mGnTdGonshlR7FN90PjwxbY7IpzutQmlqxoXz9J21vYFXaVOee0PYRpX4m1pj9dBxVrhbOQItK17FrW7yjg3ghX%2Bw7Tx%2BZUKhkd0OxViP16KNq7xd2amuiGBPrifzQ%3D%3D; ajs_anonymous_id=a4d14d8d-4adc-4319-96db-ff70fd95c7a3; sid=2c233e60-1436-4353-bd46-0dd275fd8392; x-feature-preview=false; _abck=3A7C853A781C32091034C61D3BA9509B~0~YAAQlm8/Fw8DC/iHAQAASNYYBAnv9bhR238PIACgsxPFNfCVRL9LGH+MT4zX0X4WY3aIg10YfhsSCbsc7ZPwLxltRr1VzjF/HooTc6tmw0FzqGCm09sdbWyzo8wYuvERBtpDiPCEwIpOzhMJkzGAtfNcIdthETPUqwtyg6UCRC5OA2RFcuVZxrvdkUb0anr5R9l5ZA31nhTKnPpijiGt7fPjkGSia96bgC9hefgCpXl3LvdqvtewNxyXEMO9Ix3AAuJF0s5AFPAd0mgocJTWRR8CDaNAeLMLSS1SY5BEg4RRulsLcJSdwiDovIYmq3WiBpQ97r5hVu4Rck1S3DOvoaBD5Pi9/jE6b2eUGpKa8HN9YigIYMqq+lT6GUpn63h1qSrlWMtqmQXnEMFOl+CdU85ORqZaaAk=~-1~-1~-1; bm_sz=B0A324BD3ACB845D7591AEC227AC97BE~YAAQlm8/FxIDC/iHAQAASNYYBBMxtvDBNOwU52S+iDBAXXhF57ovYgi83IB2nWCoaPBPJwtrt2PPqIt9EBHQ+1eTYRnBW1L2Od1L0C1KuF84Osw+FZvZyFbZVRYLDvBuFGH1XReLul7giuHz4WIJxFhN/EjIwXX1YxbWhpweRdXVCicN6AhH+O1A1aVx2shVmcNjQVmTJpAUbDc/0JKWxMPglyq/6TW7D+hDUcNUPBt8F/UyDqtRL21jlUpNbLg06aRag919MBL+G5hCSrMhsAL7AdFbliStO1abCM3bC2QRmQ==~3356980~4605490; RT="z=1&dm=www.chewy.com&si=fa46c427-b89c-4093-94cd-c94ad1b00c0c&ss=lhh92prw&sl=0&tt=0"; experiment_2022_09_HEADER_SITEWIDE_BANNER=VARIANT; experiment_2022_10_NAV_INGRESS_SUPERLATIVE=CONTROL; experiment_2023_03_HEADER_SIGN_IN_REDIRECT=VAR_2; experiment_2023_02_NAV_IA_TEST=CONTROL; abTestingAnonymousPID=hp2r_3ubSoGDUDAJ-FM59Q_; ak_bmsc=A754D822858E7F5F58103B69E67836E2~000000000000000000000000000000~YAAQlm8/FzUDC/iHAQAA2uQYBBPrbFNOANa2bkry2Tb4o3czayQhBT1P5Ji4vNz707WukkUR9JkDVKOvTnUQnN6hbzbFGuHWkMRPLKVIJG/WQ+SflMBWMxn68oXIsab+4Erq502oKJOWaIJgJD9XlbnjHTLpxVnOy2EkF8ZJK5PxvfU5lV4HPr+JRqQid8Opnys/Cq0+I7n+MIROaxDYABh1/XaVdQuNV+SqP5aP+2oyrQ0Dw82JOrI8DaPFiy3ESBxWQ5q7LqfIgdyW9+YaC9LlDKv5EP9KvUCOF3FbFAtlxoTjuS5lEEWy8Q1Po9ighjH7l/hAcvZBmWDV3hHL39/vW2UCoaaqa5ueh97urvojoPuVLTtkbXpuxC74yZd3TJojk/y8q2FsE9W6uEXn3SfPtHmV8NYgS1JgckINxY9IYKrgjQ/qp6dXW63L1ZjUdz8riu4wRdRrKeuXz7jLkQREyvFBYQadhCle2x+nvU75WoFO4yhPayC/rYDCnLP9dqn4aEcItlfGrC4AQJVp+lB1SBc=; pageviewCount=100; _scid_r=7242a8d9-984d-44ea-8c77-5c823e28ab02; _ga_GM4GWYGVKP=GS1.1.1683695924.11.0.1683695924.60.0.0; _uetsid=2a633c40eda111edbdf83f40685aecab; _uetvid=54e2b1e0ec4d11ed82e123b3c43a07cf; experiment_=; AKA_A2=A; AWSALB=36u9jtvr7RTm4GEUq9giKyYmm7ru4ubSTjA+emS3ss7FBC0Xkd/PE4hussmWlrFBjZD+myx81HomXuWKnWM3WPCoo+tw3lRu+dTt79B+UYDXwWBmdQxS6B5Ddsot; sbsd=00000000009ca8f7f4ce57ddad5be41f037d2f33cbaa4549297f792449dc5511c61931d23e93ee0844-5c00-49ab-a4b1-9c4df16f24671683700023; cto_bundle=okQoO19DQyUyQmxLM0thYUZ2bUFRR2xXR2NMdVR2WkJCamROZVIzUWhsJTJCZ0ZFU3hJeXJhWmklMkZRbG1LUVQ3NDdOalY4dWs1SmpLZFNzdGJOcWxtQVNyZXJyVUFNY2hyT0VKdE1pWnUxYm1mcHFmYkJSY2RBUUFsNW4zOFpIMkRnSlA1eVpTSHMyMzZmVjQxVHd0Q1BOeWtyUSUyRmMwQSUzRCUzRA; _dc_gtm_UA-23355215-1=1; _ga=GA1.2.1455861625.1683405239; _dc_gtm_UA-12345-6=1; _dc_gtm_UA-23355215-27=1; _hjIncludedInSessionSample_1727423=0; _hjSession_1727423=eyJpZCI6ImZkMzlhMDA3LTdiMzUtNGRjNi04ZTIyLWVjOWJiNzdmMjZjYiIsImNyZWF0ZWQiOjE2ODM2OTU5MjgxMjksImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; _hjHasCachedUserAttributes=true; OptanonConsent=isGpcEnabled=0&datestamp=Wed+May+10+2023+10%3A18%3A50+GMT%2B0500+(Pakistan+Standard+Time)&version=202303.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=BG15%3A1%2CC0004%3A1%2CC0010%3A1%2CC0011%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1&AwaitingReconsent=false; akavpau_defaultvp=1683696229~id=81d284ce0031b2a686eccbb9cf96f2cd; bm_sv=09B648FFD37696FA091CA669CC495773~YAAQlm8/F3gDC/iHAQAAPAoZBBOJN4QMIL99dfuGJF1s4jXu16QNWfHuvmEnJEYpBX7Fb3cWLC5cbOk2lD4YVOkXmnuZR4teM3ehcIsx6qYHRUoeXJjF/UXYZhRfOaNjJTWBFKXse6ohiM88Pp09C+01bnxXSCUHINbXHkUCrAdd3iCF3Cgr4DsPNrI0x3AO6Ycv+5Im8jJ65vnRLPqESmIzjmlBizFLxLG6pqLxb0H9s8vSQIzWVpyKlrlmEq7j~1; akaalb_chewy_ALB=1683696530~op=prd_chewy_lando:www-prd-lando-use1|chewy_com_ALB:www-chewy-use2|~rv=12~m=www-prd-lando-use1:0|www-chewy-use2:0|~os=43a06daff4514d805d02d3b6b5e79808~id=be9d1e306036ccca90e41e2a1d008766; _gali=kib-chip-choice-3',
        'pragma': 'no-cache',
        'referer': 'https://www.chewy.com/flukers-5-star-medley-freeze-dried/dp/347404',
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'x-nextjs-data': '1',
    }
    original_url = 'https://www.chewy.com/{}/dp/{}'
    flavor = ''
    flavor_name = ''
    Attr1 = ''
    custom_settings = {
        'FEED_URI': 'chewy.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORT_FIELDS': ['SKU', 'SKU_2', 'Type', 'Brand', 'Name', 'Weight (Pounds)', 'Weight (kg)',
                               'Sale price/ AutoShip Price',
                               'Regular price', 'Categories', 'Parent', 'Parent_2', 'Attribute 1 name',
                               'Attribute 1 value(s)',
                               'Attribute 2 name', 'Attribute 2 value(s)',
                               'Attribute 3 name', 'Attribute 3 value(s)', 'Attribute 4 name',
                               'Attribute 4 value(s)', 'Attribute 5 name', 'Attribute 5 value(s)', 'Attribute 6 name',
                               'Attribute 6 value(s)', 'Attribute 7 name',
                               'Attribute 7 value(s)', 'Attribute 8 name',
                               'Attribute 8 value(s)', 'Attribute 9 name',
                               'Attribute 9 value(s)', 'Attribute 10 name',
                               'Attribute 10 value(s)', 'Attribute 10 name',
                               'Attribute 11 value(s)', 'origin URL', 'URL Suffix',
                               'Details', 'Nutritional Info', 'Feeding Instructions', 'FAQ', 'Key Benefits',
                               'Images', 'Scraping_Category_Url']
    }
    got_data = []
    done_data = []
    parent_sku = ''
    parent_2 = 0
    sku_2 = ''
    parent_2_v1 = ''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_url = self.read_csv()

    def read_csv(self):
        with open('chewy_category.csv', encoding='utf-8', errors='ignore') as csv_file:
            return list(csv.DictReader(csv_file))

    def start_requests(self):
        i = 1
        for url in self.request_url[1:5]:
            cate_url = url.get('working category')
            _id = cate_url.split('/')[-1].split('-')[-1].strip()
            request_terms = {'start': 0,
                             'id': _id,
                             're': i,
                             'Category_url': cate_url}
            yield scrapy.Request(url=self.listing_api.format(0, _id), headers=self.listing_headers,
                                 meta={'request_data': request_terms})
            i = i + 1

    def parse(self, response):
        json_data = json.loads(response.body)
        request = response.meta['request_data']
        for data in json_data.get('products', []):
            if self.parent_2 == 0 and request.get('re') == 1:
                parent = 1000
                self.parent_2 = 1000
            else:
                parent = self.parent_2
            slug = data.get('href', '').split('/dp')[0].split('/')[-1]
            detail_id = data.get('catalogEntryId', '')
            if detail_id:
                yield scrapy.Request(url=self.detail_url.format(slug, detail_id, detail_id, slug),
                                     headers=self.listing_headers,
                                     callback=self.parse_details, meta={'i': 1,
                                                                        'parent': parent,
                                                                        'parent_1': detail_id,
                                                                        'brands': '',
                                                                        'Category_url': request.get('Category_url')})

        total_records = json_data.get('recordSetTotal', '')
        current_skip = request.get('start', 0)
        next_start = current_skip + 36
        if next_start < int(total_records):
            request['start'] = next_start
            yield scrapy.Request(url=self.listing_api.format(next_start, request.get('id')),
                                 headers=self.listing_headers,
                                 meta={'request_data': request})

    def parse_details(self, response):
        json_data = json.loads(response.body)
        product = json_data.get('pageProps', {}).get('__APOLLO_STATE__', {})
        item_keys = []
        items_need_to_get = []
        slug = ''
        aa = response.meta['i']
        required_key = ''
        ProductEnsemble = dict()
        parent11 = response.meta['parent_1']
        brandsss = response.meta['brands']
        for key in product:
            if 'Product' in key and 'ProductEnsemble' not in key:
                part_number = product[key].get('partNumber', '')
                for raw_key in product[key].get('items', []):
                    item_keys.append(raw_key.get('__ref', ''))
                part_slug = product[key].get('slug', '')
                if part_number:
                    required_key = key
                    if part_slug:
                        slug = part_slug
            else:
                if 'ProductEnsemble' in key:
                    ProductEnsemble = product[key]
        new_parent = response.meta['parent']
        Category_url = response.meta['Category_url']
        for item in item_keys:
            item_detail = product[item]
            check_name = item_detail.get('name', '')
            if aa == 1:
                productss, parents, parent1, brandss = self.get_main_product(product, required_key, ProductEnsemble, Category_url)
                parent11 = parent1
                brandsss = brandss
                new_parent = parents
                yield productss
            aa = aa + 1
            if check_name:
                item_entry_id = item_detail.get('entryID', '')
                if item_entry_id not in self.done_data:
                    items_data = self.get_item_data(item_detail, product, slug, new_parent, parent11, brandsss, Category_url)
                    self.done_data.append(item_entry_id)
                    yield items_data
            else:
                item_entry_ids = item_detail.get('entryID', '')
                if item_entry_ids not in self.got_data:
                    items_need_to_get.append(item_entry_ids)
                    self.got_data.append(item_entry_ids)
        if any(items_need_to_get):
            for item_id in items_need_to_get:
                yield scrapy.Request(url=self.detail_url.format(slug, item_id, item_id, slug),
                                     headers=self.listing_headers,
                                     callback=self.parse_details, meta={'i': aa,
                                                                        'parent': new_parent,
                                                                        'parent_1': parent11,
                                                                        'brands': brandsss,
                                                                        'Category_url': Category_url})

    def get_item_data(self, item_dict, product, slug, newparent, parent_1, brand, cate):
        item = dict()
        item['SKU'] = item_dict.get('entryID', '')
        item['Name'] = item_dict.get('name', '')
        pounds = item_dict.get('weight', '')
        if pounds:
            item['Weight (Pounds)'] = pounds.replace('pounds', '').replace('ounces', '').replace('pound', '').replace(
                'ounce', '').strip()
            item['Weight (kg)'] = float(item['Weight (Pounds)']) * 0.453592
        item['Sale price/ AutoShip Price'] = item_dict.get('autoshipPrice', '')
        item['Regular price'] = item_dict.get('mapPrice', '')
        item['Parent'] = parent_1
        item['Scraping_Category_Url'] = cate
        if ',' in item['Name']:
            size = item['Name'].split(',')[-1].strip()
        else:
            size = ''
        new_flavor = ''
        if len(self.flavor.split(',')) > 1:
            for flavor in self.flavor.split(','):
                if flavor in item['Name']:
                    new_flavor = flavor
        else:
            new_flavor = self.flavor
        if size:
            item[f'Attribute 1 name'] = self.Attr1
            item[f'Attribute 1 value(s)'] = size
        else:
            item[f'Attribute 1 name'] = self.Attr1
            item[f'Attribute 1 value(s)'] = size
        if new_flavor:
            item[f'Attribute 2 name'] = self.flavor_name
            item[f'Attribute 2 value(s)'] = new_flavor
        i = 3
        for attribute in item_dict.get('descriptionAttributes', []):
            item[f'Attribute {i} name'] = attribute.get('name', '')
            value = attribute.get('values', [])[0].get('__ref')
            item[f'Attribute {i} value(s)'] = value.split('"value":')[-1].split('}')[0].replace('"', '').strip()
            i = i + 1
        item['origin URL'] = self.original_url.format(slug, item['SKU'])
        item['URL Suffix'] = slug
        item['Details'] = item_dict.get('description', '')
        item['Brand'] = brand
        Nutritional = dict()
        feeling = dict()
        faq = dict()
        for data in item_dict.get('infoGroups', []):
            name = data.get('name', '')
            if "Nutritional Info" in name:
                Nutritional = data
            if "Feeding Instructions" in name:
                feeling = data
            if "FAQ" in name:
                faq = data
        item['Nutritional Info'] = ' | '.join(
            nutri.get('usage', '') + ' | ' + nutri.get('content', {}).get('content', '') for nutri in
            Nutritional.get('sections', []))
        item['Feeding Instructions'] = ' | '.join(
            feel.get('usage', '') + ' | ' + feel.get('content', {}).get('content', '') for feel in
            feeling.get('sections', []))
        item['FAQ'] = ' | '.join(
            fa.get('usage', '') + ' | ' + fa.get('content', {}).get('content', '') for fa in
            faq.get('sections', []))
        item['Key Benefits'] = ' | '.join(benefit for benefit in item_dict.get('keyBenefits', []))
        item['Images'] = ' | '.join(image.get('url({"maxHeight":630})', '') for image in item_dict.get('images', []))
        item['Type'] = 'Variation'
        self.sku_2 = newparent
        item['SKU_2'] = self.parent_2_v1 + 1
        self.sku_2 = item['SKU_2']
        self.parent_2_v1 = item['SKU_2']
        item['Parent_2'] = newparent
        self.parent_2 = item['SKU_2'] + 1
        return item

    def get_main_product(self, raw_product, req_key, ProductEnsemble, cate):
        product = raw_product[req_key]
        category_key = []
        for key in raw_product:
            if 'Breadcrumb' in key:
                category_key.append(key)
        product_dict = dict()
        product_dict['SKU'] = product.get('entryID', '')
        product_dict['Name'] = product.get('name', '')
        product_dict['Parent'] = ''
        product_dict['Scraping_Category_Url'] = cate
        product_dict['Categories'] = '>'.join(raw_product.get(crumb, {}).get('name', '') for crumb in category_key)
        self.parent_sku = product_dict['SKU']
        iss = 1
        if product.get('slug', ''):
            product_dict['origin URL'] = self.original_url.format(product.get('slug', ''), product_dict['SKU'])
            product_dict['URL Suffix'] = product.get('slug', '')
        if product.get('attributes({"usage":["DEFINING"]})', {}):
            for attribute in product.get('attributes({"usage":["DEFINING"]})', {}):
                attr = attribute.get('__ref', '')
                name = raw_product.get(attr, {}).get('name', '')
                if 'Size' in name:
                    self.Attr1 = name
                    product_dict[f'Attribute {iss} name'] = raw_product.get(attr, {}).get('name', '')
                    value = ','.join(
                        values.get('__ref', '').split('"value":')[-1].split('"}')[0].replace('"', '').strip() for values in
                        raw_product.get(attr, {}).get('values', []))
                    product_dict[f'Attribute {iss} value(s)'] = value
                    iss = iss + 1
                else:
                    product_dict[f'Attribute {iss} name'] = raw_product.get(attr, {}).get('name', '')
                    self.Attr1 = raw_product.get(attr, {}).get('name', '')
                    value = ','.join(
                        values.get('__ref', '').split('"value":')[-1].split('"}')[0].replace('"', '').strip() for values in
                        raw_product.get(attr, {}).get('values', []))
                    product_dict[f'Attribute {iss} value(s)'] = value
                    iss = iss + 1
        else:
            self.Attr1 = 'Size'
        if ProductEnsemble:
            pro_name = ProductEnsemble.get('selectorDisplayName', '')
            if 'Flavor' in pro_name:
                product_dict[f'Attribute 2 name'] = pro_name
                value = ','.join(
                    values.get('displayName', '') for values in ProductEnsemble.get('selectorProducts', []))
                product_dict[f'Attribute 2 value(s)'] = value
                self.flavor = value
                self.flavor_name = pro_name
            else:
                product_dict[f'Attribute 2 name'] = pro_name
                value = ','.join(
                    values.get('displayName', '') for values in ProductEnsemble.get('selectorProducts', []))
                product_dict[f'Attribute 2 value(s)'] = value
                self.flavor = value
                self.flavor_name = pro_name
        product_dict['Type'] = 'Variable'
        product_dict['SKU_2'] = self.parent_2
        brand = product.get('manufacturerName', '')
        product_dict['Brand'] = brand
        self.sku_2 = self.parent_2
        self.parent_2_v1 = self.parent_2
        return product_dict, self.parent_2_v1, product_dict['SKU'], brand
