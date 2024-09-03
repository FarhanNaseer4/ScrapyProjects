# -*- coding: utf-8 -*-
import json

import io
import re

import PyPDF2
import urllib.request

import scrapy


class ComplianceHealthSpider(scrapy.Spider):
    name = 'compliance_health'
    request_api = 'https://0dade6c7fc79428386ebd8d83fc2ca11.pbidedicated.windows.net/webapi/capacities/0DADE6C7-FC79-4283-86EB-D8D83FC2CA11/workloads/QES/QueryExecutionService/automatic/public/query'
    payload = "{\"version\":\"1.0.0\",\"queries\":[{\"Query\":{\"Commands\":[{\"SemanticQueryDataShapeCommand\":{\"Query\":{\"Version\":2,\"From\":[{\"Name\":\"l\",\"Entity\":\"ARTG\",\"Type\":0},{\"Name\":\"s\",\"Entity\":\"SPONSOR\",\"Type\":0},{\"Name\":\"p1\",\"Entity\":\"PRODUCT_NAMES\",\"Type\":0},{\"Name\":\"c\",\"Entity\":\"CMI_DOC_URL\",\"Type\":0},{\"Name\":\"p\",\"Entity\":\"PI_DOC_URL\",\"Type\":0},{\"Name\":\"f\",\"Entity\":\"FORMULATION_COMBINED\",\"Type\":0},{\"Name\":\"a\",\"Entity\":\"ARTG Measures\",\"Type\":0},{\"Name\":\"g\",\"Entity\":\"Global_ARTG\",\"Type\":0},{\"Name\":\"t\",\"Entity\":\"THERAPEUTIC TYPES\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"s\"}},\"Property\":\"Sponsor Name\"},\"Name\":\"SPONSOR.Sponsor Name\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"p1\"}},\"Property\":\"PRODUCT_NAMES\"},\"Name\":\"PRODUCT_NAMES.PRODUCT_NAMES\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"l\"}},\"Property\":\"ARTG ID\"},\"Name\":\"ARTG.ARTG_ID\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"l\"}},\"Property\":\"PubSummLink\"},\"Name\":\"ARTG.PubSummLink\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"c\"}},\"Property\":\"CMI_Link_Alias\"},\"Name\":\"CMI_DOC_URL.CMI_Link_Alias\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"p\"}},\"Property\":\"PI_Link_Alias\"},\"Name\":\"PI_DOC_URL.PI_Link_Alias\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"f\"}},\"Property\":\"Active Ingredient\"},\"Name\":\"FORMULATION_COMBINED.Active Ingredient\"},{\"Aggregation\":{\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"l\"}},\"Property\":\"Public Summary\"}},\"Function\":3},\"Name\":\"Min(ARTG.Public Summary)\"},{\"Aggregation\":{\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"c\"}},\"Property\":\"PICMI_URL\"}},\"Function\":3},\"Name\":\"Min(CMI_DOC_URL.PICMI_URL)\"},{\"Aggregation\":{\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"p\"}},\"Property\":\"PICMI_URL\"}},\"Function\":3},\"Name\":\"Min(PI_DOC_URL.PICMI_URL)\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"a\"}},\"Property\":\"SearchResultTitleWithDate\"},\"Name\":\"ARTG Measures.SearchResultTitleWithDate\"}],\"Where\":[{\"Condition\":{\"Contains\":{\"Left\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"g\"}},\"Property\":\"SearchValues\"}},\"Right\":{\"Literal\":{\"Value\":\"'emergo'\"}}}}},{\"Condition\":{\"Not\":{\"Expression\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"t\"}},\"Property\":\"Therapeutic Type\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"null\"}}]]}}}}}],\"OrderBy\":[{\"Direction\":2,\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"l\"}},\"Property\":\"ARTG ID\"}}}]},\"Binding\":{\"Primary\":{\"Groupings\":[{\"Projections\":[0,1,2,3,4,5,6,7,8,9]}]},\"Projections\":[10],\"DataReduction\":{\"DataVolume\":3,\"Primary\":{\"Window\":{\"Count\":1120}}},\"SuppressedJoinPredicates\":[7,8,9],\"Version\":1},\"ExecutionMetricsKind\":1}}]},\"QueryId\":\"c7af7a59-89d7-23c5-b607-38747e87dccb\",\"ApplicationContext\":{\"DatasetId\":\"bb96794b-d5ba-4fb8-878d-99d2e6a38b6d\",\"Sources\":[{\"ReportId\":\"5e35910e-c7d6-4f3e-8f84-3ddf37dee369\",\"VisualId\":\"7ea69149dad7d9203e97\"}]}}],\"cancelQueries\":[],\"modelId\":2712489,\"userPreferredLocale\":\"en-GB\"}"
    headers = {
        'authority': '0dade6c7fc79428386ebd8d83fc2ca11.pbidedicated.windows.net',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'activityid': '8ececf28-0f87-4ad8-a0c8-a065d3cb9364',
        'authorization': 'MWCToken eyJhbGciOiJodHRwOi8vd3d3LnczLm9yZy8yMDAxLzA0L3htbGVuYyNyc2Etb2FlcCIsImVuYyI6IkExMjhDQkMtSFMyNTYiLCJraWQiOiI1MjFFM0FEQjEyMDUxMTk4MjEwRjlGMjIzRjU3MzZDODBDMTZBMDJEIiwidHlwIjoiSldUIn0.d95zudqJPTM6AdsbTdPV4bbPTe1fCTgt9VHOM4kCR8ObkfgfyAb-Rm4kxTpLLNwsVU2jmKL1uPOli8s-_H30fVw77JXR5chlOBr5GpCkV6byYSVWJ2F9qT4jET12tFoA4iF9F_HfKTyN4E-Dkp9bW5aOivmBER40d0fq3wCyl2hXFjdNCuc1QvdQQj5ffXIwZ2fPi98-5R-3CGyMVgXSh53yB0PAx43we9Up7nWaEfx47SNdm16QpHeKFckQM3fcF9cW7-GROaRp1tvQYzBNqvszzlJuKSkZ5RcDOrkkzTZx_EUcy19i_nurBpw2BT05aP2lS6BPaWiN0ZLeEt8kCg.CneOWF_Ba8ICN5SCR52QFA.UWcMnlpx4KTkqD_bkoOXBUtAV1YJogFUiAUxaEivNG0pEYEPaLhXVdRj1iG2zG8y5L-jisvuw4oU6T4waA4FjhKP18WF4rpppueOnpjkJX5q6Z6BhWV1Z83oqJQ2uXxYQmKrs9siS27_s5-FfeoXxxocCpqgRd9aHb_RmUUFIgm4mINtZXmFfKEny2aQoX3ZwWMKGR1RGB62STJC6StFx9wbjhfKwL8nmo1t-HSvzhHwBIXPO-zuUrtmpCccq9kzsAQ4sqpTfh8AwPa9YN-EyvPPv2C034FXda0c-iiZAwijIM6Ge-Fg-XK16gIKwmjAFofsX9veBkkj9Pgnr1vFaIHcy6Ae9B_4Miv2tW2RRl0NIN2RJyRXsCvt8Qu8t2RWZ0mZGf1ndJqiz_mfc108rvClwn2kmupZ0fmO7fJC5jDhwTWk0EZk6WKdbSZVj5lfPWv4pV9X5_WuPIUL3ZSneaP_WfLqNyQ5g_kZ1tcJ0SbaqgZ_RaNpPtIsn_Ygj0RH7txQwR7nyB45aN2GFaSWhgMl-4iW9S-98KL7Tlzz68URRboH8kY8TFo7QF5nsMRJeozqaY2tt9Pm0xdhJNBCeljE8s-mZiHhimkKS7aeJXXg3AnNodtDbmgUFTjO1_uBmMPcKqJtcz0jW10xY3WhJ2VD7wBvXigRrTh0Z0icYSMXSW5ypuFzdjL3TazReqigy3jg9sZZI3vCceF6ieWIN1V0ubOG2t3_9SU6uYXZeIf6dk5FwawohYsRuAGQqe1Qw85gMuYE_L0rlSFlM-uICeqJub7OgG_F9_DUDEA9cJFDxCMWnnxd8mN9XwOQppjSl1IuPcGnNx3HJj8nSZE8zFjQZrVg44-Gptag06XqPjSCr0jk7c7a291LVM1rzgebSI_5jaUcI8r-pZfK67bKwo7JoQhTjcTDtH1zgSnboJv10FF5Dts7R75gFJs9CZrQpZi2Nu6LAk1YC_VF0-Qr2P3OEuXbNNPoLyPbI3s6br0w85n24jqjuEL_Wq6LQEf2p-qurJYKhftD0iHBuzduosU2yoIINEP6WxaB2c4hdggmPGxxEkrpWgpIj1VhWkpaIqgHEP0hd6eUVef2fgPnv4k3JrohVig0V27Ds6Toij4BaswbeNoTemVBimOQXI_f9cmgJrOv3LF0j9T8FYkcz8N5HtgHy5aRhjqzLFMmkXbh6CFNJpayxqZtqu_vWHhr88rltf6pWhW_-Hn3AyYlDrzeoNViHH5xwe0y7AXJk2Y8YWNOM7Hk5s0Qip2D20E_XShk2yndnrulY9d3TD6Cjp0RuTwPEs_3QmwIY7dh46CmYWZQr7_-shShH2go_nbFNfrGFR5EdfpOVsn7NcZ1Nc0en9BRFZo_3uxU--V-Enthf910gQy3i4t9rwpu6l70CCxs6StOPoNpgTNhxmUuCBSCAPLQ2Kc8rz2X5YpyH-59-sqlf9pBLOSjoPByLsTJEnU0Of9l-NGeHNrRONBu7ahy7Gjtru_Cv0B70TAT01WTXA0FZ3Clv96glyyBUCXhqJZAyEZB6CtBfbRQRSigz5ofA2mCa_vR3eci6orAv2JQAX-xXLci82y4ieggQ1-agnO6fWWH4mSmDP-bCykP0esoBB3EW9Gaa0Dsnf5-5fhXMmVM44duoSp9kzpnfHruEeHsSaPJIjrdDKgkMNIBRBqyQ82LB2-A4LuPtggBJt3ilP8N8gnrV6_U1aiSzoUxJXy1Y3OIx2uCFQrbAVMdfBocr9X80Acpl_9Dm1pW1n0O0JLTJ5qq546q9ZYsJPfFVOeEoIjnT5AmfTcdXcj_62iKHlOqIqbdMik0ImZTBY1YLomvb1zbW9rIH7sjrQTm0ngvTqWqBg3_4XipKkX9OAy5_ghIkIXxu9BnnEETE9UTxJrsfHifRxTUlF8K1Hdr62u3dENbY9kxgRs9Kypk3uVkXjWew2L4ajqntNRNXdEh3uR0g5LgS_TSucIfzpb_48aZiuM-Jmbcgv-FYbYU4PSXKcZptQcz0W1pyXk73SEWbnSZ_leFsPIW-pgUbtiDm6QQptTXP4E-uozGoOCEZ_7F_8SrN26eaAxvsdmCT8bA2pFb6i2wqcLGnkovIX5HtTw7Jd4oxILTl1_jbGA7cbyV3_eabx5Wj6ChADD70U3UNroQixjOwM7ZKVO2f4WumaSiduVApvFgl2Xl5ROl-LB324ePRo3wbf8MmQs2Ltc9y_Z104N4TT6MQbBTrn01SuEQM9q_Q7PfNJt3Qfr4iQ69VSzGctJ6fy-1_E1pUFu6vzUxxBUEIW_bvOabPadZ1wy4aF2fgG06PthT8Fyuan6HU23TP6w2rNLNyzF3bEvb_RFwrNoyfIx7Evgz-FUFDczZ85caNXYu4udNAxIymxhB0NwQ9o0l7bKde-qrRwLaV9SEuUVbc6LEcEeYdeDEAAdBpRaEFo89R6oksN-bQgi78hfYDfy6pgI1rGFivPGnqoHKyeBBshbU6RABgcBwJHxcHEmkhQVzCZgJ8eLgFWaktUkt7LGUspgBmUFVoobWI7tnOwYOVsQ7n4WClTiZauMEir3h0X-9PYbZJ53p7IaTdm4vc635STTGsmy3ZD4LHOqctbRtuhLsUuw5Qfr-58bfC-x8OChLJcRX5iIFu-Xxz6GjpaKTvt975je6x7FAVrA0zz7kFYcuDCMnkYIrdRXk7FF5MKyZs2wombMvTbcgFYMWXUrYn7GD3enLbr9Jccvoo7AU1VMMCdYlicSVzAwSaVLPWCcHq_BtYZjFSt7lJTrm86tPKyrgbGvR0EIzQvGKMyRTn7d5akgS_-tx-eYLB7Ieg3qhaNUcFNzRE5_jdAx55zd45rKOT94g94lxrpwaVYXdWIuNChP_HAP17QYPeIudeYVe4eIUrjxJTgnaU8E8EcAbf12JZLO76vFcRZagSD6GU6Hu5iA3ltQojd56b-rH65TWKT_t7Rlro1OZ94bOs8MmSdLhjFEDH7XPlxW-T7F861QXayWQr5V36Yts1wSzFR30YWNd04H6DxIH3ftcfN2lUri7XSjGH1eq3ejYLOpz3wgtswP2bh3Zry2tg_BsJ2PdObLhssVE8rBbqRkNJGXyNE6TiJ0wtQJu-SZpg37jpVuGm8BZrgB13xb8N8coypruIsc1ykL_Wtnr_Amd-ZLZSKMLflnF6BiLkPLTUHD7rBu4k1wrGiTEynalp7nqVA4i_8eOgP81nhgNTpY-OvNrvys7ILCMgkLn3xiBu12Ahz9zWKGfT1kOfRH3Xyp0RRHMByASeXSogzuNkDqni9DDgslOoHPibej6wiJL8u8c_y52wohQZVFSDLIBbBevbUktn4bLZTlDtZxZ24DXn1sNL56VJX9uHmDaOGTZK7bMUW6vNTcFaMCw_mKJHpR0S_nxqU33Y3SPpk3Yt0TJBSko2DKsQrzWCgVr99OLjsABKSXGdgD55idEsvcOs6FYceyu37NiPYhsUNbqwSScmZiRjoLmq9jWOCV2_crTTg72YxAnwSGUWcjaRFHHweIS5GhsOwyKTmev0Cv4xzIFw6ElCZBUftdli5La9BKQ5skCXqTNg1XlhLPw_4vWnzDeSzIaLTb9DH8pvyRCSFC_k4ggKWqkuCriUMeY5jMB75uVsunt8uj_yPund8CEtwcNxMrORD1v40Wh80aGp3pc4dhCEchi-0DJXpuvmU_SdbLPieTaC5qXN071ELxXPf2QK-JWad9axu4WyskgAl5SO45G0XghMCIn6PwwuTSx5r0fvoi7qeiIPBw4o63LBtY0UzBGMb6r2JzUXh99uqwoA1gWRuwEC8wbhkBASEYdDAAQTFr7Xkv7j3VuXEhlgmLyS0jxrKT9iesisv58P4YCy8sZYMjPnUtSwef-8MDrGVoKcf-ffRzkeVboXLnLAxDYMZYW7VvZ2RJ2eytjpK8LuBWGhiRd80PPWWzC9owfJMr5INvicUTZK_wUce-YRoIkhFLZDAgQ-BJaCshA312H222DPG3bWvsqhge26--5vIpqOtDcJETxgPFmYYx82I8C1HUYkgy74wZPcPS0BJx64JnTcZQbIMGXPxqGDH_3SLrzK1mSzZHf8_9IOmx5GVPjgRL1CkRXh3fD_rrD6Fl3C6wgMTU6YeI2HciWchmTAU4UYWNU7TzPm95Na8RGKyAK2OO61OiYesFL8hesSjXJpPgPPqplQtqrnQv64GEso0ANT9XKvFuhkvDWr6PiPzIWMuGCV_aaL2TmtGAA_bKVMHOitw1ClLDRBqcYDCIdwe-stOXBPW7C3UjsehzkvSMohL8X13mxJ9UcEGKnejM1H7YAQpf-iSkZwYPKECHUk19r7nbKGxmKaVDwrsS4NHi3hYCByCXZKH6LYisrJaOwI11O4I70RNfmhwGonhC4TEgBf_LWxbDFCEhxjeeTauNtRtPglfKTeVEKa0mIg8ewmscv1P0lwhSJQIV9TwXfIKRXultrFNoRQFpM1F7LjTqLSsRuUyNLjgsJGOl73T5ITpL7WURSAWGDaKc3tzBpoDlz5S5jSNTdS0aemL3URfqNtMrOPrxTUj8zSamvUfj7FZZ1CzEN7tSiiipDklEwPocH78OJmWFeSV-As8HvubbHD-tFMBPzdhDrZGhlXc6x8ZJWt433sWKUAIm83WLUDkgDXh3cG1E8LPQb_M06Z1e4lpPcRRAaQSls37qywM_TJbiUiNKSFujsPLDCMqKCwIo0m6JjMw_d0n5CISxSsuMizmwfOsi4hdYt6GTcXQmmkyfhgX72J5SKqTB5BJqJsaBLviVL2YayWktksXNpGe49OXRU75g3DJhuzKFK3YqbJf1BhW9jSaSbrDLjQ_l5yZm4NDswmR76xB-9CvQxbebmRU-4X4nmHb4wUJCSOlMpmWTpd6yOYDMRGKwuBv-AWG3pfdXYDpwBkt6ZFAvSXTOVtSodPbh2_VNuJ7kRicZfG_lNx4TXCt4dMl58xc1rjfHZM9q9ohghuthNOjj2jnWC7-PjH21vSQOdBTBXFkCUmpYi4aYsm2sn6qo.n1_VYMXjPAWOFJWqY2H1SA',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://app.powerbi.com',
        'referer': 'https://app.powerbi.com/',
        'requestid': 'c5fed331-cd66-19cd-1f2e-4aaf5a39b038',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'x-ms-parent-activity-id': 'c5fed331-cd66-19cd-1f2e-4aaf5a39b038',
        'x-ms-root-activity-id': 'c5fed331-cd66-19cd-1f2e-4aaf5a39b038',
        'x-ms-workload-resource-moniker': 'bb96794b-d5ba-4fb8-878d-99d2e6a38b6d',
    }
    pdf_base_url = 'https://www.ebs.tga.gov.au/servlet/xmlmillr6?dbid=ebs/PublicHTML/pdfStore.nsf&docid={}&agid=(PrintDetailsPublic)&actionid=1'
    custom_settings = {
        'FEED_URI': 'compliance_health_v3.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORT_FIELDS': ['ARTG ID', 'Approval Area','Sponsor', 'ARTG Start Date', 'Product Category', 'Manufacturer Name',
                               'Manufacturer Location (Country)', 'PDF URL']
    }

    def start_requests(self):
        yield scrapy.Request(url=self.request_api, method='POST', headers=self.headers, body=self.payload)

    def parse(self, response):
        json_data = json.loads(response.body)
        results = json_data.get('results', [])[0].get('result', {}).get('data', {}).get('dsr', {}).get('DS', [])
        pdf_urls = []
        if results:
            pdf_id = results[0].get('PH', [])[0].get('DM0', [])
            for ids in pdf_id:
                c_tag = ids.get('C', [])
                for tag in c_tag:
                    if len(str(tag)) == 6:
                        pdf_urls.append(self.pdf_base_url.format(tag))
        for url in pdf_urls[803:]:
            File = urllib.request.urlopen(url)
            reader = PyPDF2.PdfReader(io.BytesIO(File.read()))
            data = ""
            for datas in reader.pages:
                data += datas.extract_text()
            item = dict()
            print(url)
            for line in data.splitlines():
                if 'Sponsor' in line:
                    item['Sponsor'] = line.replace('Sponsor', '').strip()
                if 'ARTG Start Date' in line:
                    item['ARTG Start Date'] = line.replace('ARTG Start Date', '').strip()
                if 'Product Category' in line:
                    item['Product Category'] = line.replace('Product Category', '').strip()
                if 'Approval Area' in line:
                    item['Approval Area'] = line.replace('Approval Area', '').strip()
            item['ARTG ID'] = url.split('docid=', 1)[-1].split('&', 1)[0]
            if 'Manufacturers' in data:
                other_data = (data.split('Name Address'))[1].split('Products')[0]
                if len(other_data.splitlines()) > 3:
                    item['Manufacturer Name'] = other_data.splitlines()[1]
                    item['Manufacturer Location (Country)'] = other_data.splitlines()[-1]
            item['PDF URL'] = url
            yield item
