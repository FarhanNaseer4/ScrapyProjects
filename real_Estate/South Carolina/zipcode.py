import requests, json
from bs4 import BeautifulSoup
url = "https://www.southcarolina-demographics.com/zip_codes_by_population"
   
headers = {
  'authority': 'www.carriersource.io',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
  'accept-language': 'en-US,en;q=0.9',
  'cache-control': 'max-age=0',
  'cookie': 'ahoy_visitor=2babff0f-b3e5-4e1d-8298-3d5b368da0f0; ahoy_visit=443d4cd2-07b0-4b7c-8d6a-b528940ad29e; _gid=GA1.2.568799578.1672776041; remember_token=de7000b6129763122fd28b94a8634441c6edb207; _hp2_ses_props.3215006394=%7B%22ts%22%3A1672778556782%2C%22d%22%3A%22www.carriersource.io%22%2C%22h%22%3A%22%2Fcarriers%2Fr-p-m-transit%22%7D; cf_chl_2=86cf12a987ece27; cf_clearance=41y4dkQPZLu8Dv_QdZbt4rQwhevPAc7D_jmOxmccKy4-1672779696-0-160; _ga=GA1.1.767101848.1672776041; _hp2_id.3215006394=%7B%22userId%22%3A%226878695252203404%22%2C%22pageviewId%22%3A%223330303997068529%22%2C%22sessionId%22%3A%228137261038299180%22%2C%22identity%22%3A%22i.lamrabate%40gmail.com%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D; _carrier_source_session=U5Mqijvso%2FpO7FAkBrWFtRlpEAI14xH%2FIvVSqU9rC7HUrh2awkepKoKkj0%2FZshHhT%2Bluv753Bbp38vOj0sBTnivZyQPB08Khozqfmn32Kfox3cfqE7vjtLZ14OJz%2B0ltZPrymxyz6bn3HrGT599yBIUmKGje8cFyQ5ocJAEG9vkfn5t1JisdPUJhMoypAyW0Q0uRxBdRwDXaPSb29DKDGwl7o1h2LjpAEfWpyMCuJqffnp5exKmAFcThO1Pjn%2BwIg013bSz6xRhoGDKv900nmPTB6VP7VnZ49P1edVFkLFdJJQ1rDoPl1dzsR9cmEnd5%2BmCIrap7D%2FN19Ea63sDxJ63n92W1ClYtcp6XpD7odCxW53BB40dCyG5%2BgmkG%2BIKtg95t%2BCTl5VeE8GqN1mgsGkj4B5GCAmJ%2FqiSSB4OPbvkqoHZGCJJIL0sD4C71MKOmzTyUYg6jx311IZz%2BNwWstMjjuQPrSdAti5W7mhpzo6%2FoOdYwn%2Fy5%2F3gYszmh92Gf30%2BVa0DC1fEcy%2BuaJI1tWASpV2Q9ApxSulCc8TVpjxDpAyfJRv32MovwYGvpGogv%2F93R7t7591vrr82MLcZF3yZGmSJbs5pY9ulQNTUim0xMVKE04R%2BagOIZV3lpsgme7ZHoSP5idG5sjZ3%2FMPLY--NTlzR4lXi7GeAiQw--2biwlaXbMF%2FJIoYiHLhrAA%3D%3D; _ga_WDD81KY1WE=GS1.1.1672779444.2.1.1672780304.0.0.0; _nimbus_session=eyJzZXNzaW9uX2lkIjoiNzllZTNmN2U3NWFiNGZhMzcxNDE0NTU0MmJmZTVjMTQiLCJfY3NyZl90b2tlbiI6Im9IMngxNVI0d0pkSUpWOFpJN1dzU0o1OFExZDNvOHl5ZlNsVFpzZFJtajg9In0%3D--ef22eadbfbb5bd829835cb73411209e839921e0c',
  'referer': 'https://www.carriersource.io/trucking-companies/canada/quebec/melbourne?__cf_chl_tk=byrJ4FjQpBzNvgyLUSvT_9Faxsn9kz7qBBHMdVn9l7E-1672779671-0-gaNycGzND6U',
  'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"macOS"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'same-origin',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
}

def save_to_results(zip_code: str):
    with open('/Users/ab/Desktop/Python/spetan/zip_code.json', 'a') as f:
        f.write(json.dumps({'zip_code': zip_code}))
        f.write('\n')
    f.close()
        
response = requests.request("GET", url, headers=headers)

soup = BeautifulSoup(response.content, 'html.parser')
zip_codes = soup.find_all('td')
for ele in zip_codes:
  zip_codes = ele.find('a')
  if zip_codes:
    zip_codes = zip_codes.text
    save_to_results(zip_codes)
  