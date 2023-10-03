import requests
import json
from bs4 import BeautifulSoup


url = "https://www.imdb.com/title/tt1517268/"
header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"}
res = requests.get(url, headers=header)
# data = json.loads(res.content)
data = res.content.decode("utf-8")
# data_json = data.json()
soup = BeautifulSoup(data, 'html.parser')
title = soup.find_all("span")
print(soup)
