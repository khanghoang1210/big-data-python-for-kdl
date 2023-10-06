import requests
import json
from bs4 import BeautifulSoup


url = "https://www.imdb.com/title/tt1517268/"

response = requests.get(url)

soup = BeautifulSoup(response.content, 'html.parser')

rows = soup.find_all("tr")
fact_movie = []

for row in rows[1:]:

def extract_id(url: str):
    res = requests.get(url)
    id = find("...")
    return id

def crawl_box_office():
    url = "https://www.boxofficemojo.com/date/1221"
    href = "/release/..."
    url_detail = url[:10] + href

    id = extract_id(url_detail)

"adadsadsasd"

