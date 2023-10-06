import requests
import json
from bs4 import BeautifulSoup

def get_id(id_url):
    res = requests.get(id_url)
    spagetti = BeautifulSoup(res.content, 'html.parser')
    raw_href = spagetti.find_all('main')
    for href in raw_href:
        id = href.find('a', {'class':'a-link-normal'}).get('href')
        id = id.split('/')
        return id[4]
def get_id_IMDB(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    datas = soup.find_all('tr')
    for data in datas[1: ]:
        id_url = 'https://www.boxofficemojo.com/' + data.find('a', {'class':'a-link-normal'}).get('href')
        id_imdb = get_id(id_url)
        return id_imdb

def boxOfficeMojo(url):
    # Request to website and download HTML contents
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    # Find all tag named 'tr'
    rows = soup.find_all('tr')

    for row in rows[1:]:
        # Find necessary data
        rank = row.find('td',{'class':'mojo-header-column'}).text
        nameMovie = row.find('td',{'class':'mojo-field-type-release'}).text
        revenue = row.find('td',{'class':'mojo-field-type-money'}).text
        gross_change_by_date = row.find('td',{'class':'mojo-field-type-percent_delta'}).text
        studio = row.find('td',{'class':'mojo-field-type-release_studios'}).text.replace('\n',"")
        
        data = {"Rank": rank,"Name": nameMovie,"Revenue": revenue,"Gross Change": gross_change_by_date, "Studio": studio}
        final_data = json.dumps(data, indent=4)
        return final_data

url = "https://www.boxofficemojo.com/date/2023-09-02/"
boxOfficeMojo(url)
get_id_IMDB(url)
