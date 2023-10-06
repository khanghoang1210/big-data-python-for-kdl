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

def boxOfficeMojo(url):
    # Request to website and download HTML contents
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    # Find all tag named 'tr'
    rows = soup.find_all('tr')

    box_office_daily = []
    for row in rows[1:]:
        final_data = {}
        # Find necessary data
        rank = row.find('td',{'class':'mojo-header-column'}).text
        revenue = row.find('td',{'class':'mojo-field-type-money'}).text
        gross_change_by_date = row.find('td',{'class':'mojo-field-type-percent_delta'}).text
        # Get IMDB url
        url_detail = 'https://www.boxofficemojo.com/' + row.find('a', {'class':'a-link-normal'}).get('href')
        id_imdb = get_id(url_detail)

        final_data['id'] = id_imdb
        final_data['rank'] = rank
        final_data['revenue'] = revenue
        final_data['gross_change'] = gross_change_by_date
        box_office_daily.append(final_data)
    print(box_office_daily)

url = "https://www.boxofficemojo.com/date/2023-09-02/"
boxOfficeMojo(url)

