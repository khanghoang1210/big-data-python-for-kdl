import requests
import json
from bs4 import BeautifulSoup

global header
header = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    }
def get_id(id_url):
    res = requests.get(id_url)
    soup = BeautifulSoup(res.content, 'html.parser')
    raw_href = soup.find_all('main')
    for href in raw_href:
        id = href.find('a', {'class':'a-link-normal'}).get('href')
        id = id.split('/')
        return id[4]

def get_rating(imdb_web):
    res = requests.get(imdb_web, headers=header)
    soup = BeautifulSoup(res.content, 'html.parser')
    html_file = soup.find_all('main')
    for html in html_file:
        rating = html.find('span', {'class':'sc-bde20123-1'}).text
        return rating

def get_director(imdb_web):
    res = requests.get(imdb_web, headers=header)
    soup = BeautifulSoup(res.content, 'html.parser')
    html_file = soup.find_all('main')
    for html in html_file:
        director = html.find('a', {'class':'ipc-metadata-list-item__list-content-item'}).text
        return director

def get_budget(faq_web):
    res = requests.get(faq_web, headers=header)
    soup = BeautifulSoup(res.content, 'html.parser')
    html_file = soup.find_all('li',{'id':'budget'})
    for html in html_file:
        budget = html.find('div',{'class':'ipc-html-content-inner-div'}).text
        return budget
        # print(budget)


def boxOfficeMojo(date):
    url = f"https://www.boxofficemojo.com/date/{date}/"

    # Request to website and download HTML contents
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    # Find all tag named 'tr'
    rows = soup.find_all('tr')

    box_office_daily = []
    for row in rows[1:]:
        final_data = {}
        # Access boxoffice realease
        url_detail = 'https://www.boxofficemojo.com/' + row.find('a', {'class':'a-link-normal'}).get('href')

        # Access IMDB url
        subID = get_id(url_detail)
        imdb_web = "https://www.imdb.com/title/" + subID
        # Access to IMDB FAQ website
        faq_web = imdb_web + "/faq"
        # print(faq_web)
        budget = get_budget(faq_web)
        print(budget)

        # Find necessary data
        id_imdb = get_id(url_detail)
        rank = row.find('td',{'class':'mojo-header-column'}).text
        rating_score = get_rating(imdb_web)
        revenue = row.find('td',{'class':'mojo-field-type-money'}).text
        gross_change_by_date = row.find('td',{'class':'mojo-field-type-percent_delta'}).text
        director = get_director(imdb_web)

        final_data['id'] = id_imdb
        final_data['rank'] = rank
        final_data['rating'] = rating_score
        final_data['revenue'] = revenue
        final_data['gross_change'] = gross_change_by_date
        final_data['director'] = director
        box_office_daily.append(final_data)
    return box_office_daily

if __name__ =='__main__':
    #url = "https://www.boxofficemojo.com/date/2023-09-02/"
    boxOfficeMojo('2023-09-02')
