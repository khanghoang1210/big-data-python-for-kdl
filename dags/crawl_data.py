import requests
import json
from bs4 import BeautifulSoup

def get_id_boxoffice(id_url):
    res = requests.get(id_url)
    soup = BeautifulSoup(res.content, 'html.parser')
    raw_href = soup.find_all('main')
    for href in raw_href:
        id = href.find('a', {'class':'a-link-normal'}).get('href')
        id = id.split('/')
        return id[4]


def IMDB(imdb_url):
    IMDB_data = [] 
    header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}
    response = requests.get(imdb_url,headers=header)
    soup = BeautifulSoup(response.content, 'html.parser')
    id = imdb_url.split('/')
    html_file = soup.find_all('div',{'class':'ipc-html-content-inner-div'})
    # print(html_file)
    data = list(html_file)
    
    # Get neccessary data
    hour = data[0].text
    rating = float(data[2].text.replace(" out of 10",""))
    director = data[5].text
    budget = data[12].text
    worldwide = data[13].text
    genre = (data[16].text).replace(" and","")
    
    final_data = {}
    final_data['hour'] = hour
    final_data['rating'] = rating
    final_data['director'] = director
    final_data['budget'] = budget
    final_data['worldwide'] = worldwide
    final_data['genre'] = genre
    IMDB_data.append(final_data)
    # print(IMDB_data)
    return IMDB_data
    
def boxOfficeMojo(date):
    url = f"https://www.boxofficemojo.com/date/{date}"

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
        url_detail = 'https://www.boxofficemojo.com/' + row.find('a',
                                         {'class':'a-link-normal'}).get('href')
        id_imdb = get_id_boxoffice(url_detail)

        final_data['id'] = id_imdb
        final_data['rank'] = rank
        final_data['revenue'] = revenue
        final_data['gross_change'] = gross_change_by_date
        box_office_daily.append(final_data)
    return box_office_daily

if __name__ =='__main__':
    #url = "https://www.boxofficemojo.com/date/2023-09-02/"
    boxOfficeMojo('2023-09-02')

imdb_url = f"https://www.imdb.com/title/tt17024450/faq/"
IMDB(imdb_url)