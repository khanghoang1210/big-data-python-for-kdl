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
        #nameMovie = row.find('td',{'class':'mojo-field-type-release'}).text
        revenue = row.find('td',{'class':'mojo-field-type-money'}).text
        gross_change_by_date = row.find('td',{'class':'mojo-field-type-percent_delta'}).text
        #studio = row.find('td',{'class':'mojo-field-type-release_studios'}).text.replace('\n',"")
        url_detail = 'https://www.boxofficemojo.com/' + row.find('a',
                                         {'class':'a-link-normal'}).get('href')
        id_imdb = get_id_boxoffice(url_detail)

        final_data['id'] = id_imdb
        final_data['rank'] = rank
        final_data['revenue'] = revenue
        final_data['gross_change'] = gross_change_by_date
        box_office_daily.append(final_data)
    return box_office_daily



def crawl_imdb_data(**items):
    imdb_data = [] 
    context = items['ti'].xcom_pull(task_ids='crawl_fact_data')
    for item in context:
        dim_items = {}
        id = item['id']
        url = f"https://www.imdb.com/title/{id}/"

        header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}
        response = requests.get(url,headers=header)
        soup = BeautifulSoup(response.text, 'html.parser')

        dim_items["title"] = soup.find("span", {"class":"fDTGTb"}).text.replace("'","")
        #dim_items["movie_id"] = id
        dim_items["url"] = url.replace("'","")
        dim_items["director"] = soup.find("a", 
        {"class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})\
            .text.replace("'","")
        #dim_items["crawled_date"] = date
        dim_items['rating'] = soup.find('span', {'class': "sc-bde20123-1 iZlgcd"}).text
        #dim_items['genre'] = 
        imdb_data.append(dim_items)

        imdb_data.append(dim_items)
        
    return imdb_data

# if __name__ =='__main__':
#     #url = "https://www.boxofficemojo.com/date/2023-09-02/"
#     boxOfficeMojo('2023-09-02')

# imdb_url = f"https://www.imdb.com/title/tt17024450/faq/"
# crawl_imdb_data(imdb_url)