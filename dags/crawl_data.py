import requests
from bs4 import BeautifulSoup
import get_all_variables as gav

def get_id_boxoffice(id_url):
    res = requests.get(id_url)
    soup = BeautifulSoup(res.content, 'html.parser')
    raw_href = soup.find_all('main')
    for href in raw_href:
        id = href.find('a', {'class':'a-link-normal'}).get('href')
        id = id.split('/')
        return id[4]

    
def crawl_box_office_data(date) -> list:
    url = f"{gav.box_office_path}/{date}"

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

        gross_change= row.find_all('td',{'class':'mojo-field-type-percent_delta'})
        
        gross_change_per_day = gross_change[0].text
        gross_change_per_week = gross_change[1].text

        url_detail = 'https://www.boxofficemojo.com/' + row.find('a',
                                         {'class':'a-link-normal'}).get('href')
        id_imdb = get_id_boxoffice(url_detail)

        final_data['id'] = id_imdb
        final_data['rank'] = rank
        final_data['revenue'] = revenue[1:]
        final_data['gross_change_per_day'] = gross_change_per_day
        final_data['gross_change_per_week'] = gross_change_per_week
        final_data['crawled_date'] = date

        box_office_daily.append(final_data)

    return box_office_daily



def crawl_imdb_data(**items) -> list:

    imdb_data = [] 
    context = items['ti'].xcom_pull(task_ids='crawl_fact_data')

    for item in context:
        dim_items = {}
        id = item['id']
        crawled_date = item['crawled_date']
        url = f"{gav.imdb_path}/{id}/"

        header = {'User-Agent': gav.user_agent}
        response = requests.get(url,headers=header)
        soup = BeautifulSoup(response.text, 'html.parser')
        dim_items["movie_id"] = id

        dim_items["title"] = soup.find("span", {"class":"sc-afe43def-1 fDTGTb"}).text.replace("'","")
        
        dim_items["director"] = soup.find("a", 
        {"class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})\
            .text.replace("'","")
      
        dim_items["rating"] = soup.find("span", {"class": "sc-bde20123-1 iZlgcd"}).text

        dim_items["crawled_date"] = crawled_date

        imdb_data.append(dim_items)
  
    return imdb_data
