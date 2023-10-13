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
        dim_items["movie_id"] = id
        dim_items["url"] = url.replace("'","")
        dim_items["director"] = soup.find("a", 
                                          {"class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"}).text.replace("'","")
        #dim_items["crawled_date"] = date
        imdb_data.append(dim_items)

        #div = soup.find_all('div',{'class':'ipc-html-content-inner-div'})
        
        #movie_duration = soup.find("li", {"id": "run-time"})#.find("div", {'class':"ipc-html-content-inner-div"})
        #div_run_time = run_time.find("div", {'class':"ipc-html-content-inner-div"}).text
        # # Get neccessary data
        #title = soup.find('h2',{'data-testid': "subtitle"})
        # movie_duration = div_run_time
        # rating = div[2].text.split(" ")[0]
        # director = div[5].text
        # budget = div[12].text
        # # worldwide = div[13].text
        # # genre = (div[16].text).replace(" and","")
        


        # final_data = {}
        # final_data['title'] = title.text
        # final_data['id'] = id
        #final_data['movie_duration'] = soup
        # final_data['rating'] = rating
        # final_data['director'] = director
        # final_data['budget'] = budget
        # # final_data['worldwide'] = worldwide
        # # final_data['genre'] = genre
        # final_data['url'] = url

        imdb_data.append(dim_items)
        
    return imdb_data

# if __name__ =='__main__':
#     #url = "https://www.boxofficemojo.com/date/2023-09-02/"
#     boxOfficeMojo('2023-09-02')

# imdb_url = f"https://www.imdb.com/title/tt17024450/faq/"
# crawl_imdb_data(imdb_url)