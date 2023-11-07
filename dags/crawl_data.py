import requests
from bs4 import BeautifulSoup
import get_all_variables as gav
from datetime import datetime

def get_id_boxoffice(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    div = soup.find("div",{"class":"a-box-inner"})
    title = div.find("a",{"class":"a-link-normal"}).attrs['href']
    id = title.split('/')[4]

    return id
                      
def crawl_box_office_data(date) -> list:

    url = f"{gav.box_office_path}/{date}/"

    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    rows = soup.find_all("tr")
    box_office_daily = []

    for row in rows[1:]:
        movie_daily_info = {}

        rank = row.find("td",{"class":"mojo-header-column"}).text

        revenue = row.find("td",{"class":"mojo-field-type-money"}).text

        href = row.find("td",{"class": "mojo-field-type-release"}).find("a").attrs['href']
        url_detail = url[:29] + href
        id_imdb = get_id_boxoffice(url_detail)

        gross_change= row.find_all('td',{'class':'mojo-field-type-percent_delta'})
        
        gross_change_per_day = gross_change[0].text
        gross_change_per_week = gross_change[1].text

        movie_daily_info['id'] = id_imdb
        movie_daily_info['rank'] = rank
        movie_daily_info['revenue'] = revenue[1:]
        movie_daily_info['gross_change_per_day'] = gross_change_per_day
        movie_daily_info['gross_change_per_week'] = gross_change_per_week
        movie_daily_info['crawled_date'] = date

        box_office_daily.append(movie_daily_info)
    
    print("crawl box office was successfully")
    return box_office_daily



def crawl_imdb_data(**items) -> list:

    imdb_data = [] 
    context = items['ti'].xcom_pull(task_ids='crawl_fact_data')

    for item in context:
        id = item['id']
        if not id:
            print(f"{id} is not response")

        crawled_date = item['crawled_date']

        url = f"{gav.imdb_path}/{id}/faq/"

        headers = {'User-Agent': gav.user_agent, 'Accept-language': 'US-en'}
        response = requests.get(url,headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        data = soup.find_all('div',{'class':'ipc-html-content-inner-div'})

    # Get neccessary data

        title = soup.find("h2", {"data-testid": "subtitle"})
        if not title:
            print(f"{id} not response")
            title = 'null'
        else:
            title = soup.find("h2", {"data-testid": "subtitle"}).text.replace("'","")

        duration = soup.find("li", {"id":"run-time"})
        if not duration:
                print(f"{id} not response")
                duration = 'null'
        else:
            duration = soup.find("li", {"id":"run-time"}).text
            duration_question_mark_index = duration.find('?')
            duration = duration[duration_question_mark_index+1:].replace("'","")

        rating = soup.find("li", {"id":"imdb-rating"})
        if not rating:
            print(f"{id} not response")
            rating = 'null'
        else:
            rating = soup.find("li", {"id":"imdb-rating"}).text
            rating_question_mark_index = rating.find('?')
            rating = rating[rating_question_mark_index+1:].replace("'","")

        director = soup.find("li", {"id":"director"})
        if not director:
            print(f"{id} not response")
            director = 'null'
        else:
            director = soup.find("li", {"id":"director"}).text
            director_question_mark_index = director.find('?')
            director = director[director_question_mark_index+1:].replace("'","")


        budget = soup.find("li", {"id":"budget"})
        if not budget:
            #print(f"{id} not response")
            budget = 'null'
        else:   
            budget = soup.find("li", {"id":"budget"}).text
            budget_question_mark_index = budget.find('?')
            budget = budget[budget_question_mark_index+1:].replace("'","")

        worldwide = soup.find("li", {"id":"box-office-earnings"})
        if not worldwide:
            worldwide = 'null'
        else:
            worldwide = soup.find("li", {"id":"box-office-earnings"}).text
            worldwide_question_mark_index = worldwide.find('?')
            worldwide = worldwide[worldwide_question_mark_index+1:].replace("'","")

        genre = soup.find("li", {"id":"genre"})
        if not genre:
            print(f"{id} not response")
            genre = 'null'
        else:
            genre = soup.find("li", {"id":"genre"}).text
            genre_question_mark_index = genre.find('?')
            genre = genre[genre_question_mark_index+1:].replace("'","")
        
        final_data = {}
        final_data['id'] = id
        final_data['title'] = title
        final_data['duration'] = duration
        final_data['rating'] = rating
        final_data['director'] = director
        final_data['budget'] = budget
        final_data['worldwide_gross'] = worldwide
        final_data['genre'] = genre
        final_data['crawled_date'] = crawled_date
        imdb_data.append(final_data)
  
    return imdb_data
 
