import requests
from bs4 import BeautifulSoup
dim_items = {}
url = f"https://www.imdb.com/title/tt11040844/faq"


 
headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36', 'Accept-Language': 'en-US'}
response = requests.get(url,headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')
data = soup.find_all('div',{'class':'ipc-html-content-inner-div'})

# Get neccessary data

# duration = data[0].text
# rating = (data[2].text.replace(" out of 10",""))
# director = data[5].text
# budget = data[12].text
# worldwide = data[13].text
# genre = soup.find("div", {"class": "ipc-html-content ipc-html-content--base sc-9b1e9ec0-0 kgwgFB"}).text

duration = soup.find("li", {"id":"run-time"}).text
duration_question_mark_index = duration.find('?')
duration = duration[duration_question_mark_index+1:]

rating = soup.find("li", {"id":"imdb-rating"}).text
rating_question_mark_index = rating.find('?')
rating = rating[rating_question_mark_index+1:]

director = soup.find("li", {"id":"director"}).text
director_question_mark_index = director.find('?')
director = director[director_question_mark_index+1:]


budget = soup.find("li", {"id":"budget"}).text  
budget_question_mark_index = budget.find('?')
budget = budget[budget_question_mark_index+1:]

worldwide = soup.find("li", {"id":"box-office-earnings"}).text
worldwide_question_mark_index = worldwide.find('?')
worldwide = worldwide[worldwide_question_mark_index+1:]

genre = soup.find("li", {"id":"genre"}).text
genre_question_mark_index = genre.find('?')
genre = genre[genre_question_mark_index+1:]

final_data = {}
final_data['duration'] = duration
final_data['rating'] = rating
final_data['director'] = director
final_data['budget'] = budget
final_data['worldwide'] = worldwide
final_data['genre'] = genre

print(final_data)