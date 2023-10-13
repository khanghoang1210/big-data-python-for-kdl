import requests
from bs4 import BeautifulSoup

url = f"https://www.imdb.com/title/tt0439572/"

header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}
response = requests.get(url,headers=header)
soup = BeautifulSoup(response.text, 'html.parser')


a = soup.find_all("a", {"class":"ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link", "role":"button"})
# for i in range (len(a)):
#     print(a[i])
print(a)