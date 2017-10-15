import requests
from bs4 import BeautifulSoup, SoupStrainer


def get_all_traits_and_specialities():
    url = 'https://sofifa.com/players/top'
    html = requests.get(url).text
    strainer = SoupStrainer('form', action='/players', class_='pjax relative')
    soup = BeautifulSoup(html, 'lxml', parse_only=strainer)
    traits1 = list(soup.find(attrs={'name': 't1[]'}).stripped_strings)
    traits2 = list(soup.find(attrs={'name': 't2[]'}).stripped_strings)
    all_traits = [*traits1, *traits2]
    all_specialities = list(soup.find(attrs={'name': 'sc[]'}).stripped_strings)
    return {'traits': all_traits, 'specialities': all_specialities}
