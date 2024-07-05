import collections
import json

import pandas as pd
import requests
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parser

collections.Callable = collections.abc.Callable

target_url = [(
    "https://en.wikipedia.org/wiki/List_of_African_countries_by_GDP_(nominal)",
    "Africa"
), (
    "https://en.wikipedia.org/wiki/List_of_South_American_countries_and_dependencies_by_GDP_(PPP)",
    "South America"
), (
    "https://en.wikipedia.org/wiki/List_of_North_American_countries_by_GDP_(nominal)",
    "North America"
), (
    "https://en.wikipedia.org/wiki/List_of_Asian_countries_by_GDP",
    "Asia"
), (
    "https://en.wikipedia.org/wiki/List_of_sovereign_states_in_Europe_by_GDP_(nominal)",
    "Europe"
), (
    "https://en.wikipedia.org/wiki/List_of_Oceanian_countries_by_GDP",
    "Oceania"
),
]


def get_table(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    table = parser.make2d(soup.find("table"))
    return pd.DataFrame(table[1:], columns=[table[0]])


if __name__ == '__main__':
    country_region = {}
    for url, region in target_url:
        df = get_table(url)
        for row in df.filter(regex='(Country)|(Location)').iterrows():
            if row[1].iloc[0] in country_region.keys():
                print(row[1].iloc[0], " already exists ", country_region[row[1].iloc[0]], "current : ", region)
            else:
                country_region[row[1].iloc[0]] = region
    with open("country_region.json", "w") as json_file:
        json.dump(country_region, json_file)
