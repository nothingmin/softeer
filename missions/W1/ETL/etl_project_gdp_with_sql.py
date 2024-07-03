import collections
import logging
import sqlite3

import pandas as pd
import requests
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parser

collections.Callable = collections.abc.Callable
logger = logging.getLogger(__name__)
logging.basicConfig(filename='etl_project_log.txt', encoding='utf-8', level=logging.DEBUG,
                    format='%(asctime)s,%(message)s', datefmt='%Y-%B-%d-%H-%M-%S')


def remove_nested_parens(input_str):
    result = ''
    paren_level = 0
    for ch in input_str:
        if ch == '[':
            paren_level += 1
        elif (ch == ']') and paren_level:
            paren_level -= 1
        elif not paren_level:
            result += ch
    return result


logging.debug("extracting GDP from wikipedia")
html = requests.get('https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29').text
soup = BeautifulSoup(html, 'html.parser')
gdp_table = soup.find("table", attrs={"class": "wikitable sortable sticky-header-multi static-row-numbers"})
parsed = parser.make2d(gdp_table)
for i, row in enumerate(parsed):
    for j, word in enumerate(row):
        parsed[i][j] = remove_nested_parens(word).replace(',', '')
gdp_frame = pd.DataFrame(parsed[2:], columns=[parsed[0], parsed[1]])
logging.debug("extracted GDP from wikipedia done")

logging.debug("transforming GDP")
imf_gdp = gdp_frame[['Country/Territory', 'IMF']]
imf_gdp.columns = imf_gdp.columns.droplevel()
imf_gdp = imf_gdp.rename(columns={'Country/Territory': 'Country', 'Forecast': 'GDP_USD_billion'})
imf_gdp = (
    imf_gdp[imf_gdp['GDP_USD_billion'].apply(lambda x: x.isnumeric())]
    .astype({'GDP_USD_billion': 'float'})
    .astype({'Year': 'int64'})
    .query('Country != "World"')
    .sort_values('GDP_USD_billion', ascending=False)
    .reset_index(drop=True)
)
imf_gdp['GDP_USD_billion'] = imf_gdp['GDP_USD_billion'] / 1000
imf_gdp = imf_gdp.round({'GDP_USD_billion': 2})
logging.debug("transforming GDP done")

db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'


def create_sqlite_database(filename):
    """ create a database connection to an SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(filename)
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


logging.debug("loading GDP to db start")
create_sqlite_database(filename='World_Economies.db')
with sqlite3.connect(db_name) as con:
    imf_gdp.to_sql(table_name, con, if_exists='replace', index_label='Rank')
logging.debug("loading GDP in db done")

query = """
        SELECT * FROM Countries_by_GDP WHERE GDP_USD_billion >= 100
        """
logging.debug("executing query {}".format(query))
try:
    with sqlite3.connect(db_name) as conn:
        cur = conn.cursor()
        cur.execute(query)
        for row in cur.fetchall():
            print(row)
        logging.debug("executed query done")
except sqlite3.Error as e:
    logging.debug("failed query")
    logging.debug(e)
