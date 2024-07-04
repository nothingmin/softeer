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

country_region = {'Afghanistan': 'Asia', 'Armenia': 'Europe', 'Azerbaijan': 'Europe', 'Bahrain': 'Asia',
                  'Bangladesh': 'Asia', 'Bhutan': 'Asia', 'Brunei': 'Asia', 'Cambodia': 'Asia', 'China': 'Asia',
                  'Cyprus': 'Europe', 'Georgia': 'Europe', 'India': 'Asia', 'Indonesia': 'Asia', 'Iran': 'Asia',
                  'Iraq': 'Asia', 'Israel': 'Asia', 'Japan': 'Asia', 'Jordan': 'Asia', 'Kazakhstan': 'Europe',
                  'Kuwait': 'Asia', 'Kyrgyzstan': 'Asia', 'Laos': 'Asia', 'Lebanon': 'Asia', 'Malaysia': 'Asia',
                  'Maldives': 'Asia', 'Mongolia': 'Asia', 'Myanmar': 'Asia', 'Nepal': 'Asia',
                  'North Korea': 'Asia', 'Oman': 'Asia', 'Pakistan': 'Asia', 'Palestine': 'Asia',
                  'Philippines': 'Asia', 'Qatar': 'Asia', 'Saudi Arabia': 'Asia', 'Singapore': 'Asia',
                  'South Korea': 'Asia', 'Sri Lanka': 'Asia', 'Syria': 'Asia', 'Taiwan': 'Asia',
                  'Tajikistan': 'Asia', 'Thailand': 'Asia', 'Timor-Leste': 'Asia', 'Turkey': 'Europe',
                  'Turkmenistan': 'Asia', 'United Arab Emirates': 'Asia', 'Uzbekistan': 'Asia', 'Vietnam': 'Asia',
                  'Yemen': 'Asia', 'Antigua and Barbuda': 'North America', 'Bahamas': 'North America',
                  'Barbados': 'North America', 'Belize': 'North America', 'Canada': 'North America',
                  'Costa Rica': 'North America', 'Cuba': 'North America', 'Dominica': 'North America',
                  'Dominican Republic': 'North America', 'El Salvador': 'North America',
                  'Grenada': 'North America', 'Guatemala': 'North America', 'Haiti': 'North America',
                  'Honduras': 'North America', 'Jamaica': 'North America', 'Mexico': 'North America',
                  'Nicaragua': 'North America', 'Panama': 'North America',
                  'Saint Kitts and Nevis': 'North America', 'Saint Lucia': 'North America',
                  'Saint Vincent and the Grenadines': 'North America', 'Trinidad and Tobago': 'North America',
                  'United States': 'North America', 'Albania': 'Europe', 'Andorra': 'Europe', 'Austria': 'Europe',
                  'Belarus': 'Europe', 'Belgium': 'Europe', 'Bosnia and Herzegovina': 'Europe',
                  'Bulgaria': 'Europe', 'Croatia': 'Europe', 'Czech Republic': 'Europe', 'Denmark': 'Europe',
                  'Estonia': 'Europe', 'Finland': 'Europe', 'France': 'Europe', 'Germany': 'Europe',
                  'Greece': 'Europe', 'Hungary': 'Europe', 'Iceland': 'Europe', 'Ireland': 'Europe',
                  'Italy': 'Europe', 'Kosovo': 'Europe', 'Latvia': 'Europe', 'Liechtenstein': 'Europe',
                  'Lithuania': 'Europe', 'Luxembourg': 'Europe', 'Malta': 'Europe', 'Moldova': 'Europe',
                  'Monaco': 'Europe', 'Montenegro': 'Europe', 'Netherlands': 'Europe', 'North Macedonia': 'Europe',
                  'Norway': 'Europe', 'Poland': 'Europe', 'Portugal': 'Europe', 'Romania': 'Europe',
                  'Russia': 'Europe', 'San Marino': 'Europe', 'Serbia': 'Europe', 'Slovakia': 'Europe',
                  'Slovenia': 'Europe', 'Spain': 'Europe', 'Sweden': 'Europe', 'Switzerland': 'Europe',
                  'Ukraine': 'Europe', 'United Kingdom': 'Europe', 'Vatican City': 'Europe',
                  'Argentina': 'South America', 'Bolivia': 'South America', 'Brazil': 'South America',
                  'Chile': 'South America', 'Colombia': 'South America', 'Ecuador': 'South America',
                  'Guyana': 'South America', 'Paraguay': 'South America', 'Peru': 'South America',
                  'Suriname': 'South America', 'Uruguay': 'South America', 'Venezuela': 'South America',
                  'Algeria': 'Africa', 'Angola': 'Africa', 'Benin': 'Africa', 'Botswana': 'Africa',
                  'Burkina Faso': 'Africa', 'Burundi': 'Africa', 'Cabo Verde': 'Africa', 'Cameroon': 'Africa',
                  'Central African Republic': 'Africa', 'Chad': 'Africa', 'Comoros': 'Africa', 'Congo': 'Africa',
                  'Djibouti': 'Africa', 'Egypt': 'Africa', 'Equatorial Guinea': 'Africa', 'Eritrea': 'Africa',
                  'Eswatini': 'Africa', 'Ethiopia': 'Africa', 'Gabon': 'Africa', 'Gambia': 'Africa',
                  'Ghana': 'Africa', 'Guinea': 'Africa', 'Guinea-Bissau': 'Africa', 'Ivory Coast': 'Africa',
                  'Kenya': 'Africa', 'Lesotho': 'Africa', 'Liberia': 'Africa', 'Libya': 'Africa',
                  'Madagascar': 'Africa', 'Malawi': 'Africa', 'Mali': 'Africa', 'Mauritania': 'Africa',
                  'Mauritius': 'Africa', 'Morocco': 'Africa', 'Mozambique': 'Africa', 'Namibia': 'Africa',
                  'Niger': 'Africa', 'Nigeria': 'Africa', 'Rwanda': 'Africa', 'Sao Tome and Principe': 'Africa',
                  'Senegal': 'Africa', 'Seychelles': 'Africa', 'Sierra Leone': 'Africa', 'Somalia': 'Africa',
                  'South Africa': 'Africa', 'South Sudan': 'Africa', 'Sudan': 'Africa', 'Tanzania': 'Africa',
                  'Togo': 'Africa', 'Tunisia': 'Africa', 'Uganda': 'Africa', 'Zambia': 'Africa',
                  'Zimbabwe': 'Africa', 'Australia': 'Oceania', 'Fiji': 'Oceania', 'Kiribati': 'Oceania',
                  'Marshall Islands': 'Oceania', 'Micronesia': 'Oceania', 'Nauru': 'Oceania',
                  'New Zealand': 'Oceania', 'Palau': 'Oceania', 'Papua New Guinea': 'Oceania', 'Samoa': 'Oceania',
                  'Solomon Islands': 'Oceania', 'Tonga': 'Oceania', 'Tuvalu': 'Oceania', 'Vanuatu': 'Oceania'}


def extract():
    logging.debug("extracting GDP from wikipedia")
    html = requests.get('https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29').text
    soup = BeautifulSoup(html, 'html.parser')
    gdp_table = soup.find("table", attrs={"class": "wikitable sortable sticky-header-multi static-row-numbers"})
    parsed = parser.make2d(gdp_table)

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

    for i, row in enumerate(parsed):
        for j, word in enumerate(row):
            parsed[i][j] = remove_nested_parens(word).replace(',', '')
    gdp_frame = pd.DataFrame(parsed[2:], columns=[parsed[0], parsed[1]])
    imf_gdp = gdp_frame[['Country/Territory', 'IMF']]
    logging.debug("extracted GDP from wikipedia done")
    return imf_gdp


def transform(imf_gdp):
    logging.debug("transforming step start")
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

    def get_region_column(imf_gdp):
        region = []
        for row in imf_gdp.iterrows():
            country = row[1]['Country']
            if country in country_region:
                region.append(country_region[country])
            else:
                region.append(None)
        return pd.DataFrame(region, columns=['Region'])

    region_column = get_region_column(imf_gdp)
    imf_gdp_with_region = pd.merge(imf_gdp, region_column,
                                   how='right', right_index=True, left_index=True)
    logging.debug("transforming step done")
    return imf_gdp_with_region


def load(imf_gdp_with_region, db_name='World_Economies.db', table_name='Countries_by_GDP'):
    def create_sqlite_database(db_name):
        conn = None
        try:
            conn = sqlite3.connect(db_name)
        except sqlite3.Error as e:
            print(e)
        finally:
            if conn:
                conn.close()

    logging.debug("loading GDP to db start")
    create_sqlite_database(db_name='World_Economies.db')
    with sqlite3.connect(db_name) as con:
        imf_gdp_with_region.to_sql(table_name, con, if_exists='replace', index_label='Rank')
    logging.debug("loading GDP in db done")
    return True


def get_countries_gdp_more_than_100b(db_name='World_Economies.db'):
    query = """
        SELECT * FROM Countries_by_GDP WHERE GDP_USD_billion >= 100
        """
    return sql_get(query)


def get_average_gdp_for_region(db_name='World_Economies.db'):
    query = """
        SELECT Region, ROUND(AVG(GDP_USD_billion),2) FROM
        (
            SELECT
                Region,
                GDP_USD_billion,
                RANK() OVER (PARTITION BY Region ORDER BY GDP_USD_billion DESC) AS RANKING
            FROM Countries_by_GDP
        )
        WHERE RANKING <= 5 AND Region IS NOT NULL
        GROUP BY Region
        """
    return sql_get(query)


def sql_get(query, db_name='World_Economies.db'):
    logging.debug("executing query {}".format(query))
    try:
        with sqlite3.connect(db_name) as conn:
            cur = conn.cursor()
            cur.execute(query)
            logging.debug("executed query done")
            return cur.fetchall()
    except sqlite3.Error as e:
        logging.debug("failed query")
        logging.debug(e)
        print(e)
        return []


if __name__ == '__main__':
    gdp_frame = extract()
    imf_gdp_with_region = transform(gdp_frame)
    load(imf_gdp_with_region)
    print(get_countries_gdp_more_than_100b())
    print(get_average_gdp_for_region())
