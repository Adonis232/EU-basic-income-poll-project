url = 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes'
html = requests.get(url).content


soup = bs(html, 'lxml')
soup = soup.find_all('td')

lst = []

for i in soup:
    if str(i) != ' ':
        lst.append(str(i)[4:-6])

countries = []
codes = []

for i in range(0, len(lst[:56])): # Hasta UK
    if i%2==0:
        countries.append(lst[i])
    else:
        codes.append((lst[i].strip())[1:-1])

pd.DataFrame(zip(countries, codes), columns=['country', 'code']).to_parquet('../data/raw/webscraping_country_code-name.parquet')
