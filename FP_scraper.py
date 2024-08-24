import requests
from bs4 import BeautifulSoup
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Airtable configuration
AIRTABLE_API_KEY = 'patj8UX1EgnGE8UQV.d1928de4668967c5ec6e5a8a60b79275126f6a9e37280fd8775c0516c83218c6'
AIRTABLE_BASE_ID = 'appnIDmJ3VDtptSkK'
AIRTABLE_TABLE_NAME = 'Finnpanel Views'

# URLs to scrape
URLS = {
    'YLE Areena': 'https://www.finnpanel.fi/tulokset/totaltv/yle/online14/3plus.html',
    'MTV.fi': 'https://www.finnpanel.fi/tulokset/totaltv/mtv/online14/3plus.html',
    'Ruutu': 'https://www.finnpanel.fi/tulokset/totaltv/sanoma/online14/3plus.html'
}

def get_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def scrape_data(url):
    session = get_session()
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.find('table', class_='totaltv')

        if not table:
            print(f"No data table found for URL: {url}")
            return []

        rows = table.find_all('tr')[1:]  # Skip header row
        data = []

        for row in rows:
            columns = row.find_all('td')
            if len(columns) >= 3:
                rank = columns[0].text.strip()
                title = columns[1].text.strip()
                views = columns[2].text.strip()
                data.append({
                    'Rank': rank,
                    'Title': title,
                    'Views': views
                })

        return data
    except requests.exceptions.RequestException as e:
        print(f"Error scraping data from {url}: {e}")
        return []

def push_to_airtable(service, data):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }

    records = []
    for item in data:
        record = {
            "fields": {
                "Service": service,
                "Date": datetime.now().strftime('%Y-%m-%d'),
                "Rank": item['Rank'],
                "Title": item['Title'],
                "Views": item['Views']
            }
        }
        records.append(record)

    payload = {"records": records}
    session = get_session()
    try:
        response = session.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        print(f"Successfully added {len(records)} records for {service}")
    except requests.exceptions.RequestException as e:
        print(f"Error adding records for {service}: {e}")

def main_job():
    for service, url in URLS.items():
        print(f"Scraping data for {service}...")
        data = scrape_data(url)
        if data:
            push_to_airtable(service, data)
        else:
            print(f"No data scraped for {service}")
    print(f"Data update completed at {datetime.now()}")

if __name__ == "__main__":
    main_job()