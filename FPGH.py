import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from github import Github
from io import BytesIO
import os
import sys
import traceback
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_and_convert(value, convert_to=int):
    cleaned = value.replace('#', '').replace('.', '').strip()
    try:
        return convert_to(cleaned)
    except ValueError:
        logging.warning(f"Could not convert value: {value}")
        return None

def scrape_finnpanel(url):
    logging.info(f"Scraping URL: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        if 'mtv' in url:
            service = 'MTV Katsomo'
        elif 'sanoma' in url:
            service = 'Ruutu'
        elif 'yle' in url:
            service = 'Yle Areena'
        else:
            service = 'Unknown'
        
        data = []
        table = soup.find('table', class_='totaltv')
        if not table:
            logging.warning(f"Couldn't find table for {service}")
            return data
        rows = table.find_all('tr')[1:]
        
        for row in rows:
            cols = row.find_all(['th', 'td'])
            if len(cols) >= 5:
                rank = clean_and_convert(cols[0].text)
                program = cols[1].text.strip()
                episode = cols[2].text.strip() if len(cols) > 5 else ''
                duration = cols[-2].text.strip()
                viewers = clean_and_convert(cols[-1].text.replace('\xa0', ''))
                
                if rank is not None and viewers is not None:
                    data.append({
                        'Rank': rank,
                        'Service': service,
                        'Program': program,
                        'Episode': episode,
                        'Duration': duration,
                        'Viewers': viewers
                    })
        
        logging.info(f"Scraped {len(data)} records from {service}")
        return data
    except Exception as e:
        logging.error(f"Error scraping {url}: {str(e)}")
        return []

def upload_to_github(df, filename, repo_name, github_token):
    logging.info(f"Uploading file: {filename}")
    try:
        g = Github(github_token)
        repo = g.get_user().get_repo(repo_name)
        
        excel_file = BytesIO()
        df.to_excel(excel_file, index=False, engine='openpyxl')
        excel_file.seek(0)
        
        try:
            contents = repo.get_contents(filename)
            repo.update_file(contents.path, f"Update {filename}", excel_file.getvalue(), contents.sha)
            logging.info(f"File {filename} updated successfully")
        except:
            repo.create_file(filename, f"Create {filename}", excel_file.getvalue())
            logging.info(f"File {filename} created successfully")
    except Exception as e:
        logging.error(f"Error uploading to GitHub: {str(e)}")
        raise

# Main execution
try:
    logging.info("Starting Finnpanel scraper")
    GT_TOKEN = os.environ.get('GT_TOKEN')
    GITHUB_REPO = 'Finnpanel-Scraper'

    if not GT_TOKEN:
        raise ValueError("GT_TOKEN environment variable is not set")

    urls = [
        'https://www.finnpanel.fi/tulokset/totaltv/mtv/online14/3plus.html',
        'https://www.finnpanel.fi/tulokset/totaltv/sanoma/online14/3plus.html',
        'https://www.finnpanel.fi/tulokset/totaltv/yle/online14/3plus.html'
    ]

    current_date = datetime.now().strftime('%Y-%m-%d')
    all_data = []

    for url in urls:
        all_data.extend(scrape_finnpanel(url))

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.sort_values('Viewers', ascending=False).reset_index(drop=True)
        df['Rank'] = df.index + 1
        df['Date'] = current_date
        df = df[['Date', 'Rank', 'Service', 'Program', 'Episode', 'Duration', 'Viewers']]
        
        logging.info(f"Total scraped records: {len(df)}")
        logging.info("Sample data:")
        logging.info(df.head().to_string())
        
        filename = f'finnpanel_data_{current_date}.xlsx'
        upload_to_github(df, filename, GITHUB_REPO, GT_TOKEN)
        logging.info(f"Data has been scraped on {current_date} and uploaded to GitHub")
    else:
        logging.warning("No data was scraped. Please check the URLs and website structure.")
        sys.exit(1)

except Exception as e:
    logging.error(f"An error occurred: {str(e)}")
    logging.error("Traceback:")
    logging.error(traceback.format_exc())
    sys.exit(1)
