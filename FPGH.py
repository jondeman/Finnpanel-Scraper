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

URL_TEMPLATE = 'https://www.finnpanel.fi/tulokset/totaltv/{service}/{period}/{demo}.html'
SERVICES = ['yle', 'mtv', 'sanoma']

# Finnpanel TotalTV target groups (kohderyhmät): page name in the URL -> value written to the Demo column
DEMOS = {
    '3plus': '3+',        # Kaikki 3 vuotta täyttäneet
    'alle45': 'Alle 45',  # Alle 45-vuotiaat
    '45plus': '45+',      # 45 vuotta täyttäneet
    'ikar2564': '25-64',  # 25–64-vuotiaat
}

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
        response = requests.get(url, timeout=30)
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

def upload_to_github(df, filename, repo_identifier, github_token):
    logging.info(f"Uploading file: {filename}")
    try:
        g = Github(github_token)
        # Use full repo name if provided (owner/repo) to avoid needing GET /user
        if "/" in repo_identifier:
            repo = g.get_repo(repo_identifier)
        else:
            repo = g.get_user().get_repo(repo_identifier)
        
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

def process_data(period_path, period):
    all_data = []
    for demo, demo_label in DEMOS.items():
        demo_data = []
        for service in SERVICES:
            url = URL_TEMPLATE.format(service=service, period=period_path, demo=demo)
            demo_data.extend(scrape_finnpanel(url))
        if not demo_data:
            logging.warning(f"No data was scraped for demo {demo_label} in {period} period")
        for record in demo_data:
            record['Demo'] = demo_label
        all_data.extend(demo_data)
    
    if all_data:
        df = pd.DataFrame(all_data)
        # Rank across services within each demo, keeping demos in DEMOS order
        demo_order = {label: i for i, label in enumerate(DEMOS.values())}
        df['DemoOrder'] = df['Demo'].map(demo_order)
        df = df.sort_values(['DemoOrder', 'Viewers'], ascending=[True, False]).reset_index(drop=True)
        df['Rank'] = df.groupby('Demo').cumcount() + 1
        df['Date'] = datetime.now().strftime('%Y-%m-%d')
        df = df[['Date', 'Demo', 'Rank', 'Service', 'Program', 'Episode', 'Duration', 'Viewers']]
        
        logging.info(f"Total scraped records for {period} period: {len(df)}")
        logging.info(f"Records per demo: {df['Demo'].value_counts(sort=False).to_dict()}")
        logging.info("Sample data:")
        logging.info(df.head().to_string())
        
        return df
    else:
        logging.warning(f"No data was scraped for {period} period. Please check the URLs and website structure.")
        return None

def main():
    try:
        logging.info("Starting Finnpanel scraper")
        # Prefer custom secret GT_TOKEN; fallback to default GitHub Actions token
        GT_TOKEN = os.environ.get('GT_TOKEN') or os.environ.get('GITHUB_TOKEN')
        # Use full repo name when running in GitHub Actions, e.g. owner/repo
        GITHUB_REPO = os.environ.get('GITHUB_REPOSITORY', 'Finnpanel-Scraper')

        if not GT_TOKEN:
            raise ValueError("GT_TOKEN or GITHUB_TOKEN environment variable is not set")

        current_date = datetime.now().strftime('%Y-%m-%d')

        # Process 14-day data
        df_14d = process_data('online14', "14-day")
        if df_14d is not None:
            filename_14d = f'14D_Finnpanel_data_{current_date}.xlsx'
            upload_to_github(df_14d, filename_14d, GITHUB_REPO, GT_TOKEN)
            logging.info(f"14-day data has been scraped on {current_date} and uploaded to GitHub")

        # Process 90-day data
        df_90d = process_data('online90', "90-day")
        if df_90d is not None:
            filename_90d = f'90D_Finnpanel_data_{current_date}.xlsx'
            upload_to_github(df_90d, filename_90d, GITHUB_REPO, GT_TOKEN)
            logging.info(f"90-day data has been scraped on {current_date} and uploaded to GitHub")

        if df_14d is None and df_90d is None:
            logging.error("No data was scraped for either period. Exiting with error.")
            sys.exit(1)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error("Traceback:")
        logging.error(traceback.format_exc())
        sys.exit(1)

# Main execution
if __name__ == '__main__':
    main()
