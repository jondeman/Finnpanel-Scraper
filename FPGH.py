import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from github import Github
from io import BytesIO
import os

def clean_and_convert(value, convert_to=int):
    cleaned = value.replace('#', '').replace('.', '').strip()
    try:
        return convert_to(cleaned)
    except ValueError:
        return None

def scrape_finnpanel(url):
    response = requests.get(url)
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
        print(f"Couldn't find table for {service}")
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
    
    return data

def upload_to_github(df, filename, repo_name, github_token):
    g = Github(github_token)
    repo = g.get_user().get_repo(repo_name)
    
    # Convert DataFrame to Excel
    excel_file = BytesIO()
    df.to_excel(excel_file, index=False, engine='openpyxl')
    excel_file.seek(0)
    
    try:
        contents = repo.get_contents(filename)
        repo.update_file(contents.path, f"Update {filename}", excel_file.getvalue(), contents.sha)
        print(f"File {filename} updated successfully")
    except:
        repo.create_file(filename, f"Create {filename}", excel_file.getvalue())
        print(f"File {filename} created successfully")

# GitHub configuration
GT_TOKEN = os.environ.get('GT_TOKEN')
GITHUB_REPO = 'Finnpanel-Scraper'  # Ensure this matches your actual repository name

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
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    # Sort data by viewers
    df = df.sort_values('Viewers', ascending=False).reset_index(drop=True)
    
    # Update ranks and add date
    df['Rank'] = df.index + 1
    df['Date'] = current_date
    
    # Reorder columns
    df = df[['Date', 'Rank', 'Service', 'Program', 'Episode', 'Duration', 'Viewers']]
    
    print(f"Scraped {len(df)} records")
    print("Sample data:")
    print(df.head())
    
    # Upload to GitHub
    filename = f'finnpanel_data_{current_date}.xlsx'
    upload_to_github(df, filename, GITHUB_REPO, GT_TOKEN)
    print(f"Data has been scraped on {current_date} and uploaded to GitHub")
else:
    print("No data was scraped. Please check the URLs and website structure.")
