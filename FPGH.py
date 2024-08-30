import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from github import Github
from io import BytesIO
import os
import sys
import traceback

# ... (keep all other functions as they were) ...

# Main execution
try:
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
        sys.exit(1)

except Exception as e:
    print(f"An error occurred: {str(e)}")
    print("Traceback:")
    print(traceback.format_exc())
    sys.exit(1)
