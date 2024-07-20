import os
import requests
from bs4 import BeautifulSoup
from retrying import retry

data_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
data_dir = "data/2019/"

# Create directory if it does not exist
os.makedirs(data_dir, exist_ok=True)


def get_download_links():
    response = requests.get(data_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    links = soup.find_all('a')
    download_links = []

    for link in links:
        href = link.get('href')
        if href and "yellow_tripdata_2019" in href:
            download_links.append(href)

    return download_links


@retry(stop_max_attempt_number=3, wait_fixed=2000)
def download_file(url):
    local_filename = os.path.join(data_dir, os.path.basename(url))
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
    else:
        response.raise_for_status()


if __name__ == "__main__":
    try:
        download_links = get_download_links()
        for url in download_links:
            try:
                download_file(url)
                print(f"Downloaded {os.path.basename(url)}")
            except Exception as e:
                print(f"Failed to download {os.path.basename(url)}: {e}")
    except Exception as e:
        print(f"Failed to retrieve download links: {e}")
