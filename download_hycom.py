import os
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def get_file_list(url, end_str):
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        file_links = set()  # Using a set to automatically eliminate duplicates

        for link in soup.find_all('a', href=True):
            absolute_url = urljoin(url, link['href'])
            # Check if the link ends with "ts3z.nc"
            if urlparse(absolute_url).path.endswith(end_str):
                file_links.add(absolute_url)

        return sorted(list(file_links))
    else:
        print(f"Failed to retrieve content from {url}. Status code: {response.status_code}")
        return None

def download_file(file_url, download_folder):
    filename = file_url.split("/")[-1]
    file_path = os.path.join(download_folder, filename)
    print(f"Downloading {filename}...")
    
    response = requests.get(file_url, stream=True)
    
    if response.status_code == 200:
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=32768):
                file.write(chunk)
        print(f"Downloaded {filename} to {download_folder}")
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")

def download_files(file_list, download_folder):
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        executor.map(lambda file_url: download_file(file_url, download_folder), file_list)

for year in [2020, 2021, 2022]:
    print(f"Working on {year}")
    download_folder = f"/data/data658/cw_apt/NPP_JSherman/input_data/anc/hycom_mld/raw_hycom_data/{year}"  # Specify the folder where you want to save the files
    os.makedirs(download_folder, exist_ok=True)

    flist = get_file_list(f"https://data.hycom.org/datasets/GLBy0.08/expt_93.0/data/hindcasts/{year}/", "t000_ts3z.nc") 
    download_files(flist, download_folder)



#### 2012- 2015 GOFS 3.1 Reanalysis
# 2012 - get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_53.X/data/2012/", "t000.nc")
# 2013- get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_53.X/data/2013/", "t000.nc")
# 2014 - get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_53.X/data/2014/", "t000.nc")
# 2015- get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_53.X/data/2015/", "t000.nc")


#### 2016 - 2022 GOFS 3.1 Analysis
## 2016 data set is split up across a few expt as follows: Jan - Apr (expt_56.3) --> May - Dec (expt_57.2)
#  2016 - get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_56.3/data/2016/", "t000.nc")[:114] + get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_57.2/data/2016/", "t000.nc")

## 2017 data set is split up across a few expt as follows: Jan (expt_57.2) --> Feb - May (expt_92.8) --> June - Sept (expt_57.7) --> Oct - Dec (expt_92.9) 
# flist = get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_57.2/data/2017/", "t000.nc") +\
#     get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_92.8/data/2017/", "t000_ts3z.nc") +\
#     get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_57.7/data/2017/", "t000.nc")  +\
#     get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_92.9/data/2017/", "t000_ts3z.nc")

# 2018 - get_file_list("https://data.hycom.org/datasets/GLBv0.08/expt_93.0/data/hindcasts/2018/", "t000_ts3z.nc") 

# 2019 - get_file_list(f"https://data.hycom.org/datasets/GLBv0.08/expt_93.0/data/hindcasts/2019/", "t000_ts3z.nc") 

### Feb 2020 there was a grid change from GLBv to GLBy
# 2020/2021/2022 - get_file_list(f"https://data.hycom.org/datasets/GLBv0.08/expt_93.0/data/hindcasts/{year}/", "t000_ts3z.nc")

'''GLBy0.08 grid is 0.08 deg lon x 0.04 deg lat that covers 80S to 90N.
** GLBv0.08 Discontinued on 2020-Feb-18 ** grid is 0.08 deg lon x 0.08 deg lat between 40S-40N.
Poleward of 40S/40N, the grid is 0.08 deg lon x 0.04 deg lat. It spans 80S to 90N.'''