from selenium import webdriver
from selenium.webdriver.common.by import By  
import urllib.parse
import pandas as pd  
import itertools
from concurrent.futures import ProcessPoolExecutor as Pool
import multiprocessing
import subprocess
import time
from pathlib import Path
import re
import requests

num_cpu = multiprocessing.cpu_count()

def setup_driver():
    ff_opts = webdriver.FirefoxOptions()
    dr = webdriver.Remote(
        command_executor = 'http://localhost:4444',
        options = ff_opts
    )
    return dr

def get_href(el):
    return el.get_attribute('href')


def run_selenium():
    subprocess.run('docker run --rm -d -p 4444:4444 \
        -p 7900:7900 --shm-size="2g" \
        -e SE_SESSION_REQUEST_TIMEOUT=600 \
        -e SE_NODE_MAX_SESSIONS=10 \
        -e SE_NODE_OVERRIDE_MAX_SESSIONS=true \
        --name selenium \
        selenium/standalone-firefox:latest', shell=True)
    time.sleep(5)

def stop_selenium():
    subprocess.run('docker stop selenium', shell=True)
  
def stream_download(url, path):
    if not path.exists():
        with requests.get(url, stream = True) as resp:
            with open(path, 'wb') as file:
                for chunk in resp.iter_content(chunk_size = 10 * 1024):
                    file.write(chunk)
        print(f'{path} written')
  
if __name__ == '__main__':
    run_selenium()
    drvr = setup_driver()
    # get all hrefs, just want zip files, data notes, definitions
    # latter 2 are mix of pdf & docx
    base_url = 'https://civilrightsdata.ed.gov/data'
    drvr.get(base_url)
    a_els = drvr.find_elements(By.XPATH, '//table//td/a')
    urls = pd.DataFrame({ 'url': [get_href(a) for a in a_els] })
    urls['ext'] = urls['url'].apply(lambda x: Path(x).suffix)
    urls = urls.loc[urls['ext'].isin(['.zip', '.pdf', '.docx', '.xlsx'])]
    urls['year'] = urls['url'].apply(lambda x: re.search('[0-9]{4}\-[0-9]{2}', x))
    urls['year'] = urls['year'].ffill()
    urls['year'] = urls['year'].apply(lambda x: x.group(0))

    urls.to_csv('urls_meta.csv', index = False)
    drvr.quit()
    
    # iterate over rows, make dirs by year if not exist
    for yr in urls['year'].unique():
        yr_dir = Path(yr)
        if not yr_dir.exists():
            yr_dir.mkdir()
            
    # prob should use urllib
    urls['fn'] = urls['url'].apply(lambda x: Path(x).name)
    urls['path_out'] = urls.apply(lambda x: Path(x['year']) / x['fn'], axis = 1)
    
    with Pool(num_cpu) as pool: 
        pool.map(stream_download, urls['url'], urls['path_out'])
    
    stop_selenium()