import json
import csv
import requests
import os
from pathlib import Path
import pandas as pd

def main():
    api_reached_bool=contact_api()
    if(api_reached_bool==True):
        brewery_data=get_random_brewery()
    if(does_filepath_exist('./data')==False):
        os.mkdir('./data')
    if(does_filepath_exist('./data/id_lookup.csv')==False):
        create_csv(brewery_data)

def contact_api():
    url='https://api.openbrewerydb.org/breweries'

    try:
        r=get_html_request(url)

        # Check if our connection to the API is valid!
        if r.status_code==200:
            return True
    except:
        print('API connectivity not found!') # Real logging would be nice here

def get_random_brewery():
    url='https://api.openbrewerydb.org/breweries/random'
    r=get_html_request(url)
    txt=r.text

    # Prior inspection of json that comes in from URL shows it's always a list of objects. Appending [0] at the
    # end here since we'll always only be pulling exactly one brewery at a time.
    txt_dict=json.loads(txt)[0]
    headers=list(txt_dict.keys())
    
    return(headers,txt_dict)

def create_csv():
    print('hahaha')
    
def does_filepath_exist(str_path):
    path=Path(str_path)
    if(path.exists()):
        print('Path \''+str_path+'\' already exists.')
        return True
    else:
        return False

def get_html_request(url):
    r=requests.get(url)

    return r

main()