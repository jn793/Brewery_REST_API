import json
import csv
import requests
import os
import boto3
from pathlib import Path
import pandas as pd


def main():
    api_reached_bool=contact_api()
    if(api_reached_bool==True):
        # non_dupe_bool=False
        # while(non_dupe_bool==False):
            brewery_data=get_random_brewery()
    if(does_filepath_exist('./data')==False):
        os.mkdir('./data')
    if(does_filepath_exist('./data/id_lookup.csv')==False):
        print('File not found - creating id_lookup.csv')
        create_id_csv()
    else:
        print('File found - id_lookup.csv')
    create_data_csv(brewery_data)

def contact_api():
    url='https://api.openbrewerydb.org/breweries'

    try:
        r=get_html_request(url)

        # Check if our connection to the API is valid!
        if r.status_code==200:
            return True
    except:
        print('API connectivity not found!') # Real logging would be nice here

def get_html_request(url):
    r=requests.get(url)

    return r

def get_random_brewery():
    url='https://api.openbrewerydb.org/breweries/random'
    r=get_html_request(url)
    txt=r.text

    # Prior inspection of json that comes in from URL shows it's always a list of objects. Appending [0] at the
    # end here since we'll always only be pulling exactly one brewery at a time.
    txt_dict=json.loads(txt)[0]
    headers=list(txt_dict.keys())
    
    return(headers,txt_dict)

def check_brewery_duplicate(id):
    df=pd.read_csv('./data/id_lookup.csv')
    if(df['id'].isin(['id']).empty):
        return False
    else:
        return True

def does_filepath_exist(str_path):
    path=Path(str_path)
    if(path.exists()):
        print('Path \''+str_path+'\' already exists.')
        return True
    else:
        return False

def create_id_csv():
    id_str=['id']
    f=open('./data/id_lookup.csv','w')
    writer=csv.writer(f)
    writer.writerow(id_str)
    f.close()

def append_id_csv(id):
    id_str=[id]
    # id_str.append(id)
    f=open('./data/id_lookup.csv','a+')
    writer=csv.writer(f)
    writer.writerow(id_str)
    f.close()

def create_data_csv(brewery_data):
    headers=brewery_data[0]
    brew_dict=brewery_data[1]
    data_row=[]
    
    append_id_csv(brew_dict['id'])

    for key in headers:
        data_row.append(brew_dict[key])

    



main()