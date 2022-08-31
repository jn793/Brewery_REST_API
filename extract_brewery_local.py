import configparser
import json
import csv
from multiprocessing.connection import Client
import requests
import os
import boto3
from botocore.client import ClientError
from pathlib import Path
#import pandas as pd
import time

# First completed version - This script pulls JSON data from a REST API that generates data for a random
# brewery in the US. It then converts that data to csv format and uploads it as an object to AWS S3
# using boto3. An AWS Glue Crawler then ingests the data into a database table stored on that same S3 bucket.

# Logging, error validation, etc. should be added. Further work on this would involve possibly
# creating the AWS Glue Crawler programatically rather than using the AWS Console GUI,
# configuring it to run on AWS Lambda, and adding functions that manipulate the created Glue DB data using SQL.

# And stretch stretch stretch goal.... maybe making a "Random Brewery Twitter Bot"?


def main():
    api_reached_bool=contact_api()
    if(api_reached_bool==True):
        brewery_data=get_random_brewery()
    else:
        exit()

    if(does_filepath_exist('./data')==False):
        os.mkdir('./data')
    
    # The below code block exists as I originally intended to create validation to make sure a duplicate brewery gets ingested. Keeping the code in just in case I 
    # use it again but I saw no reason to block off duplicates on the first iteration.

    # if(does_filepath_exist('./data/id_lookup.csv')==False):
    #     print('File not found - creating id_lookup.csv')
    #     create_id_csv()
    # else:
    #     print('File found - id_lookup.csv')
    
    data_filepath=create_data_csv(brewery_data)
    configs=get_cfg_details('credentials.conf')
    establish_s3_connection(configs,data_filepath)

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

# Check to see that the brewery isn't already in our list. (CURRENTLY UNUSED)
def check_brewery_duplicate(id):
    df=pd.read_csv('./data/id_lookup.csv')

    if(df['id'].isin(['id']).empty):
        return True
    else:
        return False

# Return boolean when trying to check if a path already exist
def does_filepath_exist(str_path):
    path=Path(str_path)
    if(path.exists()):
        print('Path \''+str_path+'\' already exists.')
        return True
    else:
        return False

# Create a csv file that stores the brewery id lookup (CURRENTLY UNUSED)
def create_id_csv():
    id_str=['id']
    f=open('./data/id_lookup.csv','w')
    writer=csv.writer(f)
    writer.writerow(id_str)
    f.close()

# Append to the csv file that stores the brewery id lookup (CURRENTLY UNUSED)
def append_id_csv(id):
    id_str=[id]
    f=open('./data/id_lookup.csv','a+')
    writer=csv.writer(f)
    writer.writerow(id_str)
    f.close()

# Create the csv file that we need 
def create_data_csv(brewery_data):
    headers=brewery_data[0]
    brew_dict=brewery_data[1]
    data_row=[]
    
    #append_id_csv(brew_dict['id'])

    for key in headers:
        data_row.append(brew_dict[key])

    ts=get_timestamp()
    path='./data/'+ts+'/'
    os.mkdir(path)

    filepath=path+brew_dict['id']+'.csv'
    f=open(filepath,'w')
    writer=csv.writer(f)
    writer.writerow(headers)
    writer.writerow(data_row)
    f.close()

    return filepath

# Get the timestamp in epoch time for dataload purposes
def get_timestamp():
    ts=time.time()
    ts=str(ts).split('.')[0]

    return ts

# Grab our config details from our conf file and store them in a dictionary to be returned for other functions.
def get_cfg_details(conf_file):
    parser=configparser.ConfigParser()
    parser.read(conf_file)
    configs={}
    configs['access_key']=parser.get('aws_boto_credentials','access_key')
    configs['secret_key']=parser.get('aws_boto_credentials','secret_key')
    configs['bucket_name']=parser.get('aws_boto_credentials','bucket_name')
    configs['account_id']=parser.get('aws_boto_credentials','account_id')

    return configs
# Make the connection with S3 and upload the file we created.
def establish_s3_connection(configs,data_filepath):
    s3=boto3.client('s3',aws_access_key_id=configs['access_key'],
    aws_secret_access_key=configs['secret_key'])

    s3_file=data_filepath.split('/')[3]
    ts=data_filepath.split('/')[2]

    # Adding dataload helps configure it for AWS Glue
    s3.upload_file(data_filepath,configs['bucket_name'],'data/dataload='+ts+'/'+s3_file)


main()