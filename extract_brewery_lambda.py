import json
import csv
from multiprocessing.connection import Client
import urllib3
import os
import boto3
from botocore.client import ClientError
from pathlib import Path
import time

# This version of the script I had written prior to be run locally is ready to be used as an
# automated lambda function in AWS to extract, transform, and load data from a random brewery
# REST API -> json -> csv -> s3 -> AWS Athena table by way of AWS Glue

# lambda handler for main logic
def lambda_handler(event,context):
    brewery_data=get_random_brewery()
    if(does_filepath_exist('/tmp/data')==False):
        os.mkdir('/tmp/data')
    data_filepath=create_data_csv(brewery_data)

    s3=boto3.client('s3')
    establish_s3_connection(s3,data_filepath)
    call_glue_crawler(s3)

# function for get http request
def get_html_request(url):
    http=urllib3.PoolManager()
    try:
        r=http.request('GET',url)
    except:
        print('API connection unsuccessful.')

    return r

def get_random_brewery():
    url='https://api.openbrewerydb.org/breweries/random'
    r=get_html_request(url)
    
    txt=r.data

    # Prior inspection of json that comes in from URL shows it's always a list of objects. Appending [0] at the
    # end here since we'll always only be pulling exactly one brewery at a time.
    txt_dict=json.loads(txt)[0]
    headers=list(txt_dict.keys())
    
    return(headers,txt_dict)

# Return boolean when trying to check if a path already exist
def does_filepath_exist(str_path):
    path=Path(str_path)
    if(path.exists()):
        print('Path \''+str_path+'\' already exists.')
        return True
    else:
        return False

# Create the csv file that we need 
def create_data_csv(brewery_data):
    headers=brewery_data[0]
    brew_dict=brewery_data[1]
    data_row=[]

    for key in headers:
        data_row.append(brew_dict[key])

    ts=get_timestamp()
    path='/tmp/data/'+ts+'/'
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


# Make the connection with S3 and upload the file we created.
def establish_s3_connection(s3,data_filepath):
    bucket_name='brewery-project'

    s3_file=data_filepath.split('/')[4]
    ts=data_filepath.split('/')[3]

    # Adding dataload helps configure it for AWS Glue
    s3.upload_file(data_filepath,bucket_name,'data/dataload='+ts+'/'+s3_file)

# Call the glue crawler
def call_glue_crawler(s3):
    client=boto3.client('glue')
    glue_crawler='brewery_project'
    client.start_crawler(Name=glue_crawler)