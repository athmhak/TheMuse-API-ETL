#!/usr/bin/env python3

import requests
import json
import pandas as pd
import numpy as np
import subprocess
import sys
import toml
from typing import Dict

def read_api(url:str) -> Dict:
    """
    This function sends a GET request to the specified URL and returns the JSON data

    Args:
        url (str): The URL to send the GET request to

    Returns:
        dict: The JSON data returned from the API

    Raises:
        requests.exceptions.RequestException: If an error occurs while sending the GET request or processing the response.
    """

    try:
        response = requests.get(url)
        response.raise_for_status() #catch error HTTP response
        return response.json()
    
    except requests.exceptions.RequestException as err:
        print(
            f"[ERROR: ] occured reading the API with status code: {err.response.status_code}",
            file=sys.stderr
        )
        raise

# url = 'https://www.themuse.com/api/public/jobs?page=50'
# response = requests.get(url)
# data = response.json()

#data['results'][0]

def transform_data(data: Dict) -> pd.DataFrame:
    """
    This function transforms the raw data from the API into formatted DataFrame

    Args:
        data (Dict): The raw data returned from the API as a dictionary
    
    Returns:
        pd.DataFrame: A formatted DataFrame containing the extracted data

    Raises:
        Exception: If an error occurs while transforming the data

    """

    try:
        df = pd.DataFrame(columns = ['publication_date',
                                    'job_type',
                                    'job',
                                    'company',
                                    'city',
                                    'country'])

        # Loop through the results to extract required information
        for job in data['results']:
            publication_date = job.get('publication_date')[:10]
            job_type = job.get('type')
            job_name = job.get('name')
            company_name = job['company'].get('name') if job.get('company') else 'N/A'
            job_location = job['locations'][0].get('name') if job.get('locations') else 'N/A'
            job_city, job_country = job_location.split(',') if ',' in job_location else ('N/A', 'N/A')
            row = pd.DataFrame([{'publication_date':publication_date,
                                'job_type':job_type,
                                'job':job_name,
                                'company':company_name,
                                'city':job_city,
                                'country':job_country}])
            df=pd.concat([df,row],ignore_index=True)

        return df

    except Exception as err:
        print(f"[ERROR: ] occured building the DataFrame: {err}", file = sys.stderr)
        raise

def save_to_csv(df:pd.DataFrame, file_name: str) -> None:
    """
    This function saves a Pandas DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): A Pandas DataFrame to save to CSV.
        file_name (str): A string indicating the name of the CSV file.

    Returns:
        None if the file was saved successfully.
    
    Raises:
        Exception: If an error occurs while saving the DataFrame to CSV file.
    """

    try:
        df.to_csv(file_name, index=False)
    
    except Exception as err:
        print(f"[ERROR: ] saving DataFrame to CSV : {err}", file = sys.stderr)
        raise        

def upload_to_s3(file_name:str,bucket:str,folder:str) -> None:
    """
    This function uploads a file to an AWS S3 bucket.

    Args:
        file_name (str): The name of the file to upload.
        bucket (str): The name of the bucket to upload the file to.
        folder (str): The folder path inside the bucket to upload the file to.
        access_key (str): The AWS access key to access the s3 bucket.
        secret_acecss_key (str): The AWS secret access key to access the S3 bucket.

    Returns:
        None: None if the file was uploaded successfully.
    
    Raises:
        Exception: If any error occurs while uploading the file.
    """

    try:
        #use linux command to upload file to s3 bucket
        subprocess.run(['aws','s3','cp',file_name,'s3://py-project-jobs-s3bucket/input/jobs.csv'])
        #s3 = boto3.client("s3",aws_access_key_id = access_key,aws_secret_access_key = secret_access_key)
        #s3.upload_file(file_name,bucket,folder+file_name)
    
    except Exception as err:
        print(f"[ERROR: ] uploading file to S3: {err}", file = sys.stderr)
        raise


def main() -> None:
    """
    This function runs the main program that reads the API, transforms the data, saves it to a local CSV file and uploads it to AWS s3.

    Raises:
        Exception: If an error occurs during the execution of the program
    """

    try:
        app_config = toml.load('config.toml')
        url = app_config['api']['url']

        print('Reading the API....')
        data = read_api(url)
        print('API Done Reading!')

        print('Building DataFrame..')
        df = transform_data(data)

        file_name = 'jobs.csv'
        print(f"DataFrame saved to local file called {file_name}")
        save_to_csv(df,file_name)

        print('Uploading to AWS S3....')
        bucket = app_config['aws']['bucket']
        folder = app_config['aws']['folder']
        upload_to_s3(file_name,bucket,folder)
        print('File Uploading Done!')

    except Exception as err:
        print(f"[ERROR: ] in main: {err}", file = sys.stderr)
        raise

if __name__ == '__main__':
    main()










