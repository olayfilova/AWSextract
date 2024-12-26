import dask
import dask.dataframe as dd
import dask.bag as db
from dask.distributed import Client
import requests
import json
import boto3
from datetime import datetime
import pandas as pd
from io import BytesIO
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

session = boto3.Session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')

s3_client = session.client('s3')



class CommonCrawlProcessor:
    def __init__(self, bucket_name='fb-2024-25-12'):
        self.bucket_name = bucket_name
        self.indexes = [
            'CC-MAIN-2024-30',
            'CC-MAIN-2024-33',
            'CC-MAIN-2024-38',
            'CC-MAIN-2024-42',
            'CC-MAIN-2024-46',
            'CC-MAIN-2024-52'
        ]

    def query_index(self, url, index):
        cc_url = f"https://index.commoncrawl.org/{index}-index"
        params = {
            'url': url,
            'output': 'json'
        }

        try:
            response = requests.get(cc_url, params=params)
            if response.status_code == 200:
                return [json.loads(line) for line in response.text.strip().split('\n') if line]
        except Exception as e:
            print(f"Error querying {index} for {url}: {e}")
        return []

    def save_to_s3(self, data, folder_name, index):
        """Save data to specific S3 folder with index information"""
        if not data:
            return

        s3_client = boto3.client('s3')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        df = pd.DataFrame(data)

        #to S3 with indx info in path
        buffer = BytesIO()
        df.to_parquet(buffer)

        key = f'{folder_name}/{index}/{timestamp}.parquet'

        try:
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue()
            )
            print(f"Successfully saved {len(df)} records to {key}")
        except Exception as e:
            print(f"Error saving to S3: {e}")

    def process_with_dask(self, target_urls):
        client = Client()  #init Dask

        try:
            for index in self.indexes:
                print(f"\nProcessing index: {index}")

                #Dask bag for parallel processing
                urls_bag = db.from_sequence(target_urls)

                #query indx for each URL
                records_bag = urls_bag.map(lambda url: self.query_index(url, index))

                #store in different folders
                folders = {
                    'process_crawl': lambda x: x,  # Raw data
                    'explore_crawl': lambda x: [r for r in x if r.get('status') == '200'],  # Successful requests
                    'raw_data': lambda x: x,  # All data
                    'processed_data': lambda x: [r for r in x if r.get('mime') == 'text/html'],  # HTML only
                }

                #folder in parallel
                for folder_name, processor_func in folders.items():
                    print(f"Processing for folder: {folder_name}")

                    processed_records = records_bag.map(processor_func).compute()

                    # flatten results and save
                    all_records = [item for sublist in processed_records for item in sublist]
                    self.save_to_s3(all_records, folder_name, index)

        finally:
            client.close()


def main():
    target_urls = [
        "*.no/*",
        "www.vg.no/*",
        "*.aftenposten.no/*",
        "https://data.commoncrawl.org/*"

    ]

    processor = CommonCrawlProcessor(bucket_name='fb-2024-25-12')

    #memory management
    dask.config.set({
        'distributed.worker.memory.target': 0.6,
        'distributed.worker.memory.spill': 0.7,
        'distributed.worker.memory.pause': 0.8,
    })

    processor.process_with_dask(target_urls)


if __name__ == "__main__":
    main()

