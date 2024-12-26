import boto3
import gspread
from datetime import datetime
import os
from dotenv import load_dotenv
import time
from random import uniform
import gzip
import io
from warcio.archiveiterator import ArchiveIterator
import json
from bs4 import BeautifulSoup


load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE')
SPREADSHEET_KEY = os.getenv('SPREADSHEET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
S3_PREFIX = os.getenv('S3_PREFIX')
SUBFOLDER_TYPES = ['text/', 'warc/', 'wat/', 'wet/']



def extract_gz_content(s3_client, key, max_records=5):
    """Extract content from gzipped WARC/WAT/WET files"""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        gz_content = response['Body'].read()

        extracted_data = []
        with gzip.GzipFile(fileobj=io.BytesIO(gz_content)) as gz_file:
            if '.wat.gz' in key:
                for record in ArchiveIterator(gz_file):
                    if len(extracted_data) >= max_records:
                        break
                    if record.rec_type == 'metadata':
                        try:
                            content = record.content_stream().read().decode('utf-8')
                            json_content = json.loads(content)
                            extracted_data.append({
                                'URL': record.rec_headers.get_header('WARC-Target-URI', 'N/A'),
                                'Type': 'WAT',
                                'Content': str(json_content)[:500]
                            })
                        except Exception as e:
                            print(f"Error processing WAT record: {e}")
                            continue

            elif '.wet.gz' in key:
                for record in ArchiveIterator(gz_file):
                    if len(extracted_data) >= max_records:
                        break
                    if record.rec_type == 'conversion':
                        try:
                            content = record.content_stream().read().decode('utf-8')
                            extracted_data.append({
                                'URL': record.rec_headers.get_header('WARC-Target-URI', 'N/A'),
                                'Type': 'WET',
                                'Content': content[:500]
                            })
                        except Exception as e:
                            print(f"Error processing WET record: {e}")
                            continue

            elif '.warc.gz' in key:
                # Process WARC file
                for record in ArchiveIterator(gz_file):
                    if len(extracted_data) >= max_records:
                        break
                    if record.rec_type == 'response':
                        try:
                            content = record.content_stream().read().decode('utf-8', errors='ignore')
                            soup = BeautifulSoup(content, 'html.parser')
                            extracted_data.append({
                                'URL': record.rec_headers.get_header('WARC-Target-URI', 'N/A'),
                                'Type': 'WARC',
                                'Content': soup.get_text()[:500]
                            })
                        except Exception as e:
                            print(f"Error processing WARC record: {e}")
                            continue

        return extracted_data

    except Exception as e:
        print(f"Error extracting content from {key}: {e}")
        return []


def exponential_backoff(attempt):
    wait_time = min(300, (2 ** attempt) + uniform(0, 1))
    time.sleep(wait_time)


def retry_with_backoff(func, max_attempts=5):
    for attempt in range(max_attempts):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                print(f"Rate limit hit, attempt {attempt + 1} of {max_attempts}")
                exponential_backoff(attempt)
                continue
            raise
    raise Exception("Max retry attempts reached")


def get_all_folders(s3_client):
    paginator = s3_client.get_paginator('list_objects_v2')
    folders = []

    for page in paginator.paginate(
            Bucket=BUCKET_NAME,
            Prefix=S3_PREFIX,
            Delimiter='/'
    ):
        folders.extend(page.get('CommonPrefixes', []))

    return folders


def clean_folder_name(folder_name):
    return folder_name.strip("'")


def get_s3_folders_and_contents():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    folders_data = {}
    paginator = s3_client.get_paginator('list_objects_v2')

    for prefix in get_all_folders(s3_client):
        folder_name = clean_folder_name(prefix['Prefix'].split('/')[-2])
        folders_data[folder_name] = {}

        for subfolder_type in SUBFOLDER_TYPES:
            subfolder_prefix = f"{prefix['Prefix']}{subfolder_type}"
            contents = []

            for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=subfolder_prefix):
                for item in page.get('Contents', []):
                    extracted_content = extract_gz_content(s3_client, item['Key'])

                    for content in extracted_content:
                        contents.append([
                            item['Key'].split('/')[-1],
                            str(item['LastModified']),
                            content['URL'],
                            content['Type'],
                            content['Content']
                        ])

            folders_data[folder_name][subfolder_type.strip('/')] = contents

    return folders_data


def update_sheet_with_retry(worksheet, data):
    def update():
        worksheet.clear()
        worksheet.update('A1', data)
        time.sleep(1)

    retry_with_backoff(update)


def update_google_sheets(folders_data):
    client = gspread.service_account(filename=CREDENTIALS_FILE)
    spreadsheet = client.open_by_key(SPREADSHEET_KEY)

    main_sheet = spreadsheet.sheet1
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    main_data = [
        ['Timestamp:', timestamp],
        ['Folder Names'],
        []
    ]

    for folder_name in folders_data.keys():
        main_data.append([folder_name])

    update_sheet_with_retry(main_sheet, main_data)

    for folder_name, subfolder_data in folders_data.items():
        print(f"Processing folder: {folder_name}")

        for subfolder_type, contents in subfolder_data.items():
            sheet_name = f"{folder_name}.{subfolder_type}"
            print(f"Processing subfolder: {sheet_name}")

            try:
                worksheet = spreadsheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(sheet_name, rows=1000, cols=20)
                time.sleep(2)

            sheet_data = [
                ['Folder Contents:', sheet_name],
                ['Last Updated:', timestamp],
                [],
                ['File Name', 'Last Modified', 'URL', 'Type', 'Content Preview']
            ]
            sheet_data.extend(contents)

            update_sheet_with_retry(worksheet, sheet_data)
            print(f"Completed sheet for: {sheet_name}")


def main():
    try:
        folders_data = get_s3_folders_and_contents()
        update_google_sheets(folders_data)
        print("Successfully updated Google Sheets with extracted content")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()


