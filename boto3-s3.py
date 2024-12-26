import boto3
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
from dotenv import load_dotenv
from random import uniform

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE')
SPREADSHEET_KEY = os.getenv('SPREADSHEET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
S3_PREFIX = os.getenv('S3_PREFIX')
SUBFOLDER_TYPES = ['text/', 'warc/', 'wat/', 'wet/']

def clean_folder_name(folder_name):
    return folder_name.strip("'")


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


def get_s3_folders_and_contents():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=S3_PREFIX,
        Delimiter='/'
    )

    folders_data = {}
    for prefix in response.get('CommonPrefixes', []):
        folder_name = clean_folder_name(prefix['Prefix'].split('/')[-2])
        folders_data[folder_name] = {}

        # Get contents for each subfolder type
        for subfolder_type in SUBFOLDER_TYPES:
            subfolder_prefix = f"{prefix['Prefix']}{subfolder_type}"
            subfolder_contents = s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=subfolder_prefix
            )

            contents = []
            for item in subfolder_contents.get('Contents', []):
                contents.append([
                    item['Key'].split('/')[-1],
                    str(item['LastModified']),
                    str(item['Size']),
                    item['Key']
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

    # Update main sheet
    main_sheet = spreadsheet.sheet1
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Prepare main sheet data
    main_data = [
        ['Timestamp:', timestamp],
        ['Folder Names'],
        []
    ]

    # Add folder names to main data
    for folder_name in folders_data.keys():
        main_data.append([folder_name])

    # Update main sheet in one batch
    update_sheet_with_retry(main_sheet, main_data)

    # Process each folder and its subfolders
    for folder_name, subfolder_data in folders_data.items():
        print(f"Processing folder: {folder_name}")

        for subfolder_type, contents in subfolder_data.items():
            sheet_name = f"{folder_name}.{subfolder_type}"
            print(f"Processing subfolder: {sheet_name}")

            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(sheet_name, rows=1000, cols=20)
                time.sleep(2)

            # Prepare folder sheet data
            sheet_data = [
                ['Folder Contents:', sheet_name],
                ['Last Updated:', timestamp],
                [],
                ['File Name', 'Last Modified', 'Size (bytes)', 'Full Path']
            ]
            sheet_data.extend(contents)

            # Update in batch
            update_sheet_with_retry(worksheet, sheet_data)
            print(f"Completed sheet for: {sheet_name}")


def main():
    try:
        folders_data = get_s3_folders_and_contents()
        update_google_sheets(folders_data)
        print("Successfully updated Google Sheets with all folder and subfolder contents")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()



def get_s3_folders():
    # Create S3 client with credentials from env variables
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=S3_PREFIX,
        Delimiter='/'
    )

    folders = []
    for prefix in response.get('CommonPrefixes', []):
        folder_name = prefix['Prefix'].split('/')[-2]
        folders.append([folder_name])

    return folders


def update_google_sheet(folders):
    # Using direct service account authentication
    client = gspread.service_account(filename=CREDENTIALS_FILE)

    # Open spreadsheet by key
    spreadsheet = client.open_by_key(SPREADSHEET_KEY)
    worksheet = spreadsheet.sheet1

    # Add timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Clear existing content and write new data
    worksheet.clear()
    worksheet.append_row(['Timestamp:', timestamp])
    worksheet.append_row(['Folder Names'])

    # Write folder names
    for folder in folders:
        worksheet.append_row(folder)


def main():
    try:
        folders = get_s3_folders()
        update_google_sheet(folders)
        print("Successfully updated Google Sheet with folder names")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()