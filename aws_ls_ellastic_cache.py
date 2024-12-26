import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv


def verify_and_print_credentials():
    """Verify and print AWS credentials (safely)"""
    load_dotenv()

    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_REGION', 'us-east-1')

    print("\nChecking AWS Credentials:")
    print(f"Access Key exists: {bool(access_key)}")
    print(f"Secret Key exists: {bool(secret_key)}")
    print(f"Region: {region}")

    if access_key:
        print(f"Access Key ends with: ...{access_key[-4:]}")

    return bool(access_key and secret_key)


class AWSServiceManager:
    def __init__(self):
        load_dotenv()

        if not verify_and_print_credentials():
            raise NoCredentialsError("AWS credentials not found in environment variables")

        self.session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )

        self.elasticache = self.session.client('elasticache')
        self.rds = self.session.client('rds')

    def list_and_stop_services(self):
        """List and stop both ElastiCache and RDS services"""
        try:
            print("\nChecking ElastiCache clusters...")
            cache_response = self.elasticache.describe_cache_clusters()

            if 'CacheClusters' in cache_response:
                for cluster in cache_response['CacheClusters']:
                    print(f"Found cluster: {cluster['CacheClusterId']}")
            else:
                print("No ElastiCache clusters found")

            print("\nChecking RDS instances...")
            rds_response = self.rds.describe_db_instances()

            if 'DBInstances' in rds_response:
                for instance in rds_response['DBInstances']:
                    print(f"Found RDS: {instance['DBInstanceIdentifier']}")
            else:
                print("No RDS instances found")

        except ClientError as e:
            print(f"AWS API Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")


def main():
    try:
        if not os.path.exists('../.env'):
            print("Creating new .env file...")
            with open('../.env', 'w') as f:
                f.write("""
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-1
""".strip())
            print("\nPlease add your AWS credentials to the .env file and run again")
            return

        manager = AWSServiceManager()
        manager.list_and_stop_services()

    except NoCredentialsError as e:
        print(f"\nError: {e}")
        print("\nPlease ensure your .env file contains:")
        print("AWS_ACCESS_KEY_ID=your_access_key")
        print("AWS_SECRET_ACCESS_KEY=your_secret_key")
        print("AWS_REGION=us-east-1")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
