import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()


class FreeTierAnalyzer:
    def __init__(self):
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region = os.getenv('AWS_REGION', 'us-east-1')

        # Initialize AWS clients
        self.freetier_client = boto3.client('freetier',
                                            aws_access_key_id=self.aws_access_key_id,
                                            aws_secret_access_key=self.aws_secret_access_key,
                                            region_name=self.region
                                            )

        self.ce_client = boto3.client('ce',
                                      aws_access_key_id=self.aws_access_key_id,
                                      aws_secret_access_key=self.aws_secret_access_key,
                                      region_name=self.region
                                      )

        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.aws_access_key_id,
                                      aws_secret_access_key=self.aws_secret_access_key,
                                      region_name=self.region
                                      )

    def get_free_tier_usage(self):
        """Get current Free Tier usage"""
        try:
            response = self.freetier_client.get_free_tier_usage()
            return response.get('FreeTierUsages', [])
        except Exception as e:
            print(f"Error getting Free Tier usage: {e}")
            return []

    def analyze_s3_free_tier(self):
        """Analyze S3 usage against Free Tier limits"""
        s3_usage = {
            'storage': 0,  # Standard storage in GB
            'get_requests': 0,
            'put_requests': 0,
            'data_transfer_out': 0  # in GB
        }

        # S3 Free Tier limits
        FREE_TIER_LIMITS = {
            'storage': 5,  # 5GB Standard storage
            'get_requests': 20000,  # 20,000 GET requests
            'put_requests': 2000,  # 2,000 PUT requests
            'data_transfer_out': 1  # 1GB data transfer out
        }

        try:
            #S3 storage usage
            buckets = self.s3_client.list_buckets()['fb-2024-25-12']

            for bucket in buckets:
                bucket_name = bucket['Name']

                #bucket size
                try:
                    paginator = self.s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=bucket_name):
                        for obj in page.get('Contents', []):
                            s3_usage['storage'] += obj['Size'] / (1024 * 1024 * 1024)  # Convert to GB
                except Exception as e:
                    print(f"Error getting size for bucket {bucket_name}: {e}")

            # Get request metrics for the current month
            end_time = datetime.now()
            start_time = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

            cloudwatch = boto3.client('cloudwatch',
                                      aws_access_key_id=self.aws_access_key_id,
                                      aws_secret_access_key=self.aws_secret_access_key,
                                      region_name=self.region
                                      )

            metrics = {
                'get_requests': 'NumberOfGETRequests',
                'put_requests': 'NumberOfPUTRequests'
            }

            for usage_type, metric_name in metrics.items():
                try:
                    response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/S3',
                        MetricName=metric_name,
                        Dimensions=[],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=2592000,  # 30 days in seconds
                        Statistics=['Sum']
                    )

                    if response['Datapoints']:
                        s3_usage[usage_type] = int(response['Datapoints'][0]['Sum'])
                except Exception as e:
                    print(f"Error getting {metric_name} metrics: {e}")

            #usage percentages and create report
            usage_report = []
            for resource, usage in s3_usage.items():
                limit = FREE_TIER_LIMITS[resource]
                percentage = (usage / limit) * 100 if limit > 0 else 0
                status = "WARNING" if percentage >= 80 else "OK"

                usage_report.append({
                    'Resource': resource,
                    'Usage': f"{usage:.2f}",
                    'Limit': limit,
                    'Percentage': f"{percentage:.1f}%",
                    'Status': status
                })

            return usage_report

        except Exception as e:
            print(f"Error analyzing S3 Free Tier usage: {e}")
            return []

    def get_cost_forecast(self):
        """Get cost forecast for the current month"""
        try:
            end_date = datetime.now() + timedelta(days=30)
            start_date = datetime.now()

            response = self.ce_client.get_cost_forecast(
                TimePeriod={
                    'Start': start_date.strftime('%Y-%m-%d'),
                    'End': end_date.strftime('%Y-%m-%d')
                },
                Metric='UNBLENDED_COST',
                Granularity='MONTHLY'
            )

            return response.get('Total', {}).get('Amount', 0)
        except Exception as e:
            print(f"Error getting cost forecast: {e}")
            return 0

    def print_report(self):
        """Print comprehensive Free Tier usage report"""
        print("\n=== AWS Free Tier Usage Analysis ===\n")

        #S3 Free Tier usage
        s3_usage_report = self.analyze_s3_free_tier()

        if s3_usage_report:
            print("S3 Free Tier Usage:")
            print(tabulate(s3_usage_report, headers='keys', tablefmt='grid'))

            # Print warnings for high usage
            print("\nWarnings:")
            warnings = [item for item in s3_usage_report if item['Status'] == "WARNING"]
            if warnings:
                for warning in warnings:
                    print(f"⚠️  High usage alert: {warning['Resource']} at {warning['Percentage']} of Free Tier limit")
            else:
                print("No warnings - all services within safe limits")

        # Get cost forecast
        forecast = self.get_cost_forecast()
        print(f"\nProjected costs for next 30 days: ${float(forecast):.2f}")

        # Print recommendations
        print("\nRecommendations:")
        for item in s3_usage_report:
            if float(item['Percentage'].strip('%')) > 80:
                resource = item['Resource']
                if resource == 'storage':
                    print(f"- Consider cleaning up unnecessary files in S3 buckets")
                elif resource in ['get_requests', 'put_requests']:
                    print(f"- Review application logic for potential optimization of {resource}")
                elif resource == 'data_transfer_out':
                    print(f"- Consider implementing caching to reduce data transfer")

        print("\nAction Items:")
        print("1. Set up AWS Budgets alerts for early warning")
        print("2. Review and clean up unused resources")
        print("3. Implement recommended optimizations")
        print("4. Monitor usage regularly")


def main():
    try:
        analyzer = FreeTierAnalyzer()
        analyzer.print_report()
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
