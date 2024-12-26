import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from tabulate import tabulate
import matplotlib.pyplot as plt

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')


def get_cost_and_usage(start_date, end_date):
    """Get AWS cost and usage data for S3"""
    client = boto3.client('ce',
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                          region_name=AWS_REGION)

    try:
        response = client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity='DAILY',
            Metrics=['UnblendedCost', 'UsageQuantity'],
            GroupBy=[
                {'Type': 'DIMENSION', 'Key': 'SERVICE'},
                {'Type': 'DIMENSION', 'Key': 'OPERATION'},
            ],
            Filter={
                'Dimensions': {
                    'Key': 'SERVICE',
                    'Values': ['Amazon Simple Storage Service']
                }
            }
        )
        return response['ResultsByTime']
    except Exception as e:
        print(f"Error getting cost data: {e}")
        return []


def analyze_s3_metrics():
    """Analyze S3 metrics including requests and storage"""
    cloudwatch = boto3.client('cloudwatch',
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                              region_name=AWS_REGION)
    s3 = boto3.client('s3',
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_REGION)

    metrics_data = {
        'GetRequests': 0,
        'PutRequests': 0,
        'ListRequests': 0,
        'CopyRequests': 0,
        'TotalStorage': 0,
        'BucketDetails': []
    }

    try:
        # Get list of all buckets
        buckets = s3.list_buckets()['XXXXXXX']

        for bucket in buckets:
            bucket_name = bucket['Name']
            print(f"Analyzing bucket: {bucket_name}")

            # Get bucket size and object count
            try:
                response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='BucketSizeBytes',
                    Dimensions=[{'Name': 'BucketName', 'Value': bucket_name},
                                {'Name': 'StorageType', 'Value': 'StandardStorage'}],
                    StartTime=datetime.now() - timedelta(days=1),
                    EndTime=datetime.now(),
                    Period=86400,
                    Statistics=['Average']
                )

                bucket_size = response['Datapoints'][0]['Average'] if response['Datapoints'] else 0
                metrics_data['TotalStorage'] += bucket_size

                # Get request metrics for the bucket
                for operation in ['Get', 'Put', 'List', 'Copy']:
                    try:
                        response = cloudwatch.get_metric_statistics(
                            Namespace='AWS/S3',
                            MetricName=f'NumberOf{operation}Requests',
                            Dimensions=[{'Name': 'BucketName', 'Value': bucket_name}],
                            StartTime=datetime.now() - timedelta(days=7),
                            EndTime=datetime.now(),
                            Period=86400,
                            Statistics=['Sum']
                        )

                        requests = sum(point['Sum'] for point in response['Datapoints'])
                        metrics_data[f'{operation}Requests'] += requests

                    except Exception as e:
                        print(f"Error getting {operation} requests for {bucket_name}: {e}")

                # Get bucket lifecycle rules
                try:
                    lifecycle = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
                    has_lifecycle = True
                except:
                    has_lifecycle = False

                # Get bucket versioning status
                try:
                    versioning = s3.get_bucket_versioning(Bucket=bucket_name)
                    versioning_status = versioning.get('Status', 'Disabled')
                except:
                    versioning_status = 'Unknown'

                metrics_data['BucketDetails'].append({
                    'BucketName': bucket_name,
                    'SizeBytes': bucket_size,
                    'HasLifecycle': has_lifecycle,
                    'Versioning': versioning_status
                })

            except Exception as e:
                print(f"Error analyzing bucket {bucket_name}: {e}")

        return metrics_data

    except Exception as e:
        print(f"Error in analyze_s3_metrics: {e}")
        return metrics_data


def generate_cost_report():
    """Generate a comprehensive cost report"""
    # Get dates for analysis
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    # Get cost data
    cost_data = get_cost_and_usage(start_date, end_date)

    # Get metrics data
    metrics_data = analyze_s3_metrics()

    # Create report
    print("\n=== AWS S3 Cost Analysis Report ===")
    print(f"Period: {start_date} to {end_date}")
    print("\n1. Storage Usage Summary:")
    print(f"Total Storage Used: {metrics_data['TotalStorage'] / (1024 ** 3):.2f} GB")

    print("\n2. Request Patterns (Last 7 days):")
    print(f"GET Requests: {metrics_data['GetRequests']:,}")
    print(f"PUT Requests: {metrics_data['PutRequests']:,}")
    print(f"LIST Requests: {metrics_data['ListRequests']:,}")
    print(f"COPY Requests: {metrics_data['CopyRequests']:,}")

    print("\n3. Bucket Details:")
    bucket_df = pd.DataFrame(metrics_data['BucketDetails'])
    if not bucket_df.empty:
        bucket_df['SizeGB'] = bucket_df['SizeBytes'] / (1024 ** 3)
        print(tabulate(bucket_df, headers='keys', tablefmt='grid'))

    print("\n4. Cost Breakdown by Operation:")
    cost_breakdown = []
    for day in cost_data:
        for group in day['Groups']:
            cost_breakdown.append({
                'Date': day['TimePeriod']['Start'],
                'Operation': group['Keys'][1],
                'Cost': float(group['Metrics']['UnblendedCost']['Amount']),
                'Usage': float(group['Metrics']['UsageQuantity']['Amount'])
            })

    if cost_breakdown:
        cost_df = pd.DataFrame(cost_breakdown)
        operation_costs = cost_df.groupby('Operation')['Cost'].sum().sort_values(ascending=False)

        print("\nTotal Cost by Operation:")
        print(tabulate(operation_costs.reset_index(), headers=['Operation', 'Cost ($)'], tablefmt='grid'))

        # Create a pie chart of costs
        plt.figure(figsize=(10, 6))
        plt.pie(operation_costs.values, labels=operation_costs.index, autopct='%1.1f%%')
        plt.title('S3 Costs by Operation')
        plt.axis('equal')
        plt.savefig('s3_cost_analysis.png')
        print("\nCost analysis chart saved as 's3_cost_analysis.png'")

    print("\n5. Cost Optimization Recommendations:")
    recommendations = []

    # Storage class recommendations
    for bucket in metrics_data['BucketDetails']:
        if not bucket['HasLifecycle']:
            recommendations.append(f"Consider adding lifecycle rules for bucket '{bucket['BucketName']}' "
                                   "to automatically transition objects to cheaper storage classes")

    # Versioning recommendations
    for bucket in metrics_data['BucketDetails']:
        if bucket['Versioning'] == 'Enabled':
            recommendations.append(f"Review versioning necessity for bucket '{bucket['BucketName']}'. "
                                   "Consider adding lifecycle rules to clean up old versions")

    if recommendations:
        print("\nRecommendations:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
    else:
        print("No specific recommendations at this time.")


if __name__ == "__main__":
    generate_cost_report()
