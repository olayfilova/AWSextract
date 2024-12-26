import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from collections import defaultdict
from tabulate import tabulate

load_dotenv()


class S3OperationsOptimizer:
    def __init__(self):
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region = os.getenv('AWS_REGION', 'us-east-1')

        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.aws_access_key_id,
                                      aws_secret_access_key=self.aws_secret_access_key,
                                      region_name=self.region
                                      )

        self.cloudwatch = boto3.client('cloudwatch',
                                       aws_access_key_id=self.aws_access_key_id,
                                       aws_secret_access_key=self.aws_secret_access_key,
                                       region_name=self.region
                                       )

    def analyze_request_patterns(self, bucket_name, days=7):
        """Analyze request patterns for a specific bucket"""
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)

        metrics = {
            'GetRequests': 'NumberOfGETRequests',
            'PutRequests': 'NumberOfPUTRequests',
            'ListRequests': 'NumberOfLISTRequests',
            'CopyRequests': 'NumberOfCOPYRequests'
        }

        pattern_data = defaultdict(int)

        for metric_name, cloudwatch_metric in metrics.items():
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName=cloudwatch_metric,
                    Dimensions=[{'Name': 'BucketName', 'Value': bucket_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,  # 1-hour periods
                    Statistics=['Sum']
                )

                for datapoint in response['Datapoints']:
                    hour = datapoint['Timestamp'].hour
                    pattern_data[hour] += int(datapoint['Sum'])

            except Exception as e:
                print(f"Error getting {metric_name} metrics for {bucket_name}: {e}")

        return pattern_data

    def analyze_object_size_distribution(self, bucket_name):
        """Analyze object size distribution in a bucket"""
        size_distribution = defaultdict(int)
        total_objects = 0

        paginator = self.s3_client.get_paginator('list_objects_v2')

        try:
            for page in paginator.paginate(Bucket=bucket_name):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        size = obj['Size']
                        total_objects += 1

                        if size < 1024:  # < 1KB
                            size_distribution['< 1KB'] += 1
                        elif size < 102400:  # < 100KB
                            size_distribution['< 100KB'] += 1
                        elif size < 1048576:  # < 1MB
                            size_distribution['< 1MB'] += 1
                        elif size < 10485760:  # < 10MB
                            size_distribution['< 10MB'] += 1
                        else:  # >= 10MB
                            size_distribution['≥ 10MB'] += 1

        except Exception as e:
            print(f"Error analyzing object sizes in {bucket_name}: {e}")

        return size_distribution, total_objects

    def analyze_access_patterns(self, bucket_name):
        """Analyze object access patterns"""
        try:
            response = self.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
            has_lifecycle_rules = True
        except:
            has_lifecycle_rules = False

        return has_lifecycle_rules

    def generate_optimization_recommendations(self):
        """Generate comprehensive optimization recommendations"""
        recommendations = []
        bucket_stats = []

        try:
            # Corrected bucket listing
            response = self.s3_client.list_buckets()
            buckets = response['Buckets']  # This is the correct key

            for bucket in buckets:
                bucket_name = bucket['Name']
                print(f"\nAnalyzing bucket: {bucket_name}")

                # Analyze request patterns
                request_patterns = self.analyze_request_patterns(bucket_name)

                # Analyze object sizes
                size_distribution, total_objects = self.analyze_object_size_distribution(bucket_name)

                # Analyze access patterns
                has_lifecycle_rules = self.analyze_access_patterns(bucket_name)

                # Generate bucket-specific recommendations
                bucket_recommendations = []

                # Check for small object optimization opportunities
                if size_distribution.get('< 100KB', 0) > (total_objects * 0.5 if total_objects > 0 else 0):
                    bucket_recommendations.append(
                        f"High proportion of small objects detected. Consider implementing object combining for files < 100KB"
                    )

                # Check for request pattern optimization opportunities
                peak_hour = max(request_patterns, key=request_patterns.get) if request_patterns else 0
                peak_requests = request_patterns[peak_hour] if request_patterns else 0

                if peak_requests > 1000:
                    bucket_recommendations.append(
                        f"High request volume detected during hour {peak_hour}. Consider implementing caching"
                    )

                # Check lifecycle rules
                if not has_lifecycle_rules:
                    bucket_recommendations.append(
                        "No lifecycle rules detected. Consider implementing lifecycle policies for cost optimization"
                    )

                # Add to bucket stats
                bucket_stats.append({
                    'Bucket': bucket_name,
                    'Total Objects': total_objects,
                    'Size Distribution': dict(size_distribution),
                    'Peak Hour': peak_hour,
                    'Peak Requests': peak_requests,
                    'Has Lifecycle Rules': has_lifecycle_rules,
                    'Recommendations': bucket_recommendations
                })

        except Exception as e:
            print(f"Error generating recommendations: {e}")
            raise

        return bucket_stats

    def print_optimization_report(self, bucket_stats):
        """Print detailed optimization report"""
        print("\n=== S3 Operations Optimization Report ===\n")

        for stat in bucket_stats:
            print(f"\nBucket: {stat['Bucket']}")
            print("=" * (len(stat['Bucket']) + 8))

            print("\nSize Distribution:")
            if stat['Size Distribution']:
                size_df = pd.DataFrame([stat['Size Distribution']]).T
                size_df.columns = ['Count']
                print(tabulate(size_df, headers='keys', tablefmt='grid'))
            else:
                print("No objects found in bucket")

            print(f"\nRequest Patterns:")
            print(f"Peak Hour: {stat['Peak Hour']:02d}:00")
            print(f"Peak Requests: {stat['Peak Requests']:,}")

            print(f"\nLifecycle Rules: {'Configured' if stat['Has Lifecycle Rules'] else 'Not Configured'}")

            print("\nRecommendations:")
            if stat['Recommendations']:
                for i, rec in enumerate(stat['Recommendations'], 1):
                    print(f"{i}. {rec}")
            else:
                print("No specific recommendations at this time")

            print("\n" + "-" * 80)


def main():
    try:
        optimizer = S3OperationsOptimizer()
        bucket_stats = optimizer.generate_optimization_recommendations()
        optimizer.print_optimization_report(bucket_stats)
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()

