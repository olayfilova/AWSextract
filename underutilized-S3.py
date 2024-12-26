import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from tabulate import tabulate
from collections import defaultdict

load_dotenv()


class ServiceUtilizationAnalyzer:
    def __init__(self):
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region = os.getenv('AWS_REGION', 'us-east-1')

        # Initialize AWS clients
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

    def analyze_bucket_utilization(self, bucket_name, days=30):
        """Analyze bucket utilization patterns"""
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)

        metrics = {
            'GetRequests': 'NumberOfGETRequests',
            'PutRequests': 'NumberOfPUTRequests',
            'ListRequests': 'NumberOfLISTRequests',
            'BytesDownloaded': 'BytesDownloaded',
            'BytesUploaded': 'BytesUploaded'
        }

        utilization_data = defaultdict(int)

        for metric_name, metric in metrics.items():
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName=metric,
                    Dimensions=[{'Name': 'BucketName', 'Value': bucket_name}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=days * 24 * 3600,
                    Statistics=['Sum']
                )

                if response['Datapoints']:
                    utilization_data[metric_name] = response['Datapoints'][0]['Sum']
            except Exception as e:
                print(f"Error getting {metric_name} for {bucket_name}: {e}")

        return utilization_data

    def analyze_bucket_configurations(self, bucket_name):
        """Analyze bucket configurations for potential optimization"""
        config_status = {
            'Versioning': False,
            'Lifecycle': False,
            'Replication': False,
            'Analytics': False,
            'ExpressZone': False,
            'TransferAcceleration': False
        }

        try:
            # Check versioning
            versioning = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            config_status['Versioning'] = versioning.get('Status') == 'Enabled'

            # Check lifecycle rules
            try:
                lifecycle = self.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
                config_status['Lifecycle'] = True
            except self.s3_client.exceptions.ClientError:
                pass

            # Check replication
            try:
                replication = self.s3_client.get_bucket_replication(Bucket=bucket_name)
                config_status['Replication'] = True
            except self.s3_client.exceptions.ClientError:
                pass

            # Check analytics configurations
            try:
                analytics = self.s3_client.list_bucket_analytics_configurations(Bucket=bucket_name)
                config_status['Analytics'] = len(analytics.get('AnalyticsConfigurationList', [])) > 0
            except self.s3_client.exceptions.ClientError:
                pass

            # Check transfer acceleration
            try:
                acceleration = self.s3_client.get_bucket_accelerate_configuration(Bucket=bucket_name)
                config_status['TransferAcceleration'] = acceleration.get('Status') == 'Enabled'
            except self.s3_client.exceptions.ClientError:
                pass

        except Exception as e:
            print(f"Error analyzing configurations for {bucket_name}: {e}")

        return config_status

    def disable_unused_features(self, bucket_name, config_status, utilization_data):
        """Disable unused features based on analysis"""
        actions_taken = []

        try:
            # Check and disable versioning if unused
            if config_status['Versioning'] and utilization_data['PutRequests'] < 100:
                try:
                    self.s3_client.put_bucket_versioning(
                        Bucket=bucket_name,
                        VersioningConfiguration={'Status': 'Suspended'}
                    )
                    actions_taken.append("Disabled versioning")
                except Exception as e:
                    print(f"Error disabling versioning: {e}")

            # Disable transfer acceleration if unused
            if config_status['TransferAcceleration'] and utilization_data['BytesUploaded'] < 1024 * 1024 * 100:  # 100MB
                try:
                    self.s3_client.put_bucket_accelerate_configuration(
                        Bucket=bucket_name,
                        AccelerateConfiguration={'Status': 'Suspended'}
                    )
                    actions_taken.append("Disabled transfer acceleration")
                except Exception as e:
                    print(f"Error disabling transfer acceleration: {e}")

        except Exception as e:
            print(f"Error disabling features for {bucket_name}: {e}")

        return actions_taken

    def generate_report(self):
        """Generate comprehensive utilization report"""
        print("\n=== AWS S3 Service Utilization Analysis ===\n")

        try:
            buckets = self.s3_client.list_buckets()['fb-2024-25-12']

            for bucket in buckets:
                bucket_name = bucket['Name']
                print(f"\nAnalyzing bucket: {bucket_name}")

                # Analyze utilization
                utilization_data = self.analyze_bucket_utilization(bucket_name)

                # Analyze configurations
                config_status = self.analyze_bucket_configurations(bucket_name)

                # Print utilization data
                print("\nUtilization Metrics (Last 30 days):")
                utilization_df = pd.DataFrame([utilization_data]).T
                utilization_df.columns = ['Value']
                print(tabulate(utilization_df, headers='keys', tablefmt='grid'))

                # Print configuration status
                print("\nFeature Configuration Status:")
                config_df = pd.DataFrame([config_status]).T
                config_df.columns = ['Enabled']
                print(tabulate(config_df, headers='keys', tablefmt='grid'))

                # Identify underutilized features
                underutilized = []
                if config_status['Versioning'] and utilization_data['PutRequests'] < 100:
                    underutilized.append("Versioning (low write activity)")
                if config_status['TransferAcceleration'] and utilization_data['BytesUploaded'] < 1024 * 1024 * 100:
                    underutilized.append("Transfer Acceleration (low transfer volume)")
                if config_status['Analytics'] and sum(utilization_data.values()) < 1000:
                    underutilized.append("Analytics (low overall activity)")

                if underutilized:
                    print("\nUnderutilized Features:")
                    for feature in underutilized:
                        print(f"- {feature}")

                    # Ask for confirmation before disabling features
                    response = input("\nWould you like to disable underutilized features? (yes/no): ")
                    if response.lower() == 'yes':
                        actions = self.disable_unused_features(bucket_name, config_status, utilization_data)
                        if actions:
                            print("\nActions taken:")
                            for action in actions:
                                print(f"- {action}")

                print("\nRecommendations:")
                self.print_recommendations(bucket_name, utilization_data, config_status)

                print("-" * 80)

        except Exception as e:
            print(f"Error generating report: {e}")

    def print_recommendations(self, bucket_name, utilization_data, config_status):
        """Print specific recommendations based on analysis"""
        if sum(utilization_data.values()) == 0:
            print("- Consider deleting this bucket as it shows no activity")
            return

        if utilization_data['GetRequests'] > 1000 and not config_status['TransferAcceleration']:
            print("- Consider enabling Transfer Acceleration for better performance")

        if utilization_data['PutRequests'] < 10 and config_status['Versioning']:
            print("- Consider disabling versioning due to low write activity")

        if not config_status['Lifecycle'] and utilization_data['GetRequests'] > 0:
            print("- Consider implementing lifecycle rules for cost optimization")


def main():
    try:
        analyzer = ServiceUtilizationAnalyzer()
        analyzer.generate_report()
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
