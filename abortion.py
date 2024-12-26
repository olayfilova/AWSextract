import boto3
import os
from dotenv import load_dotenv

load_dotenv()


def stop_multipart_uploads():
    """Stop and clean up incomplete multipart uploads"""
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                             aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                             )

    try:
        # List all buckets
        buckets = s3_client.list_buckets()['XXXXXXX']

        for bucket in buckets:
            bucket_name = bucket['Name']
            print(f"Checking bucket: {bucket_name}")

            try:
                # List all multipart uploads
                multipart_uploads = s3_client.list_multipart_uploads(Bucket=bucket_name)

                # If there are any multipart uploads
                if 'Uploads' in multipart_uploads:
                    for upload in multipart_uploads['Uploads']:
                        try:
                            # Abort the multipart upload
                            s3_client.abort_multipart_upload(
                                Bucket=bucket_name,
                                Key=upload['Key'],
                                UploadId=upload['UploadId']
                            )
                            print(f"Aborted multipart upload for {upload['Key']}")
                        except Exception as e:
                            print(f"Error aborting upload for {upload['Key']}: {e}")

                # Set lifecycle policy to abort incomplete multipart uploads
                lifecycle_policy = {
                    'Rules': [
                        {
                            'ID': 'AbortIncompleteMultipartUpload',
                            'Status': 'Enabled',
                            'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 1},
                            'Filter': {'Prefix': ''}
                        }
                    ]
                }

                s3_client.put_bucket_lifecycle_configuration(
                    Bucket=bucket_name,
                    LifecycleConfiguration=lifecycle_policy
                )
                print(f"Set lifecycle policy for {bucket_name} to abort incomplete multipart uploads")

            except Exception as e:
                print(f"Error processing bucket {bucket_name}: {e}")

    except Exception as e:
        print(f"Error: {e}")


def optimize_copy_operations():
    """Add recommendations for optimizing copy operations"""
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                             aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                             )

    try:
        buckets = s3_client.list_buckets()['XXXXXXX']

        for bucket in buckets:
            bucket_name = bucket['Name']
            print(f"\nAnalyzing copy operations for bucket: {bucket_name}")

            # Get bucket versioning status
            try:
                versioning = s3_client.get_bucket_versioning(Bucket=bucket_name)
                if versioning.get('Status') == 'Enabled':
                    print(f"Recommendation: Consider disabling versioning in {bucket_name} to reduce copy operations")
            except Exception as e:
                print(f"Error checking versioning for {bucket_name}: {e}")

    except Exception as e:
        print(f"Error: {e}")


def main():
    print("Starting cost optimization...")

    # Stop multipart uploads
    print("\n1. Handling multipart uploads...")
    stop_multipart_uploads()

    # Optimize copy operations
    print("\n2. Analyzing copy operations...")
    optimize_copy_operations()

    print("\nRecommendations to reduce costs:")
    print("1. For any future uploads, use single-part uploads for files < 100MB")
    print("2. Review and potentially disable versioning where not needed")
    print("3. Consider using S3 Transfer Acceleration if frequent copies are necessary")
    print("4. Use server-side copy operations when possible")
    print("5. Batch small objects together before copying")


if __name__ == "__main__":
    main()
