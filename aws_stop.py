import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import os
from dotenv import load_dotenv


class MemoryDBManager:
    def __init__(self):
        load_dotenv()
        self.memorydb = boto3.client('memorydb',
                                     aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                     aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                     region_name=os.getenv('AWS_REGION', 'us-east-1')
                                     )

    def delete_parameter_group(self, parameter_group_name):
        try:
            response = self.memorydb.delete_parameter_group(
                ParameterGroupName=parameter_group_name
            )
            print(f"Successfully deleted parameter group: {parameter_group_name}")
            return response
        except ClientError as e:
            if 'InvalidParameterGroupState' in str(e):
                print(
                    f"Cannot delete parameter group {parameter_group_name}: This might be a default parameter group or it's associated with clusters")
            else:
                print(f"Error deleting parameter group {parameter_group_name}: {e}")
            return None


def main():
    try:
        manager = MemoryDBManager()

        response = manager.memorydb.describe_parameter_groups()

        print("\nAvailable Parameter Groups:")
        for group in response['ParameterGroups']:
            print(f"Name: {group['Name']}")
            print(f"ARN: {group['ARN']}")
            print("---")

        group_name = input("\nEnter the name of the parameter group to delete (or 'exit' to quit): ")

        if group_name.lower() == 'exit':
            print("Operation cancelled.")
            return

        if group_name.startswith('default.'):
            print("Cannot delete default parameter groups. Please select a custom parameter group.")
            return

        confirm = input(f"\nAre you sure you want to delete parameter group '{group_name}'? (yes/no): ")

        if confirm.lower() == 'yes':
            manager.delete_parameter_group(group_name)
        else:
            print("Operation cancelled.")

    except NoCredentialsError:
        print(
            "Error: Unable to locate credentials. Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in your environment or .env file.")
    except ClientError as e:
        print(f"AWS Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
