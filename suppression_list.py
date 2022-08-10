import boto3
import csv
import os
from botocore.exceptions import ClientError
for region in ['us-east-1','ap-south-1']:
    client = boto3.client('sesv2',
                            aws_access_key_id=' ',
                            aws_secret_access_key=' ',
                            aws_session_token = ' '
                            )
    response = client.list_suppressed_destinations(
                # Reasons=[
                #     'BOUNCE'or'COMPLAINT',
                # ],
            )
    try:
        
        #print("Before Response")
        for suppressed_destination in response['SuppressedDestinationSummaries']:
            # print(f" Suppressed Destination: {suppressed_destination['EmailAddress']}")
            # print(f"Reason: {suppressed_destination['Reason']}")
            # #print(f"LastUpdateTime: {suppressed_destination['LastUpdateTime']}")
            #print("After Response")
            filename = 'suppression_list.csv'
            if os.path.isfile(filename):
                with open(filename, 'a', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=',')
                    csvwriter.writerow([suppressed_destination['EmailAddress'],suppressed_destination['Reason']])
            else:
                with open(filename, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=',')
                    csvwriter.writerow(['Email Address','Reason'])
                    csvwriter.writerow([suppressed_destination['EmailAddress'],suppressed_destination['Reason']])

    except ClientError as e:
        #print("No Email ID Found")
        print(f"No Email Found: {response['Error']['Message']}")
