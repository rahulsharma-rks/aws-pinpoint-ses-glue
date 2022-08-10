import boto3
import csv
import os
from botocore.exceptions import ClientError
for region in ['us-east-1','ap-south-1']:
    client = boto3.client('sesv2',
                            aws_access_key_id='ASIA3JAMRZORVMB5F3ED',
                            aws_secret_access_key='RMoxy8cq7RXcmsOGUfKs4EZYGtW64abYehSs3BXD',
                            aws_session_token = 'IQoJb3JpZ2luX2VjENf//////////wEaCXVzLWVhc3QtMSJIMEYCIQCJXvWWdIkzLkheMsNw2KndG/jftl4T6qdtsj6hIppOawIhAKo+Kr9nnuK1UpVpCnzlzZk+VYm9Ul/Xr7OkykPvSCbQKpIDCD8QABoMNzc1MjY3OTI4OTk1IgxbU0FERvTN5Tx/cuwq7wIfPUBErV6Dpf9sYuwnkAgHtlJeehRM+USZt1Z6H520A/NQNVpSEZKpuSIeC2qJylbMVjB4QoLaEKpoMTq8E6RF7IZlkrTQPXgZo80MR/DxXd4xOVUu0LXb2h3En2Xsc3ca1LOEPFPB2U/u+t2mmOMSaCfgu9IMVerBkmHwzU9JesukMqp8ZmUZP+gm/2eaTe3Nvf89jrrtWwTW8fiazwivn4tKRAtYd0VJRZxP0YD6B//7NHG8yS9uPmRTuKHrV8Dl3SEKBJqPM9VmFfDF7bDzOTvmMBJfukbCso+tPqJqjn35OlQLPqMfaNBYzyQHoqOoncO4HJvPzDJnkOO02/dce7F5kI29RLH1mgg0ulwW5ShButw3/JmYpNYbB84yhB6XAVMVoc2DWaoARYgcsiBV3UvcQZ2SmuMwlQoPkAA7gbyI/o218L2mY870oqFR2DHgP2aHFwcskoymoXKBBuvA9UHXH4rMRrjWed5jMFX6MIuZzZcGOqUBa44WayQg8OOoSaHuk6xSYuwxhwfUx9wRYJUzB9hsSac6XKA6FClVPBtanMUxStpzq3d7h6yC/9J7hJqIjbHBjp+OZVzLTEVEjRwER/Cac7yudx8sOpULnDzTNqg4mu+Tle4wjYK2vyrT2pOAE7j/1GNl4YhQPetcWSHIfNesN18o69zV/q2r7Vcy9DNsgpvtm1OcI+b1XufwH9dOKYvvnf/XY44T'
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
