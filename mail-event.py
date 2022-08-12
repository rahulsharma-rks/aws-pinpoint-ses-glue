import json
import boto3
import datetime
import xlsxwriter

def fix_json_log ():
    with open('/tmp/ses-log', 'r') as input_f, open('/tmp/output.json', 'w') as output_f:
        output_f.write('[')
        tmp = input_f.read()
        tmp = tmp.replace('}{', '},{')
        tmp = tmp.replace('}\n{', '},{')
        output_f.write(tmp)
        output_f.write(']')

def lambda_handler(event, context):
    
    current_time = datetime.datetime.now()
    s3_client = boto3.client('s3')
    s3_export_client = boto3.client('s3')
    
    #Log File Name
    filename = "PUT-S3-KpQy8-Pinpoint-1-2022-07-19-13-21-04-966084d1-dd1b-4997-aa85-7ea7bc92009e"
    s3_client.download_file('deliverystream19', 'PUT-S3-KpQy8-Pinpoint-1-2022-07-19-13-21-04-966084d1-dd1b-4997-aa85-7ea7bc92009e', '/tmp/ses-log')

    fix_json_log()
    
    pinpoint_workbook = xlsxwriter.Workbook(f'/tmp/pinpoint-log.xlsx')
    
    opens_worksheet = pinpoint_workbook.add_worksheet('Opens')
    opens_header_keys = ['User ID', 'Event Type', 'Destination', 'Time']
    opens_worksheet.write_row(0, 0, opens_header_keys)
    
    clicks_worksheet = pinpoint_workbook.add_worksheet('Clicks')
    clicks_header_keys = ['User ID', 'Event Type', 'Destination', 'Time']
    clicks_worksheet.write_row(0, 0, clicks_header_keys)
    
    click_list = []
    open_list = []

    opens_row_count = 1
    clicks_row_count = 1
    with open('/tmp/output.json', 'r+') as input_json_file:
        events = json.loads(input_json_file.read())
        
        for event in events:
            if event["event_type"] == "_email.open" or event["event_type"] == "_email.click":
                for header in event["facets"]["email_channel"]["mail_event"]["mail"]["headers"]:
                    if header["name"] == "Date":
                        event_time = header["value"].split("+")[0]
                        break
                
                event_type = event["event_type"]
                destination = event["facets"]["email_channel"]["mail_event"]["mail"]["destination"][0]
                user_id = event["attributes"]["user_id"]
                
                attributes = [user_id, event_type, destination, event_time]
                
                if event["event_type"] == "_email.open":
                    open_list.append(attributes)
                    opens_worksheet.write_row(opens_row_count, 0, attributes)
                    opens_row_count += 1
                else:
                    click_list.append(attributes)
                    clicks_worksheet.write_row(clicks_row_count, 0, attributes)
                    clicks_row_count =+ 1
    
    print(open_list)
    print(click_list)
    
    pinpoint_workbook.close()
    
    s3_export_client.upload_file(f'/tmp/pinpoint-log.xlsx', 'pinpoint-lambda-output', f'pinpoint-{current_time}.xlsx')
    
