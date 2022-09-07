import json
import boto3
import xlsxwriter
import datetime

current_time = datetime.datetime.now()

def fix_json_log (file_key):
    with open(f'/tmp/{file_key}', 'r') as input_f, open('/tmp/output.json', 'w') as output_f:
        output_f.write('[')
        tmp = input_f.read()
        tmp = tmp.replace('}{', '},{')
        tmp = tmp.replace('}\n{', '},{')
        output_f.write(tmp)
        output_f.write(']')


def count_elements(mail_event_list):
    g = {}
    for i in mail_event_list:
        if i in g:
            g[i] += 1
        else:
            g[i] = 1
    return g


def lambda_handler(event, context):
    # print(f'event: {event}')
    
    for record in event["Records"]:
        file_key = record["s3"]["object"]["key"]
        print(file_key)
    
    s3_client = boto3.resource("s3")
    s3_export_client = boto3.client('s3')
    s3_client.meta.client.download_file("output-bucket", file_key, f'/tmp/{file_key}')
    
    mail_events_workbook = xlsxwriter.Workbook(f'/tmp/mail-events-log-{current_time}.xlsx')
    click_worksheet = mail_events_workbook.add_worksheet('Click List')
    click_log_header = ["Destination", "Clicks"]
    click_worksheet.write_row(0, 0, click_log_header)
    open_worksheet = mail_events_workbook.add_worksheet('Open List')
    open_log_header = ["Destination", "Opens"]
    open_worksheet.write_row(0, 0, open_log_header)
    
    
    fix_json_log(file_key)
    
    mail_list = []
    open_list = []
    click_list = []
    
    with open(f'/tmp/output.json', 'r+') as input_json_file:
        events = json.loads(input_json_file.read())
        for mail_event in events:
            # print(mail_event)
            try:
                event_type = mail_event["Event"]
                destination = mail_event["Destination"][0]
                
                if (event_type == "_email.open"):
                    open_list.append(destination)
                elif (event_type == "_email.click"):
                    click_list.append(destination)
                else:
                    continue
                
            except:
                continue
    
    open_count_dict = count_elements(open_list)
    click_count_dict = count_elements(click_list)
    
    print(open_count_dict)
    print(click_count_dict)
    
    click_row_count = 1
    for i in click_count_dict:
        destination = i
        click_count = click_count_dict[i]
        click_worksheet.write_row(click_row_count, 0, [destination, click_count])
    
    open_row_count = 1
    for i in open_count_dict:
        destination = i
        open_count = open_count_dict[i]
        open_worksheet.write_row(open_row_count, 0, [destination, open_count])
        open_row_count += 1 
    
    
    mail_events_workbook.close()
    s3_export_client.upload_file(f'/tmp/mail-events-log-{current_time}.xlsx', 'deliverystream19', f'output/mail-events-log-{current_time}.xlsx')
