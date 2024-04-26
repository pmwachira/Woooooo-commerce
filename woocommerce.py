from google.cloud import bigquery
import os
from google.oauth2 import service_account
import requests
from requests.auth import HTTPBasicAuth
import json
from creds import URL, KEY, SECRET, WOO_COLUMNS, PROJECT_ID, WOO_DEST_TABLE, creds_file_path

def send_to_bigquery(result_set, page):

    errors = client.insert_rows_json(table_id, result_set)  # Make an API request.
    if errors == []:
        print("Page:"+str(page)+" added to bigquery")
    else:
        client.close()
        print("Encountered errors while inserting rows: {}".format(errors))
pass


def process_data(data, page):
    batch=[]
    row = data[0]
    cols = data[0].keys()
    conv = str(data[0].get('billing'))
    # check if new columns exist
    if len(WOO_COLUMNS)!= len(cols):
        print('Columns number mismatch between data and definition')

    for row in data:
        row_hold={}
        for key in WOO_COLUMNS:
            row_hold[key]=str(row[key])

        batch.append(row_hold)

    send_to_bigquery(batch, page)

    return 0
    



# def check_if_table_exists():
#     table_id = bigquery.Table.from_string(DEST_TABLE)
#     schema = [
#         bigquery.SchemaField("original_address", "STRING", mode="REQUIRED"),
#         bigquery.SchemaField("transformed_address", "STRING", mode="REQUIRED")
#     ]
#     table = bigquery.Table(table_id, schema=schema)
#     table = client.create_table(table)
#     print(
#         "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
#     )
#     return table_id


if __name__ == '__main__':
    # print(os.getcwd())
    # gc creds
    with open(creds_file_path) as source:
        info = json.load(source)

    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    table_id = bigquery.Table.from_string(WOO_DEST_TABLE)

    # url=URL
    # incase of failure, especially db lock
    url = "https://www.davywine.co.uk/wp-json/wc/v3/orders?per_page=100&page=23"
    consumer_key=KEY
    consumer_secret=SECRET

    page=23
    response = requests.get(url, auth=HTTPBasicAuth(consumer_key, consumer_secret))

    data = json.loads(response.text)
    
    process_data(data, page)
    
    while 'next' in response.links.keys():
        page+=1
        # get next link
        next_link = response.links.get('next').get('url')
        response = requests.get(next_link, auth=HTTPBasicAuth(consumer_key, consumer_secret))
        data = json.loads(response.text)

        process_data(data, page)

    client.close()

