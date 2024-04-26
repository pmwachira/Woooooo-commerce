from google.cloud import bigquery
import requests
import os
from google.oauth2 import service_account
from requests.auth import HTTPBasicAuth
import json
from creds import token, collins_url, COL_COLUMNS, PROJECT_ID, COL_DEST_TABLE, creds_file_path, collins_end_points

def send_to_bigquery(result_set, page):
    table_id = bigquery.Table.from_string(COL_DEST_TABLE)

    errors = client.insert_rows_json(table_id, result_set)  # Make an API request.
    if errors == []:
        print("Page: "+str(page)+' added to bigquery')
    else:
        client.close()
        print("Encountered errors while inserting rows: {}".format(errors))
pass


def process_data(data, endpoint, page):
    batch=[]
    row = data[0]
    cols = data[0].keys()
    # check if new columns exist
    if len(COL_COLUMNS)!= len(cols):
        print('Columns number mismatch between data and definition')

    for row in data:
        row_hold={}
        for key in COL_COLUMNS:
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

    # gc creds
    with open(creds_file_path) as source:
        info = json.load(source)

    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    headers = {
        "Authorization": "Bearer "+token
    }

    for endpoint in collins_end_points:
        url = collins_url+endpoint
        page=1

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            # Do something with the JSON data
            process_data(data, endpoint, page)
        elif response.status_code == 429:
            print("Rate limit exceeded for endpoint: " + endpoint, response.status_code)

        else:
            print("Request failed with status code for endpoint: "+endpoint, response.status_code)


        while 'next' in response.links.keys():
            # get next link
            next_link = response.links.get('next').get('url')
            page=+1
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                # Do something with the JSON data
                process_data(data, endpoint, page)
            elif response.status_code == 429:
                print("Rate limit exceeded for endpoint: " + endpoint, response.status_code)

            else:
                print("Request failed with status code for endpoint: " + endpoint, response.status_code)

        client.close()



