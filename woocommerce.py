from google.cloud import bigquery
import requests
from requests.auth import HTTPBasicAuth
import json
from creds import URL, KEY, SECRET, COLUMNS, PROJECT_ID, DEST_TABLE

def send_to_bigquery(result_set):
    rows_to_insert = []
    # for i in result_set:
    #     for j in COLUMNS:
    #         rows_to_insert.append({j: i[0], u"transformed_address": i[1]})

    errors = client.insert_rows_json(table_id, result_set)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
pass


def process_data(data):
    batch=[]
    # check if new columns exist
    if len(COLUMNS)!= len(data[0].keys()):
        print('Columns number mismatch between data and definition')

    # for row in data:
    #     row_hold=[]
    #     for key in COLUMNS:
    #         row_hold+=row[key]
    #
    #     batch+=row_hold

    send_to_bigquery(data)

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

    client = bigquery.Client(project=PROJECT_ID)
    table_id = bigquery.Table.from_string(DEST_TABLE)

    url=URL
    consumer_key=KEY
    consumer_secret=SECRET

    response = requests.get(url, auth=HTTPBasicAuth(consumer_key, consumer_secret))

    data = json.loads(response.text)
    
    process_data(data)
    
    while 'next' in response.links.keys():
        # get next link
        next_link = response.links.get('next').get('url')
        print(next_link)
        response = requests.get(next_link, auth=HTTPBasicAuth(consumer_key, consumer_secret))
        data = json.loads(response.text)

        process_data(data)

    client.close()

