from google.cloud import bigquery
import urllib.parse
import os
from datetime import datetime, timedelta
from google.oauth2 import service_account
import requests
from requests.auth import HTTPBasicAuth
import json
from creds import URL, KEY, SECRET, WOO_COLUMNS_ORDERS,WOO_COLUMNS_CUSTOMERS, WOO_COLUMNS_PRODUCTS, WOO_DEST_TABLE_CUSTOMERS, PROJECT_ID, WOO_DEST_TABLE_ORDERS, creds_file, woo_endpoints

def send_to_bigquery(result_set, endpoint, page):
    # dynamic table insert
    # column checks
    if endpoint == 'orders':
        WOO_DEST_TABLE = WOO_DEST_TABLE_ORDERS
    elif endpoint == 'customers':
        WOO_DEST_TABLE = WOO_DEST_TABLE_CUSTOMERS

    else:
        WOO_DEST_TABLE = []

    table_id = bigquery.Table.from_string(WOO_DEST_TABLE)

    errors = client.insert_rows_json(table_id, result_set)  # Make an API request.

    if errors == []:
        print("woo Endpoint: "+endpoint+" Page:"+str(page)+" added to bigquery"+str(len(result_set)))
    else:
        print("woo Endpoint: "+endpoint+" Page:"+str(page) + " Encountered errors while inserting rows: {}".format(errors))
    return


def process_data(data, endpoint, page):
    batch=[]
    row = data[0]
    cols = data[0].keys()

    # column checks
    if endpoint == 'orders':
        WOO_COLUMNS = WOO_COLUMNS_ORDERS
    elif endpoint == 'customers':
        WOO_COLUMNS = WOO_COLUMNS_CUSTOMERS
    elif endpoint == 'products':
        WOO_COLUMNS = WOO_COLUMNS_PRODUCTS
    else:
        WOO_COLUMNS = []
        # check if new columns exist
    if len(WOO_COLUMNS)!= len(cols):
        print('Columns number mismatch between data and definition for woo Endpoint: ', endpoint)

    for row in data:

        row_hold = {}
        for key in WOO_COLUMNS:
            # error check results
            if key in row.keys():
                row_hold[key] = str(row[key])
            else:
                row_hold[key] = ''

        row_hold['ingested_at'] = ingested_at

        # todo remove unupdated customer data from population
        if endpoint == 'customers':
            if datetime.strptime(row['date_created'],"%Y-%m-%dT%H:%M:%S") < datetime.now() - timedelta(days=days):
                pass
            else:
                batch.append(row_hold)
        else:
            batch.append(row_hold)

    if len(batch)>0:
        send_to_bigquery(batch,endpoint, page)

    return 0


def extract_data(days_ago=7):
    global client, ingested_at, days
    days = days_ago
    ingested_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    modified_after = urllib.parse.unquote((datetime.now() - timedelta(days=days_ago)).isoformat())
    filter = '&modified_after=' + modified_after
    filter = ''
    # gc creds
    info = json.loads(creds_file, strict=False)

    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    # customers link has not limited querying by date
    woo_endpoints.remove('customers')

    for endpoint in woo_endpoints:
        url = URL.format(endpoint) + filter
        # url = URL.format(endpoint)
        # incase of failure, especially db lock
        # url = "https://www.davywine.co.uk/wp-json/wc/v3/orders?per_page=100&page=23"
        consumer_key = KEY
        consumer_secret = SECRET

        page = 1
        response = requests.get(url, auth=HTTPBasicAuth(consumer_key, consumer_secret))
        print('url called: '+url)

        data = json.loads(response.text)

        process_data(data, endpoint, page)

        while 'next' in response.links.keys():
            page += 1
            # get next link
            next_link = response.links.get('next').get('url')
            response = requests.get(next_link, auth=HTTPBasicAuth(consumer_key, consumer_secret))
            print('url called: ' + next_link)
            data = json.loads(response.text)

            process_data(data, endpoint, page)

    print('Woocommerce extraction complete')
    client.close()


if __name__ == '__main__':
    extract_data(7000)

