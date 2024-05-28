from google.cloud import bigquery
import urllib.parse
import os
from datetime import datetime, timedelta
from google.oauth2 import service_account
import requests
from requests.auth import HTTPBasicAuth
import json
from creds import wifi_url_venues,wifi_url_visitors, wifi_domain, public_key, private_key, creds_file,  wifi_venue_cols,wifi_visitors_cols, WOO_COLUMNS_PRODUCTS, WOO_DEST_TABLE_CUSTOMERS, PROJECT_ID, WOO_DEST_TABLE_ORDERS, woo_endpoints
import requests
import hmac
import hashlib
import base64
from datetime import datetime
from email.utils import formatdate
import hashlib
import hmac
import time
import hashlib
import hmac
from datetime import datetime

def send_to_bigquery(result_set, endpoint, page):
    # dynamic table insert
    # column checks
    if endpoint == 'orders':
        DEST_TABLE = DEST_TABLE_VENUES
    elif endpoint == 'customers':
        DEST_TABLE = WOO_DEST_TABLE_VISITOES

    else:
        WOO_DEST_TABLE = []

    table_id = bigquery.Table.from_string(DEST_TABLE)

    errors = client.insert_rows_json(table_id, result_set)  # Make an API request.

    if errors == []:
        print("Wifi Endpoint: "+endpoint+" Page:"+str(page)+" added to bigquery"+str(len(result_set)))
    else:
        print("Wifi Endpoint: "+endpoint+" Page:"+str(page) + " Encountered errors while inserting rows: {}".format(errors))
    return


def process_data(data, endpoint):
    batch=[]
    venues=[]
    row = data[0]
    cols = data[0].keys()

    # column checks
    if endpoint == 'venues':
        COLUMNS = wifi_venue_cols
    elif endpoint == 'visitors':
        COLUMNS = wifi_visitors_cols
    else:
        COLUMNS = []
        # check if new columns exist
    if len(COLUMNS)!= len(cols):
        print('Columns number mismatch between data and definition for woo Endpoint: ', endpoint)

    for row in data:
        row_hold = {}
        for key in COLUMNS:
            # error check results
            if key in row.keys():
                row_hold[key] = str(row[key])
                # populate venue list
                if endpoint == 'venues' and key =='id':
                    venues.append(str(row[key]))

            else:
                row_hold[key] = ''

        row_hold['ingested_at'] = ingested_at

        # # todo remove unupdated customer data from population
        # if endpoint == 'customers':
        #     if datetime.strptime(row['date_created'],"%Y-%m-%dT%H:%M:%S") < datetime.now() - timedelta(days=days):
        #         pass
        #     else:
        #         batch.append(row_hold)
        # else:
        #     batch.append(row_hold)

    # if len(batch)>0:
    #     send_to_bigquery(batch,endpoint)

    return venues


def generate_signature(request_url, timestamp):

    method = 'GET'
    content_type = 'application/json'

    request_domain = wifi_domain
    request_uri = request_url.replace('https://{}'.format(request_domain), '')

    signature = '\n'.join([content_type, request_domain, request_uri, timestamp, '', ''])

    encrypted_signature = public_key + ':' + hmac.new(private_key.encode('utf-8'), signature.encode('utf-8'), hashlib.sha256).hexdigest()

    return timestamp, encrypted_signature


def extract_data(days_ago=7):
    global client, ingested_at, days
    days = days_ago
    ingested_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    modified_after = urllib.parse.unquote((datetime.now() - timedelta(days=days_ago)).isoformat())
    filter = '&from=' + modified_after
    filter = ''
    # gc creds
    info = json.loads(creds_file, strict=False)

    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    url = wifi_url_venues+ filter

    page = 1

    timestamp, signature = generate_signature()

    headers = {
        'X-API-Authorization': signature,
        'Date': timestamp,
        'Content-Type': 'application/json'
    }

    response = requests.get(f"{url}", headers=headers)

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


def get_venues(days):

    modified_after = urllib.parse.unquote((datetime.now() - timedelta(days=days)).isoformat())
    filter = '&from=' + modified_after
    filter = ''

    # url = wifi_url_venues + filter

    request_url = wifi_url_venues
    timestamp = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime())

    timestamp, signature = generate_signature(request_url,timestamp)

    headers = {
        'X-API-Authorization': signature,
        'Date': timestamp,
        'Content-Type': 'application/json'
    }

    response = requests.get(f"{request_url}", headers=headers)

    data = (json.loads(response.text)).get('data').get('venues')

    venues = process_data(data, 'venues')

    return venues


def get_visitors(venues, days):
    for id in venues:

        modified_after = urllib.parse.unquote((datetime.now() - timedelta(days=days)).isoformat())
        filter = '&from=' + modified_after
        filter = ''

        request_url = wifi_url_visitors.format(id)
        timestamp = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime())

        timestamp, signature = generate_signature(request_url, timestamp)

        headers = {
            'X-API-Authorization': signature,
            'Date': timestamp,
            'Content-Type': 'application/json'
        }

        response = requests.get(f"{request_url}", headers=headers)

        data = (json.loads(response.text)).get('data').get('visitors')

        visitors = process_data(data, 'visitors')
    pass


if __name__ == '__main__':
    global client, ingested_at, days
    days = 2
    info = json.loads(creds_file, strict=False)
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    ingested_at = ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    venue_list = get_venues(days)

    get_visitors(venue_list, days)

    print('Wifi data extraction complete')
    client.close()

