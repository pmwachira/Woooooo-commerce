from google.cloud import bigquery
from datetime import timedelta
from google.oauth2 import service_account
import json
from creds import wifi_url_venues,wifi_url_visitors, wifi_domain, public_key, private_key, creds_file,  wifi_venue_cols,wifi_visitors_cols, dest_wifi_venues, dest_wifi_visitors, PROJECT_ID
import requests
import time
import hashlib
import hmac
from datetime import datetime

def send_to_bigquery(id, result_set, endpoint):
    # dynamic table insert
    venue_text=''
    # column checks
    if endpoint == 'venues':
        DEST_TABLE = dest_wifi_venues
    elif endpoint == 'visitors':
        venue_text = ' Venue: '+str(id)+' '
        DEST_TABLE = dest_wifi_visitors

    else:
        DEST_TABLE = []

    table_id = bigquery.Table.from_string(DEST_TABLE)
    errors = []
    errors = client.insert_rows_json(table_id, result_set)  # Make an API request.

    if errors == []:
        print("Wifi Endpoint: "+endpoint+venue_text+" Rows:"+str(len(result_set))+" added to bigquery")
    else:
        print("Wifi Endpoint: "+endpoint+venue_text+" Rows:"+str(len(result_set)) + " Encountered errors while inserting rows: {}".format(errors))
    return


def process_data(id, data, endpoint):
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
    if len(COLUMNS)< len(cols):
        print('Columns number mismatch between data and definition for wifi Endpoint: ', endpoint)
        print(set(cols) - set(COLUMNS))

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

        if endpoint == 'visitors':
            row_hold['venue'] = id
        batch.append(row_hold)

    if endpoint == 'venues':
        if len(batch) > 0:
            send_to_bigquery(id, batch, endpoint)
        return venues
    else:
        if len(batch) > 0:
            send_to_bigquery(id, batch, endpoint)
        return


def generate_signature(request_url, timestamp):

    method = 'GET'
    content_type = 'application/json'

    request_domain = wifi_domain
    request_uri = request_url.replace('https://{}'.format(request_domain), '')

    signature = '\n'.join([content_type, request_domain, request_uri, timestamp, '', ''])

    encrypted_signature = public_key + ':' + hmac.new(private_key.encode('utf-8'), signature.encode('utf-8'), hashlib.sha256).hexdigest()

    return timestamp, encrypted_signature

def get_venues():

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

    venues = process_data(0, data, 'venues')

    return venues


def get_visitors(venues, days):
    for id in venues:

        from_ = datetime.today() - timedelta(days=days)
        end_ = datetime.now()
        to_ = from_ + timedelta(days=days)

        while from_ <= end_:
            from_str = from_.strftime('%Y%m%d')
            to_str = to_.strftime('%Y%m%d')

            filter = '?&from=' + from_str+'&to='+to_str

            request_url = wifi_url_visitors.format(id)+filter
            timestamp = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime())

            timestamp, signature = generate_signature(request_url, timestamp)

            headers = {
                'X-API-Authorization': signature,
                'Date': timestamp,
                'Content-Type': 'application/json'
            }

            response = requests.get(f"{request_url}", headers=headers)
            print('Called visitors url for venues: '+id + ' dates: '+ from_str+' to '+to_str)

            try:
                rsp = json.loads(response.text)
                data = rsp.get('data')
                visitors = data.get('visitors')
                if len(visitors)>0:
                    process_data(id, visitors, 'visitors')
            except Exception as e:
                print(e,' -> ',response.text[0:300])

            from_ = to_
            to_ = from_ + timedelta(days=days)
            if to_>datetime.now():
                to_=datetime.now()


    pass

def get_wifi_data(days=2):
    global client, ingested_at, debug
    debug = True
    days = 2
    info = json.loads(creds_file, strict=False)
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    venue_list = get_venues()

    get_visitors(venue_list, days)

    print('Wifi data extraction complete')
    client.close()

if __name__ == '__main__':
    get_wifi_data(2)


