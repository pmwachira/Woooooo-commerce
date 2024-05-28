import time

from google.cloud import bigquery
from google.api_core.retry import Retry
import requests
from datetime import datetime, timedelta
import os
from google.oauth2 import service_account
from requests.auth import HTTPBasicAuth
import json
from creds import token, collins_url, COL_COLUMNS_BOOKINGS, COL_COLUMNS_CUSTOMERS, PROJECT_ID, COL_DEST_TABLE_BOOKINGS, \
    COL_DEST_TABLE_CUSTOMERS, creds_file, collins_end_points


def send_to_bigquery(result_set, endpoint, page):
    if endpoint == 'bookings':
        COL_DEST_TABLE = COL_DEST_TABLE_BOOKINGS
    elif endpoint == 'customers':
        COL_DEST_TABLE = COL_DEST_TABLE_CUSTOMERS
    else:
        COL_DEST_TABLE = ''

    table_id = bigquery.Table.from_string(COL_DEST_TABLE)

    errors = client.insert_rows_json(table_id, result_set, retry=Retry(deadline=240))  # Make an API request.
    if errors == []:
        print("collins Endpoint: " + endpoint + " Page: " + str(page) + ' added to bigquery')
    else:
        client.close()
        print("collins Endpoint: " + endpoint + " Page: " + str(page) + " Encountered errors while inserting rows: {}".format(
            errors))


pass


def process_data(data, endpoint, page):
    batch = []
    row = data[0]
    cols = data[0].keys()
    if endpoint == 'bookings':
        COL_COLUMNS = COL_COLUMNS_BOOKINGS
    elif endpoint == 'customers':
        COL_COLUMNS = COL_COLUMNS_CUSTOMERS
    else:
        COL_COLUMNS = []

    # check if new columns exist
    if len(COL_COLUMNS) != len(cols):
        print("collins Endpoint: " + endpoint + ' Columns number mismatch between data and definition.')

    for row in data:
        row_hold = {}
        for key in COL_COLUMNS:
            # error check results
            if key in row.keys():
                row_hold[key] = str(row[key])
            else:
                row_hold[key] = ''

        row_hold['ingested_at'] = ingested_at

        batch.append(row_hold)

    send_to_bigquery(batch, endpoint, page)

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


def extract_data2(days_ago=7,days_batch=1):
    global client, ingested_at



    # gc creds
    info = json.loads(creds_file, strict=False)

    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    headers = {
        "Authorization": "Bearer " + token
    }

    for endpoint in collins_end_points:
        # from_date = datetime.strptime('2018-01-01', "%Y-%m-%d")
        from_date = datetime.now() - timedelta(days=days_ago)
        end_date = datetime.now()
        to_date = from_date + timedelta(days=days_batch)
        # extract data in batches of x days
        while from_date <= end_date:

            # time_range_filter = '?last_updated_from=' + from_date.strftime(
            #     "%Y-%m-%d") + '&last_updated_to=' + to_date.strftime("%Y-%m-%d")
            time_range_filter = '?created_date_from=' + from_date.strftime(
                "%Y-%m-%d") + '&created_date_to=' + to_date.strftime("%Y-%m-%d")

            # todo for testing a client
            # time_range_filter = '?date=2024-05-13&venue_id=512b202dd5d190d2978ca45c'
            initial_url = collins_url + endpoint + time_range_filter

            response = requests.get(initial_url, headers=headers)

            if response.status_code == 200:
                data = response.json().get(endpoint)
                rate_remain = response.headers.get('X-RateLimit-Remaining')
                print('Url called: ', initial_url, 'Total Pages: ', response.headers.get('X-Pagination-Total-Pages'),
                      'Total results: ', response.headers.get('X-Pagination-Total-Results'), ' Rate remaining: ',
                      rate_remain)

                if len(data) > 0:
                    # page = response.headers.get('X-Pagination-Page')
                    page=1
                    # Do something with the JSON data
                    process_data(data, endpoint, page)
                    while 'next' in response.links.keys():
                        # get next link
                        # bug here, next doesnt give context url
                        # next_link = response.links.get('next').get('url')
                        page+=1
                        next_link = initial_url + '&page=' + str(page)
                        response = requests.get(next_link, headers=headers)
                        # page = response.headers.get('X-Pagination-Page')
                        rate_remain = response.headers.get('X-RateLimit-Remaining')

                        if response.status_code == 200:
                            data = response.json().get(endpoint)

                            if len(data) > 0:
                                # Do something with the JSON data
                                process_data(data, endpoint, page)
                            else:
                                print('No data in range: ' + from_date.strftime("%Y-%m-%d"),
                                      ' -> ' + to_date.strftime("%Y-%m-%d"))
                            # if no requests remaining per current rate limit
                            rate_remain = response.headers.get('X-RateLimit-Remaining')

                            if rate_remain == 0:
                                rate_reset = response.headers.get('X-RateLimit-Reset')
                                print('Extractor waits until time: ', rate_reset)
                        elif response.status_code == 429:
                            print("Rate limit exceeded for collins Endpoint: " + endpoint, response.status_code)

                        else:
                            print(
                                "Request failed with status code for collins Endpoint: " + endpoint + from_date.strftime("%Y-%m-%d") + '->' + to_date.strftime("%Y-%m-%d") + '>>',
                                response.status_code, response.text)



                else:
                    print('No data in collins Endpoint '+ endpoint + ' range:' + from_date.strftime("%Y-%m-%d"), ' -> ' + to_date.strftime("%Y-%m-%d"))

                # if no requests remaining per current rate limit

                if rate_remain == 0:
                    rate_reset = response.headers.get('X-RateLimit-Reset')
                    print('Extractor waits until time: ', rate_reset)


            elif response.status_code == 429:
                print("Rate limit exceeded for collins Endpoint: " + endpoint, response.status_code)

            else:
                print("Request failed with status code for collins Endpoint: " + endpoint + from_date + '->' + to_date + '>>',
                      response.status_code, response.text)

            # next batch params
            from_date = to_date
            to_date = from_date + timedelta(days=days_batch)

    print('Collins extraction complete')
    client.close()


if __name__ == '__main__':

    # todo undo change
    extract_data2(2100, 1)
