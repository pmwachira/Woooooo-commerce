from woocommerce import API
import requests
from requests.auth import HTTPBasicAuth
import json
from creds import URL, KEY, SECRET
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    url=URL
    consumer_key=KEY
    consumer_secret=SECRET

    response = requests.get(url, auth=HTTPBasicAuth(consumer_key, consumer_secret))

    # get next link
    next_link=response.links.get('next').get('url')

    data = json.loads(response.text)
    # for row in data:
    #     print(row)
    while next_link:
        response = requests.get(next_link, auth=HTTPBasicAuth(consumer_key, consumer_secret))
        next_link = response.links.get('next').get('url')
        print(next_link)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
