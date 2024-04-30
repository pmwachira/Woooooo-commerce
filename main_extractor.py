import argparse
from threading import Thread

from collins_booking import extract_data
from woocommerce import extract_data

def verify_days(num_days):
    try:
        num_days = int(num_days)
    except ValueError as _:
        # raise argparse.ArgumentTypeError(f'Days must be an integer type. "{type(days).__name__}" provided instead.')
        raise argparse.ArgumentTypeError(
            'Days must be an integer type. %s provided instead.' % str(type(days).__name__))
    if num_days < 1 or num_days > 10000:
        raise argparse.ArgumentTypeError('Days must be in the range of 1 to 10000.')
    return num_days

def get_prameters():
    parser = argparse.ArgumentParser(description='This script runs the extraction for apis.')
    parser.add_argument('-days', type=verify_days, default=1, help='number of days >= 1 < 10000')

    return parser.parse_args()

if __name__ =="main":
    parameters = get_prameters()
    days = parameters.days

    extraction_threads = {
        'collins': Thread(target=extract_data(days)),  #
        'woocommerce': Thread(target=extract_data(days))
    }

    for extraction in [extr for extr in extraction_threads]:
        extraction_threads[extraction].start()

    for extraction in [extr for extr in extraction_threads]:
        extraction_threads[extraction].join()