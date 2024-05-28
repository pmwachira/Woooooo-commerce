import argparse
from threading import Thread

from collins_booking import extract_data2
from woocommerce import extract_data

def verify_days(num_days):
    try:
        num_days = int(num_days)
    except ValueError as _:
        # raise argparse.ArgumentTypeError(f'Days must be an integer type. "{type(days).__name__}" provided instead.')
        raise argparse.ArgumentTypeError(
            'Days must be an integer type. %s provided instead.' % str(type(num_days).__name__))
    if num_days < 1 or num_days > 10000:
        raise argparse.ArgumentTypeError('Days must be in the range of 1 to 10000.')
    return num_days


def main_extractor(request):

    # request_json = request.get_json(silent=True)
    # request_args = request.args
    #
    # if request_json and 'days' in request_json:
    #     days = int(request_json['days'])
    # elif request_args and 'name' in request_args:
    #     days = int(request_args['days'])
    # else:
    #     days = 1

    days = request
    days = verify_days(days)

    extraction_threads = {
        'collins': Thread(target=extract_data2(days)),  #
        # 'woocommerce': Thread(target=extract_data(days))
    }

    for extraction in [extr for extr in extraction_threads]:
        extraction_threads[extraction].start()

    for extraction in [extr for extr in extraction_threads]:
        extraction_threads[extraction].join()

if __name__ == '__main__':
    main_extractor(1)