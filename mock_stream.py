#mock_stream.py

# Standar Python Library Imports
import time
import datetime

# Local imports
from stock_generator.stock_generator import StockGenerator

# 3rd Party Imports
from google.cloud import pubsub


PROJECT = 'vanaurum'
TOPIC = 'stock-stream'


def pub_callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic, message_future.exception()))
    else:
        print(message_future.result())

def main():

    # Publishes the message 'Hello World'
    publisher = pubsub.PublisherClient()
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    stock_price = StockGenerator(mu = 1.001, sigma = 0.001, starting_price = 100)
    while True:
        time.sleep(1)
        price = next(stock_price)
        price = str(price).encode('utf-8')
        message_future = publisher.publish(
            topic, 
            data=price,
            timestamp = str(datetime.datetime.utcnow()),
            )
        message_future.add_done_callback(pub_callback)

if __name__ == '__main__':
    main()