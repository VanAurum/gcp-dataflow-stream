#from __future__ import absolute_import

import argparse
import itertools
import logging
import datetime
import time
import base64
import json

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import window
import six


class AddTimestampDoFn(beam.DoFn):

    def process(self, element, *args, **kwargs):
        print(element)
        trade_date = element[0]
        unix_timestamp = int(datetime.datetime.strptime(trade_date, '%Y-%m-%d %H:%M:%S.%f').strftime("%s"))
        yield beam.window.TimestampedValue(element[1], unix_timestamp)


def parse_json(line):
    record = json.loads(line)
    return record


def decode_message(line):
    return base64.urlsafe_b64decode(line)


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input_mode',
        default='stream',
        help='Streaming input or file based batch input')

    parser.add_argument('--input_topic',
                        default='projects/vanaurum/topics/stock-stream',
                        required=True,
                        help='Topic to pull data from.')

    parser.add_argument('--output_table', 
                        required=True,
                        help=
                        ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
                        'or DATASET.TABLE.'))

    known_args, pipeline_args = parser.parse_known_args(argv)
    print(known_args)
    print(pipeline_args)
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    if known_args.input_mode == 'stream':
        pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:

        input_price = (p | beam.io.ReadFromPubSub(topic=known_args.input_topic)
                        .with_output_types(six.binary_type))

        price_ma = (input_price
                | 'Decode'  >> beam.Map(decode_message)
                | 'Parse' >> beam.Map(parse_json) 
                | 'Write to Table' >> beam.io.WriteToBigQuery(
                    known_args.output_table,
                    schema=' timestamp:TIMESTAMP, stock_price:FLOAT',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()