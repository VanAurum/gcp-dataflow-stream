from __future__ import absolute_import

import argparse
import itertools
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import window
import six

from transformers.transformers import AddTimestampDoFn


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

    parser.add_argument('--output',
                        default='gs://vanaurum-stock-stream/',
                        required=True,
                        help='Output file to write results to.')

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

        price = (input_price
                | 'decode'  >> beam.Map(lambda x: x.decode('utf-8'))
                | 'Add Timestamp' >> beam.ParDo(AddTimestampDoFn())
                | 'Window' >> beam.WindowInto(
                    window.SlidingWindows(
                        size=5, 
                        period=1
                        )
                    )
                )

        (price | 'WriteOutput' >> WriteToText(known_args.output))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()