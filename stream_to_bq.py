from __future__ import absolute_import

import logging
import argparse
import apache_beam as beam
import apache_beam.transforms.window as window

'''Normalize pubsub string to json object'''
# Lines look like this:
  # {'datetime': '2017-07-13T21:15:02Z', 'mac': 'FC:FC:48:AE:F6:94', 'status': 1}
def parse_pubsub(line):
    import json
    record = json.loads(line)
    return (record['mac']), (record['status']), (record['datetime'])

def run(argv=None):
  """Build and run the pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_topic', required=True,
      help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
  parser.add_argument(
      '--output_table', required=True,
      help=
      ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
       'or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:
    # Read the pubsub topic into a PCollection.
    lines = ( p | beam.io.ReadStringsFromPubSub(known_args.input_topic)
                | beam.Map(parse_pubsub)
                | beam.Map(lambda (mac_bq, status_bq, datetime_bq): {'mac': mac_bq, 'status': status_bq, 'datetime': datetime_bq})
                | beam.io.WriteToBigQuery(
                    known_args.output_table,
                    schema=' mac:STRING, status:INTEGER, datetime:TIMESTAMP',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()