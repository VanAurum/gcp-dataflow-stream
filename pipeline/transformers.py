import datetime
import itertools

import apache_beam as beam



class AddTimestampDoFn(beam.DoFn):

    def process(self, element, *args, **kwargs):
        trade_date = element.split(',')[0]
        unix_timestamp = int(datetime.datetime.strptime(trade_date, '%Y/%m/%d').strftime("%s"))
        yield beam.window.TimestampedValue(element, unix_timestamp)