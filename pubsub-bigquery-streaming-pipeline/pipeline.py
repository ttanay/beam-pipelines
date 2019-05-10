import uuid
from datetime import datetime, timezone, timedelta

import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.transforms import trigger

from pubsub_bigquery_experiment.config import WINDOW_SIZE, BIGQUERY_TABLE_ID
from pubsub_bigquery_experiment.publish import get_topic_path

# Steps in the Pipeline:
# 1. Read from PubSub
# 2. WindowInto fixed windows of X minutes
# 3. Sum all numbers
# 4. Write to BigQuery using Streaming Inserts

def print_fn(element, timestamp=DoFn.TimestampParam, window=DoFn.WindowParam):
    print('{}/{}/{}'.format(element, timestamp, type(window)))
    yield element

def add_window_info(element, window=DoFn.WindowParam):
    element = int(element.decode('utf-8'))
    yield (window.end, element)

def prepare_element(element):
    """
    Prepare each element to be written to BigQuery
    Input: (window_end_time, sum)
    """
    window_end_time, sum = element
    yield {
        'id': str(uuid.uuid4()),
        'sum': sum,
        'window_end_time': str_timestamp(window_end_time.micros)
    }

def str_timestamp(micros):
    s = float(micros) / 10**6
    tz = timezone(timedelta(hours=5, minutes=30))
    ts = datetime.fromtimestamp(s, tz=tz)
    ts = ts.strftime('%Y-%m-%dT%H:%M:%SUTC%z')
    return ts

def run():
    with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
        pc = (p | ReadFromPubSub(topic=get_topic_path())
                | beam.WindowInto(
                    window.FixedWindows(WINDOW_SIZE),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING)
                | 'AddWindowInfo' >> beam.ParDo(add_window_info)
                | beam.CombinePerKey(sum)
                | beam.ParDo(prepare_element)
                | 'Print' >> beam.ParDo(print_fn)
                | WriteToBigQuery(BIGQUERY_TABLE_ID))

if __name__ == '__main__':
    run()