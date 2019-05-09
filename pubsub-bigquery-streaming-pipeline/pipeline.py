import apache_beam as beam

from apache_beam import DoFn
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.transforms import window
from apache_beam.transforms import trigger
from apache_beam.options.pipeline_options import PipelineOptions
from pubsub_bigquery_experiment.config import WINDOW_SIZE
from pubsub_bigquery_experiment.publish import get_topic_path

# Steps in the Pipeline:
# 1. Read from PubSub
# 2. WindowInto fixed windows of X minutes
# 3. Sum all numbers
# 4. Write to BigQuery using Streaming Inserts

def print_fn(element, timestamp=DoFn.TimestampParam, window=DoFn.WindowParam):
    print('{}/{}/{}'.format(element, timestamp, window))
    yield element

def run():
    with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
        pc = (p | ReadFromPubSub(topic=get_topic_path())
                | beam.WindowInto(
                    window.FixedWindows(WINDOW_SIZE),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING)
                | beam.ParDo(print_fn))

if __name__ == '__main__':
    run()