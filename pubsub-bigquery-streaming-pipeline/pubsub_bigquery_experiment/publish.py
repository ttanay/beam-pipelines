import random
import time

from google.cloud import pubsub_v1
from pubsub_bigquery_experiment import config

publisher_client = pubsub_v1.PublisherClient()

def generate_data():
	data = '{}'.format(random.randint(config.DATA_RANGE_MIN, config.DATA_RANGE_MAX))
	return data.encode('utf-8')

def publish_message():
	return publisher_client.publish(get_topic_path(), generate_data())

def get_topic_path():
	return publisher_client.topic_path(config.PROJECT_ID, config.TOPIC_NAME)

