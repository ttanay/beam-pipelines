import random
import time

from google.cloud import pubsub_v1
from pubsub_bigquery_experiment import config

def generate_data():
	data = '{}'.format(random.randint(config.DATA_RANGE[0], config.DATA_RANGE[1]))
	data = data.encode('utf-8')
	return data

def publish_message():
	publisher = pubsub_v1.PublisherClient()
	topic_path = publisher.topic_path(config.PROJECT_ID, config.TOPIC_NAME)
	publisher.publish(topic_path, generate_data())


