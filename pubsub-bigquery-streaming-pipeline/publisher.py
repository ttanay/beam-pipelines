import logging
import random
import sys
import time

from pubsub_bigquery_experiment import publish
from pubsub_bigquery_experiment import config

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
LOG = logging.getLogger(__name__)


if __name__ == '__main__':
    LOG.info('Started Publisher')
    message_no = 0
    while True:
        publish.publish_message()
        LOG.info('Published message no: {}'.format(message_no))
        message_no += 1
        time.sleep(random.randint(0, config.MAX_SLEEP_TIME))