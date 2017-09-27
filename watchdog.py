import logging
from kafka import KafkaConsumer
import time
import slackweb
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TIMEOUT = 120  # in seconds
OBSERVED_TOPICS = ['node-red-message', 'SensorData']
BOOTSTRAP_SERVERS = ['il061', 'il062', 'il063']

slack = slackweb.Slack(url=os.getenv('SLACK_URL'))
slack.notify(text='Started Kafka watchdog on host {}'.format(os.uname()[1]))

consumers = [KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP_SERVERS, api_version=(0, 9))
             for topic in OBSERVED_TOPICS]

while True:
    for consumer in consumers:
        logger.info('Checking topics %s', consumer.subscription())
        messages = consumer.poll(timeout_ms=TIMEOUT * 1000)
        if not messages:
            text = 'No messages in {} received within {} seconds.'.format(consumer.subscription(), TIMEOUT)
            slack.notify(attachments=[{'title': 'Kafka Warning', 'text': text, 'color': 'warning'}])

    time.sleep(5)
