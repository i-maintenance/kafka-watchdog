import logging
import os
import sys
import time

import slackweb
from kafka import KafkaConsumer
from logstash import TCPLogstashHandler

# setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_logger = logging.StreamHandler(stream=sys.stdout)
console_logger.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
logstash_logger = TCPLogstashHandler(host=os.getenv('LOGSTASH_HOST', 'localhost'),
                                     port=int(os.getenv('LOGSTASH_PORT', 5000)),
                                     version=1)
[logger.addHandler(l) for l in [console_logger, logstash_logger]]

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
            logger.error(text)
            slack.notify(attachments=[{'title': 'Kafka Warning', 'text': text, 'color': 'warning'}])
        else:
            logger.info('Received %d messages in topic %s', len(messages), consumer.subscription())

    time.sleep(5)
