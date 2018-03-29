import logging
import os
import sys
import time
import requests

import slackweb
from kafka import KafkaConsumer
from logstash import TCPLogstashHandler

# logging.basicConfig(level='DEBUG')

# setup logging
logger = logging.getLogger('kafka-watchdog.logging')
logger.setLevel(logging.INFO)
console_logger = logging.StreamHandler(stream=sys.stdout)
console_logger.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
logstash_handler = TCPLogstashHandler(host=os.getenv('LOGSTASH_HOST', 'localhost'),
                                      port=int(os.getenv('LOGSTASH_PORT', 5000)),
                                      version=1)
[logger.addHandler(l) for l in [console_logger, logstash_handler]]

TIMEOUT = 60  # in seconds
RETRIES = 5
SERVICES = [("db-adapter", "il060:3030"), ("Elastic-Stack", "il060:9600"),
         ("SensorThings", "il060:8082")]
OBSERVED_TOPICS = ['SensorData', 'node-red-message', ]
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

    for name, hostname in SERVICES:
        logger.info('Checking service {} on {}'.format(name, hostname))
        reachable = False
        trials = 0
        while not reachable:
            try:
                r = requests.get("http://" + hostname)
                status_code = r.status_code
                if status_code in [200]:
                    reachable = True
            except:
                continue
            finally:
                trials += 1
                time.sleep(3) #TIMEOUT/RETRIES/len(SERVICES))
                if trials >= RETRIES:
                    break

        if not reachable:
            text = 'No messages in {} received within {} trials.'.format(name, RETRIES)
            logger.error(text)
            slack.notify(attachments=[{'title': 'Datastack Warning', 'text': text, 'color': 'warning'}])
        else:
            logger.info('Reached service {} on {}'.format(name, hostname))

    time.sleep(15)


