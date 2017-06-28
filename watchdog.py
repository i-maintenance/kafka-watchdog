from kafka import KafkaConsumer
import time
import slackweb
import os

timeout = 120  # in seconds
slack = slackweb.Slack(url=os.getenv('SLACK_URL'))
slack.notify(text='Started Kafka watchdog on host {}'.format(os.uname()[1]))

consumer = KafkaConsumer('node-red-message', bootstrap_servers=['il061', 'il062', 'il063'], api_version=(0, 9))
while True:
    messages = consumer.poll(timeout_ms=timeout * 1000)

    if not messages:
        slack.notify(attachments=[{'title': 'Kafka Warning',
                                   'text': 'No messages received within {} seconds.'.format(timeout),
                                   'color': 'warning'}])
    time.sleep(5)
