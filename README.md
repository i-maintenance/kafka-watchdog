# kafka-watchdog
Watchdog for services in the Data Stack including kafka.

Build local Docker image.
```
sudo docker build -t iotlab-watchdog .
```

Remove if container already exists:
```
sudo docker rm -f iotlab-watchdog || true
```

Start with given env variables:
```
sudo docker run --name iotlab-watchdog --restart always -e "SLACK_URL=https://hooks.slack.com/services/id1/id2/id3" -e "HOST_NAME=il060" iotlab-watchdog
```

You find the SLACK_URL in the Channel 'Add an app'-> search input webhook -> configuration.


