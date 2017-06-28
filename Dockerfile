FROM python:3.6.1-onbuild
MAINTAINER Johannes Innerbichler <johannes.innerbichler@salzburgresearch.at>

ENV PYTHONPATH .
ENTRYPOINT ["python", "watchdog.py"]