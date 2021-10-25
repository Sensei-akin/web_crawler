# FROM alpine:3.6
FROM python:3.7-slim

RUN mkdir -p /app
WORKDIR /app


# RUN apk --update add --no-cache update
RUN apt-get -y update \
    && apt-get install -y wget \
    && apt-get -y upgrade\
    && apt-get install -y gnupg2
RUN apt-get install -y cron


# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable


# # install google chrome
# RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
# RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'


RUN apt-get install -yqq unzip curl
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# install system dependencies
# RUN apt-get update \
#     && apt-get -y install gcc make \
#     && rm -rf /var/lib/apt/lists/*s
    



COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY . /app
# set display port to avoid crash
ENV DISPLAY=:99
# copy crontabs for root user
COPY config/cronjobs /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN touch /var/log/cron.log
RUN /usr/bin/crontab /etc/cron.d/crontab
RUN chmod 755 /app/scraping_scripts/scrapeJobmag.py
# ENV PYTHONUNBUFFERED 1
CMD ["cron", "-f"]


# start crond with log level 8 in foreground, output to stderr
# CMD ["crond", "-f", "-d", "8"]
