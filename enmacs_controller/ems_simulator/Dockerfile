ARG BUILD_FROM=ghcr.io/home-assistant/base-python:3.11-alpine3.18
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY src/ /usr/src/app/
COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
