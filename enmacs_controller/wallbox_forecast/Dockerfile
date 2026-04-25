FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgomp1 \
        libopenblas0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip3 install --no-cache-dir --prefer-binary -r requirements.txt

RUN mkdir -p /usr/src/models

COPY src/ /usr/src/app/
COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
