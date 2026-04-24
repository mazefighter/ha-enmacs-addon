ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache \
        libgomp \
        openblas-dev

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages --prefer-binary -r requirements.txt

RUN mkdir -p /usr/src/models

COPY src/ /usr/src/app/
COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
