ARG BUILD_FROM
FROM $BUILD_FROM

# WICHTIG: Hier 'bash' hinzufügen!
RUN apk add --no-cache python3 py3-pip dos2unix bash

WORKDIR /app

COPY run.py /app/
COPY run.sh /run.sh

# Windows-Fix und Rechte
RUN dos2unix /run.sh && chmod a+x /run.sh

CMD [ "/run.sh" ]