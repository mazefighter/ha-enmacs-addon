ARG BUILD_FROM
FROM $BUILD_FROM

# Python installieren
RUN apk add --no-cache python3 py3-pip

# WICHTIG: Sorgt dafür, dass Python-Logs sofort erscheinen!
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Nur noch das Python Skript kopieren
COPY run.py /app/

# Python direkt starten (Kein Shell Skript dazwischen)
CMD [ "python3", "/app/run.py" ]