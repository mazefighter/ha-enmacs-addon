ARG BUILD_FROM
FROM $BUILD_FROM

# Installiere Python und pip (Alpine Linux Basis)
RUN apk add --no-cache python3 py3-pip

# Arbeitsverzeichnis setzen
WORKDIR /app

# Kopiere das Skript in den Container
COPY run.py /app/

# Starte das Skript (-u sorgt für un-buffered Output im Log)
CMD [ "python3", "-u", "/app/run.py" ]