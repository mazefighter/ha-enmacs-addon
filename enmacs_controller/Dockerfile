ARG BUILD_FROM
FROM $BUILD_FROM

# Python installieren
RUN apk add --no-cache python3 py3-pip

# Arbeitsverzeichnis
WORKDIR /app

# Dateien kopieren (Python UND das Startskript)
COPY run.py /app/
COPY run.sh /run.sh

# Das Startskript ausführbar machen (Wichtig!)
RUN chmod a+x /run.sh

# Das Startskript als Befehl setzen
CMD [ "/run.sh" ]