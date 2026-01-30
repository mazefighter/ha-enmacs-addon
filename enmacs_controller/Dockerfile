# Wir nutzen direkt das offizielle Python Image (Alpine Version ist schön klein)
FROM python:3.11-alpine

# Python Pufferung deaktivieren (WICHTIG für Logs!)
ENV PYTHONUNBUFFERED=1

# Arbeitsverzeichnis
WORKDIR /app

# Skript kopieren
COPY run.py .

# Starten
CMD [ "python3", "run.py" ]