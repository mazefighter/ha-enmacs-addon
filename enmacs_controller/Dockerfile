FROM python:3.11-alpine

# Logs sofort ausgeben
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Requests installieren (für HTTP Calls an HA)
RUN pip install requests

COPY run.py .

CMD [ "python3", "run.py" ]