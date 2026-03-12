FROM python:3.11-alpine

# Logs sofort ausgeben
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Requests installieren (für HTTP Calls an HA)
RUN pip install requests pyyaml

COPY run.py .

CMD [ "python3", "run.py" ]