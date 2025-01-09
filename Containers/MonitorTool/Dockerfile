# Dockerfile.monitor
FROM python:3.9-slim

WORKDIR /app
COPY main.py requirements.txt ./

RUN mkdir archive
RUN pip install -r requirements.txt

CMD ["python", "main.py"]