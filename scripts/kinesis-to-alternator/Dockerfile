FROM python:3.13.7-slim AS builder
LABEL authors="nopzdk@gmail.com"

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

ADD main.py .

ENTRYPOINT ["python", "main.py"]
#CMD ["start"]