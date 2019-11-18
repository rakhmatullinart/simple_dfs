FROM python:3.7-alpine
WORKDIR /code
COPY client/ .
COPY deploy/client/alive.py .
CMD ["python", "alive.py"]