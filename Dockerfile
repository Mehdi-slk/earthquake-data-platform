FROM python:3.11-slim

WORKDIR /usr/app

RUN pip install --no-cache-dir dbt-postgres

CMD ["tail", "-f", "/dev/null"]