FROM python:3.7
WORKDIR /de-challenge

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY data ./data
COPY etl_job.py .

CMD [ "python", "./etl_job.py" ]