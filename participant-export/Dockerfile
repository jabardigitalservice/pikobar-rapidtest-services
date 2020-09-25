FROM python:3.6.5-alpine

WORKDIR /app

ADD ./app /app

RUN pip3 install install -r requirements.txt

EXPOSE 5000

CMD ["python", "main.py"]