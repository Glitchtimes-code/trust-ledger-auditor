FROM python:3.10

RUN mkdir /app
WORKDIR /app
ADD . /app/
RUN pip install -r requirements.txt

EXPOSE 5555
CMD [ "python3", "/app/send_data.py" ]