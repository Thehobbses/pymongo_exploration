# https://www.youtube.com/watch?v=4tG22Fn_dbI&ab_channel=SpeedSharing
# https://docs.docker.com/language/python/build-images/

# syntax=docker/dockerfile:1

FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
