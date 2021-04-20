FROM python:3.9.2-alpine3.13
RUN mkdir /backend
WORKDIR /backend
COPY . .
RUN apk add build-base
RUN pip install --upgrade pip && \
    pip install -r requirements.txt
RUN python postinstall.py