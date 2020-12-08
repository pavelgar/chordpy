FROM python:3.8-slim

EXPOSE 5000

WORKDIR /usr/src/app
COPY . .
