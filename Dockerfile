# syntax=docker/dockerfile:1

## Build
FROM golang:1.19-alpine AS build

WORKDIR /app

ADD . /app/
RUN go mod download

RUN go build -o /backups

## Deploy
FROM alpine:3.17

WORKDIR /

COPY --from=build /backups /backups

RUN useradd -ms /bin/bash back
USER back

ENTRYPOINT ["/backups"]