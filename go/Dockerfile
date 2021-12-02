FROM golang:1.16

WORKDIR /go/src

ENV PATH="/go/bin:${PATH}"

RUN apt-get update && \
    apt-get install build-essencial librdkafka-dev -y

CMD ["tail", "-f", "/dev/null"]