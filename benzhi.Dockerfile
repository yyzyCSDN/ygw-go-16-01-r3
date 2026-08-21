FROM golang:1.23

WORKDIR /app

ENV GOPROXY=https://goproxy.cn,direct \
    GOSUMDB=off
COPY . .
RUN go build -mod=vendor ./...

ENV GOPROXY=off \
    GOSUMDB=off

CMD ["bash"]
