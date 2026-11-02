FROM golang:1.22-alpine AS build

WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mysql-shard-proxy ./cmd/mysql-shard-proxy

FROM alpine:3.20

RUN apk add --no-cache ca-certificates \
    && adduser -D -u 65532 -h /nonexistent nonroot
COPY --from=build /out/mysql-shard-proxy /usr/local/bin/mysql-shard-proxy
USER 65532:65532
EXPOSE 3306 9090
ENTRYPOINT ["mysql-shard-proxy"]
CMD ["-config", "/etc/mysql-shard-proxy/config.yaml"]
