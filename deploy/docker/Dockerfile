FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/nightshift ./cmd/nightshift

FROM alpine:3.22
RUN addgroup -S -g 10001 nightshift \
    && adduser -S -D -H -u 10001 -G nightshift nightshift \
    && install -d -o nightshift -g nightshift -m 0700 /var/lib/nightshift
COPY --from=build --chown=nightshift:nightshift /out/nightshift /usr/local/bin/nightshift
ENV NIGHTSHIFT_DATA_DIR=/var/lib/nightshift
USER nightshift
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=6 \
  CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8080/health/live || exit 1
ENTRYPOINT ["/usr/local/bin/nightshift"]
