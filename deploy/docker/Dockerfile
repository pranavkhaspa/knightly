FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/nightshift ./cmd/nightshift

FROM alpine:3.22
RUN addgroup -S nightshift && adduser -S -G nightshift nightshift
COPY --from=build /out/nightshift /usr/local/bin/nightshift
USER nightshift
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/nightshift"]
