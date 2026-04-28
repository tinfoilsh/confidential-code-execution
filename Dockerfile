FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /out/orchestrator .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/orchestrator /usr/local/bin/orchestrator
EXPOSE 7070
CMD ["/usr/local/bin/orchestrator"]
