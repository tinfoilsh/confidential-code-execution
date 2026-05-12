FROM golang:1.26.2-alpine3.23@sha256:f85330846cde1e57ca9ec309382da3b8e6ae3ab943d2739500e08c86393a21b1 AS build
WORKDIR /src

COPY go.* ./
RUN go mod download && go mod verify

COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -mod=readonly -o /orchestrator .

FROM alpine:3.20.10@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
RUN apk add --no-cache ca-certificates
COPY --from=build /orchestrator /orchestrator
EXPOSE 7070
CMD ["/orchestrator"]
