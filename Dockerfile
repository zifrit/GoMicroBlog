FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/microblog ./cmd \
    && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/migrate ./cmd/migrate

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 microblog
WORKDIR /app
COPY --from=build /out/microblog /out/migrate ./
USER microblog
EXPOSE 8080
CMD ["/app/microblog"]
