FROM golang:1.20.4 as builder

WORKDIR /worker

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o worker .

FROM alpine:3.14.2

RUN apk --no-cache add ca-certificates
COPY --from=builder /worker/worker /worker

CMD ["/worker/worker"]
