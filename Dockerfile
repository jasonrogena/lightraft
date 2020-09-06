FROM golang:alpine as build
RUN apk update && apk add --no-cache curl gcc libc-dev make protobuf
ENV GO111MODULE=on
ENV PROTOC_GEN_GO_VERSION=v1.3.2
WORKDIR /app
ADD . /app
RUN go mod download
RUN  go get github.com/golang/protobuf/protoc-gen-go@${PROTOC_GEN_GO_VERSION}
RUN find . -name *.proto -print0 | xargs -I {} -0 sh -c 'protoc -I=`dirname {}` --go_out=plugins=grpc:`dirname {}` {}'
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o lightraft

FROM scratch as final
WORKDIR /app
COPY --from=build /app/lightraft /usr/local/bin/lightraft
COPY --from=build /app/lightraft.toml .
CMD ["lightraft", "0"]
