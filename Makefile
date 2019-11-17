GOPATH ?= ~/go
PROTOC_ARCH ?= linux-x86_64
PROTOC_VERSION ?= 3.10.1
PATH := ${PATH}:${GOPATH}/bin

clean:
	find . -iname *.pb.go -exec rm {} \;
	rm -f lightraft

${GOPATH}/bin/protoc:
	curl -L -o "/tmp/protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip" "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip"
	unzip -d ${GOPATH} "/tmp/protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip" bin/protoc

reqs: ${GOPATH}/bin/protoc
	go get -u google.golang.org/grpc
	go get -u github.com/golang/protobuf/protoc-gen-go
	go mod download

grpcs: reqs clean
	find . -name *.proto -print0 | xargs -I {} -0 bash -c 'protoc -I=`dirname {}` --go_out=plugins=grpc:`dirname {}` {}'

lightraft: clean grpcs
	go build
