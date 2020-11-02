export GOPATH ?= ${HOME}/go
export PATH := ${GOPATH}/bin:${PATH}
export GO111MODULE = on
PROTOC_ARCH ?= linux-x86_64
PROTOC_VERSION ?= 3.10.1
PROTOC_GEN_GO_VERSION ?= v1.3.2

clean:
	find . -iname *.pb.go -exec rm {} \;
	rm -f lightraft

${GOPATH}/bin/protoc:
	curl -L -o "/tmp/protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip" "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip"
	unzip -d ${GOPATH} "/tmp/protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip" bin/protoc

reqs: ${GOPATH}/bin/protoc
	go mod download
	go get github.com/golang/protobuf/protoc-gen-go@${PROTOC_GEN_GO_VERSION}

grpcs: reqs clean
	find . -name *.proto -print0 | xargs -I {} -0 sh -c 'protoc -I=`dirname {}` --go_out=plugins=grpc:`dirname {}` {}'

lightraft: clean grpcs
	go build
