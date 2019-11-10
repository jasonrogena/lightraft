clean:
	find . -iname *.pb.go -exec rm {} \;

reqs:
	go mod download

protobufs: reqs clean
	find . -name *.proto -print0 | xargs -I {} -0 bash -c 'protoc -I=`dirname {}` --go_out=`dirname {}` {}'

build: protobufs
	go build
