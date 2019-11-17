```
╦  ┬┌─┐┬ ┬┌┬┐┬─┐┌─┐┌─┐┌┬┐
║  ││ ┬├─┤ │ ├┬┘├─┤├┤  │ 
╩═╝┴└─┘┴ ┴ ┴ ┴└─┴ ┴└   ┴ 
```

Wraps around SQLite and creates a distributed database. Uses [RAFT](https://raft.github.io/) for distributed consensus. This is an experiment, don't use in prod.

### Building Lightraft

In order to build this project, you will need to install the following packages (make sure the executables are added to your PATH):

1. go (v1.13 and up)
1. make
1. curl
1. unzip

Before building the project export any of the following environment variables if you don't want to use their default values:

|    Variable    | Default Value | Purpose                                                                                                                                                      |
|:--------------:|:-------------:|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| GOPATH         | ~/go          | Specifies the directory that contain the source for Go projects and their binaries. The Makefile for this project supports a single directory in the GOPATH. |
| PROTOC_ARCH    | linux-x86_64  | The architecture of the protoc binary to download from [here](https://github.com/protocolbuffers/protobuf/releases). Another possible value is `osx-x86_64`. |
| PROTOC_VERSION | 3.10.1        | The version of the protoc binary to download. Check for the list of available variables [here](https://github.com/protocolbuffers/protobuf/releases).        |

Run the following command to build the Lightraft binary (will be placed in the project's root directory):

```sh
make lightraft
```

