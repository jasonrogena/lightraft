```
╦  ┬┌─┐┬ ┬┌┬┐┬─┐┌─┐┌─┐┌┬┐
║  ││ ┬├─┤ │ ├┬┘├─┤├┤  │ 
╩═╝┴└─┘┴ ┴ ┴ ┴└─┴ ┴└   ┴ 
```

Wraps around SQLite and creates a distributed database. Lightraft uses the [Raft consensus algorithm](https://raft.github.io/) for distributed consensus. This is an experiment, don't use in prod.

![Demo](./media/demo.gif)

_Demo showing a cluster with three nodes with write queries being sent to the different nodes_

### Building Lightraft

In order to build this project, you will need to install the following packages (make sure the executables are added to your PATH):

1. go (v1.13 and up)
1. make
1. curl
1. unzip

Before building the project export any of the following environment variables if you don't want to use their default values:

| Variable       | Default Value | Purpose                                                                                                                                                      |
|----------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| GOPATH | ~/go | Specifies the directory that contain the source for Go projects and their binaries. The Makefile for this project supports a single directory in the GOPATH. |
| PROTOC_ARCH | linux-x86_64 | The architecture of the protoc binary to download from [here](https://github.com/protocolbuffers/protobuf/releases). Another possible value is `osx-x86_64`. |
| PROTOC_VERSION | 3.10.1 | The version of the protoc binary to download. Check for the list of available variables [here](https://github.com/protocolbuffers/protobuf/releases). |
| PROTOC_GEN_GO_VERSION | 1.3.2 | The version of the protoc-gen-go binary to download. Should be the same version of `github.com/golang/protobuf` in the go.mod file. |

Run the following command to build the Lightraft binary (will be placed in the project's root directory):

```sh
make lightraft
```

### Running Lightraft

Update [lightraft.toml](./lightraft.toml) in case you would want to run the cluster's nodes in different hosts. By default, the configuration file allows brining up three nodes, all running in the same host but on different ports.

Each of the nodes is assigned an index (0 based index) with configurations for the node matching a `nodes` block in the lightraft.toml file. The configuration for node `0` will be the first `nodes` block in the configuration file.

To start a node, run:

```sh
./lightraft <index of node>
```

You will need to start all the nodes defined in the configuration file for the cluster to work properly.

To connect to a running node so that you can start running your SQL queries, run:

```sh
nc <configured node's client bind address> <configured node's client bind port>
```
