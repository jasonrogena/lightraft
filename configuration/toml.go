package configuration

import (
	"github.com/BurntSushi/toml"
)

type Config struct {
	Cluster Cluster
	Nodes   []Node
	Driver  Driver
}

type Cluster struct {
	Name string
}

type Node struct {
	ClientBindAddress string
	ClientBindPort    int
	RPCBindAddress    string
	RPCBindPort       int
}

type Driver struct {
	Name string
}

const configPath string = "./lightraft.toml"

func GetConfig() (Config, error) {
	config := Config{}
	_, parseError := toml.DecodeFile(configPath, &config)

	return config, parseError
}
