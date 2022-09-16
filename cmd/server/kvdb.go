package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"kvdb/pkg/server"
	"kvdb/pkg/utils"
	"log"
	"path"

	"gopkg.in/yaml.v2"
)

type Config struct {
	NodeConf *NodeConfig `yaml:"node"`
}

type NodeConfig struct {
	WorkDir       string            `yaml:"workDir"`
	Name          string            `yaml:"name"`
	PeerAddress   string            `yaml:"peerAddress"`
	ServerAddress string            `yaml:"serverAddress"`
	Servers       map[string]string `yaml:"servers"`
}

var configFile string

func init() {
	flag.StringVar(&configFile, "f", "config.yaml", "配置文件")
}

func main() {

	flag.Parse()
	conf, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Panicf("读取配置文件 %s 失败: %v", conf, err)
	}

	var config Config
	err = yaml.Unmarshal(conf, &config)

	if err != nil {
		log.Panicf("解析配置文件 %s 失败: %v", conf, err)
	}

	logger := utils.GetLogger(path.Join(config.NodeConf.WorkDir, config.NodeConf.Name))
	sugar := logger.Sugar()

	json, _ := json.Marshal(config.NodeConf.Servers)

	sugar.Infof("配置文件: %s , %s", configFile, string(json))

	server.Bootstrap(&server.Config{
		Dir:           config.NodeConf.WorkDir,
		Name:          config.NodeConf.Name,
		PeerAddress:   config.NodeConf.PeerAddress,
		ServerAddress: config.NodeConf.ServerAddress,
		Peers:         config.NodeConf.Servers,
		Logger:        sugar,
	}).Start()

}
