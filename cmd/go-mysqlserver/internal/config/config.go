package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config mirrors cmd/go-mysqlserver/config.yaml.
type Config struct {
	Servers []Server `yaml:"servers"`
}

// Server defines a single upstream MySQL configuration.
type Server struct {
	Name         string `yaml:"name"`
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Database     string `yaml:"database"`
	Charset      string `yaml:"charset"`
	Collation    string `yaml:"collation"`
	Timeout      string `yaml:"timeout"`
	ReadTimeout  string `yaml:"readTimeout"`
	WriteTimeout string `yaml:"writeTimeout"`
	Loc          string `yaml:"loc"`
	TLS          string `yaml:"tls"`
}

// Load reads and parses a YAML config file from the provided path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	return &cfg, nil
}
