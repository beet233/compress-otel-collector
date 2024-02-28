package compressotelreceiver

import (
	"fmt"
	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for your receiver.
type Config struct {
	Port int `mapstructure:"port"`
}

var _ component.Config = (*Config)(nil)

// Validate the configuration for errors to implement the configvalidator interface.
// You can skip this if you do not want to validate your config
func (c *Config) Validate() error {
	fmt.Println("Port: ", c.Port)
	return nil

}
