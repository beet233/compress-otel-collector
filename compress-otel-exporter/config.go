package compressotelexporter

import (
	"fmt"
)

// Config defines configuration for your exporter.
type config struct {
	Leb128Enabled     bool   `mapstructure:"leb128_enabled"`
	StringPoolEnabled bool   `mapstructure:"string_pool_enabled"`
	TargetReceiverUrl string `mapstructure:"target_receiver_url"`
}

// var _ component.Config = (*config)(nil)
var MyConfig *config = &config{}

// Validate the configuration for errors to implement the configvalidator interface.
// You can skip this if you do not want to validate your config
func (c *config) Validate() error {
	fmt.Println("Leb128Enabled: ", c.Leb128Enabled)
	fmt.Println("StringPoolEnabled: ", c.StringPoolEnabled)
	fmt.Println("TargetReceiverUrl: ", c.TargetReceiverUrl)
	return nil

}
