package compressotelreceiver

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	// typeStr is the type of the receiver
	typeStr = "compressotelreceiver"
)

// NewFactory creates a receiver factory
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		// Uncomment the receiver type that you would like, change the second parameter as you like
		// component.StabilityLevelUndefined
		// component.StabilityLevelUnmaintained
		// component.StabilityLevelDeprecated
		// component.StabilityLevelDevelopment
		// component.StabilityLevelAlpha
		// component.StabilityLevelBeta
		// component.StabilityLevelStable
        receiver.WithTraces(createTracesReceiver, component.StabilityLevelBeta),
        
        receiver.WithLogs(createLogsReceiver, component.StabilityLevelAlpha),
        
	)
}

func createDefaultConfig() component.Config {

	return &Config{}
}
func createTracesReceiver(
	ctx context.Context,
	set receiver.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver receiver.Traces, err error) {

	return &trace{}, err

}
func createLogsReceiver(
	ctx context.Context,
	set receiver.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver receiver.Logs, err error) {

	return &log{}, err

}