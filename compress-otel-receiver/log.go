package compressotelreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
)

// You can change struct name
type log struct {
}

func (comp *log)Start(ctx context.Context, host component.Host) error {
	return nil

}

func (comp *log)Shutdown(ctx context.Context) error {
	return nil
}
