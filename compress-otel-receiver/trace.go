package compressotelreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
)

type trace struct {
}

func (comp *trace)Start(ctx context.Context, host component.Host) error {
	return nil

}

func (comp *trace)Shutdown(ctx context.Context) error {
	return nil
}
