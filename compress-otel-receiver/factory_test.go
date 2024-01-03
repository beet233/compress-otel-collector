package compressotelreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}
func TestCreateTracesReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	te, err := factory.CreateTracesReceiver(context.Background(), receivertest.NewNopCreateSettings(), cfg, nil)
	assert.NoError(t, err)
	assert.NotNil(t, te)
}
func TestCreateLogsReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	te, err := factory.CreateLogsReceiver(context.Background(), receivertest.NewNopCreateSettings(), cfg, nil)
	assert.NoError(t, err)
	assert.NotNil(t, te)
}