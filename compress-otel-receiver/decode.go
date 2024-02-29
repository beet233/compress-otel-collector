package compressotelreceiver

import (
	"github.com/beet233/compressotelcollector/model"
	"io"
)

func Decode(def *model.Definition, in io.Reader) (*model.Value, error) {
	data, err := io.ReadAll(in)
	if err != nil {
		return nil, err
	}
	reader := NewDataReader(data)
}
