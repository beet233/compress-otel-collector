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
	var result model.Value
	// decode stringPool
	stringPoolSize, err := reader.readLeb128Int()
	if err != nil {
		return nil, err
	}
	stringPool := make([]string, 0, stringPoolSize)
	for i := 0; i < stringPoolSize; i++ {
		stringLen, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		string, err := reader.readString(stringLen)
		if err != nil {
			return nil, err
		}
		stringPool = append(stringPool, string)
		// fmt.Println(string)
	}
	// decode valuePools
	// valuePools := make(map[string][]model.Value)
	return &result, nil
}
