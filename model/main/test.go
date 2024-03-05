package main

import (
	"fmt"
	"model"
)

func main() {
	// v1 := &model.BytesValue{Data: []byte{105, 106, 153, 114, 61, 135, 109, 87}}
	v2 := &model.BytesValue{Data: []byte{105, 106, 153, 114, 61, 135, 109, 87}}
	v3 := &model.BytesValue{Data: []byte{105, 106, 153, 114, 61, 135, 109, 88}}
	fmt.Println(model.ValueComparator(v2, v3))
}
