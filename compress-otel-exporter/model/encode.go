package model

import (
	"github.com/emirpasic/gods/maps/treemap"
	"io"
)

// 将 Value 根据 Definition 进行编码，和字典一起编入 io.Writer
func Encode(val *Value, def *Definition, out io.Writer) (err error) {
	valuePools := make(map[string]*treemap.Map)
	stringPool := make(map[string]int)
	return nil
}
