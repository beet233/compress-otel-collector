package model

import (
	"bytes"
	"errors"
	"github.com/emirpasic/gods/maps/treemap"
	"io"
)

const (
	initialCompressedBufferSize = 1024
	typeConflictErrMsg          = "value & definition type conflict"
	notNullableErrMsg           = "value is not nullable"
)

// Encode 将 Value 根据 Definition 进行编码，和字典一起编入 io.Writer
func Encode(val Value, def *Definition, out io.Writer) (err error) {
	valuePools := make(map[string]*treemap.Map)
	stringPool := make(map[string]int)
	w := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))
	err = innerEncode(val, def, "", valuePools, stringPool, w)
	if err != nil {
		return
	}
	_, err = out.Write(w.Bytes())
	if err != nil {
		return
	}
	return nil
}

func innerEncode(val Value, def *Definition, myName string, valuePools map[string]*treemap.Map, stringPool map[string]int, buf io.Writer) (err error) {
	if val == nil {
		if def.Nullable {
			// TODO: 编个 bit 0
			return nil
		} else {
			return errors.New(notNullableErrMsg)
		}
	}
	if val.GetType() != def.Type {
		return errors.New(typeConflictErrMsg)
	}
	// TODO：这里内部可能还是存在为空值的情况，只是 Value 本身不为 nil，有待研究各个值的空值是什么，比如 string 显然为 “”
	switch val.(type) {
	case *IntegerValue:
		// TODO: 基本类型直接编码就行，int 用 LEB
	case *BooleanValue:
	case *DoubleValue:
	case *BytesValue:
	case *StringValue:
		strv := val.(*StringValue).Data
		if _, ok := stringPool[strv]; !ok {
			stringPool[strv] = len(stringPool)
		}
		// TODO: 编入 stringPool[strv]
	case *ObjectValue:
		if def.Pooled {
			if _, ok := valuePools[myName]; !ok {
				valuePools[myName] = treemap.NewWith(ValueComparator)
			}
			myNameMap := valuePools[myName]
			if _, ok := myNameMap.Get(val); !ok {
				myNameMap.Put(val, myNameMap.Size())
			}
			// TODO: 编入 myNameMap.Get(val)
		}
		objv := val.(*ObjectValue).Data
		for fieldName, fieldDef := range def.Fields {
			innerVal, ok := objv[fieldName]
			innerEncode(innerVal, fieldDef, fieldName, valuePools, stringPool, buf)
		}
	case *ArrayValue:
		if def.Pooled {
			if _, ok := valuePools[myName]; !ok {
				valuePools[myName] = treemap.NewWith(ValueComparator)
			}
			myNameMap := valuePools[myName]
			if _, ok := myNameMap.Get(val); !ok {
				myNameMap.Put(val, myNameMap.Size())
			}
			// TODO: 编入 myNameMap.Get(val)
		}
		arrv := val.(*ArrayValue).Data
		for _, item := range arrv {
			innerEncode(item, def.ItemDefinition, "", valuePools, stringPool, buf)
		}
	}
	return nil
}
