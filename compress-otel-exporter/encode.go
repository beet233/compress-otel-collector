package compressotelexporter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/beet233/compressotelcollector/model"
	"github.com/emirpasic/gods/maps/treemap"
	"io"
)

const (
	initialCompressedBufferSize = 1024
	typeConflictErrMsg          = "value & definition type conflict"
	notNullableErrMsg           = "value is not nullable"
)

// Encode 将 Value 根据 Definition 进行编码，和字典一起编入 io.Writer
func Encode(val model.Value, def *model.Definition, out io.Writer) (err error) {
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

func innerEncode(val model.Value, def *model.Definition, myName string, valuePools map[string]*treemap.Map, stringPool map[string]int, buf *bytes.Buffer) (err error) {
	if def.Nullable {
		if val == nil || isNullValue(val) {
			// 编个 bit 0，由于 golang 即使 boolean 也是用一整个 byte 的，所以只好如此
			err := binary.Write(buf, binary.LittleEndian, false)
			if err != nil {
				return err
			}
			return nil
		}
	}
	if val == nil {
		return errors.New(notNullableErrMsg)
	}
	if val.GetType() != def.Type {
		return errors.New(typeConflictErrMsg)
	}
	switch val.(type) {
	case *model.IntegerValue:
		err := encodeInt(val.(*model.IntegerValue).Data, buf)
		if err != nil {
			return err
		}
	case *model.BooleanValue:
		err := binary.Write(buf, binary.LittleEndian, val.(*model.BooleanValue).Data)
		if err != nil {
			return err
		}
	case *model.DoubleValue:
		err := binary.Write(buf, binary.LittleEndian, val.(*model.DoubleValue).Data)
		if err != nil {
			return err
		}
	case *model.BytesValue:
		_, err := buf.Write(val.(*model.BytesValue).Data)
		if err != nil {
			return err
		}
	case *model.StringValue:
		strv := val.(*model.StringValue).Data
		if MyConfig.StringPoolEnabled {
			if _, ok := stringPool[strv]; !ok {
				stringPool[strv] = len(stringPool)
			}
			err := encodeInt(stringPool[strv], buf)
			if err != nil {
				return err
			}
		} else {
			err := encodeInt(len(strv), buf)
			if err != nil {
				return err
			}
			_, err = buf.WriteString(strv)
			if err != nil {
				return err
			}
		}
	case *model.ObjectValue:
		if def.Pooled {
			if _, ok := valuePools[myName]; !ok {
				valuePools[myName] = treemap.NewWith(model.ValueComparator)
			}
			myNameMap := valuePools[myName]
			if _, ok := myNameMap.Get(val); !ok {
				myNameMap.Put(val, myNameMap.Size())
			}
			index, _ := myNameMap.Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
		}
		objv := val.(*model.ObjectValue).Data
		for fieldName, fieldDef := range def.Fields {
			innerVal := objv[fieldName]
			err := innerEncode(innerVal, fieldDef, fieldName, valuePools, stringPool, buf)
			if err != nil {
				return err
			}
		}
	case *model.ArrayValue:
		if def.Pooled {
			if _, ok := valuePools[myName]; !ok {
				valuePools[myName] = treemap.NewWith(model.ValueComparator)
			}
			myNameMap := valuePools[myName]
			if _, ok := myNameMap.Get(val); !ok {
				myNameMap.Put(val, myNameMap.Size())
			}
			index, _ := myNameMap.Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
		}
		arrv := val.(*model.ArrayValue).Data
		err := encodeInt(len(arrv), buf)
		if err != nil {
			return err
		}
		for _, item := range arrv {
			err := innerEncode(item, def.ItemDefinition, "", valuePools, stringPool, buf)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// val can't be nil
func isNullValue(val model.Value) bool {
	switch val.(type) {
	case *model.IntegerValue:
		return val.(*model.IntegerValue).Data == 0
	case *model.BytesValue:
		return len(val.(*model.BytesValue).Data) == 0
	case *model.StringValue:
		return len(val.(*model.StringValue).Data) == 0
	case *model.ArrayValue:
		return len(val.(*model.ArrayValue).Data) == 0
	}
	return false
}

func encodeInt(val int, buf *bytes.Buffer) error {
	if MyConfig.Leb128Enabled {
		for {
			// Get the 7 least significant bits of the value.
			byteVal := byte(val & 0x7F)
			val >>= 7

			// If there are no more bits to encode, or there are bits left but they
			// are just the sign bit, write the byte and break.
			// 这里使用的是有符号的 sleb128，对于负数可以省略前置的 11111111，对于正数省略前置的 00000000
			if (val == 0 && (byteVal&0x40) == 0) || (val == -1 && (byteVal&0x40) != 0) {
				return buf.WriteByte(byteVal)
			}

			// Set the continuation bit for the next byte.
			byteVal |= 0x80
			if err := buf.WriteByte(byteVal); err != nil {
				return err
			}
		}
	} else {
		return binary.Write(buf, binary.LittleEndian, int64(val))
	}
}
