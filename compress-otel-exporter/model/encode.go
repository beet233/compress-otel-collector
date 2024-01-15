package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/emirpasic/gods/maps/treemap"
	"io"
)

const (
	leb128Enabled               = true
	stringPoolEnabled           = true
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

func innerEncode(val Value, def *Definition, myName string, valuePools map[string]*treemap.Map, stringPool map[string]int, buf *bytes.Buffer) (err error) {
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
	case *IntegerValue:
		err := encodeInt(val.(*IntegerValue).Data, buf)
		if err != nil {
			return err
		}
	case *BooleanValue:
		err := binary.Write(buf, binary.LittleEndian, val.(*BooleanValue).Data)
		if err != nil {
			return err
		}
	case *DoubleValue:
		err := binary.Write(buf, binary.LittleEndian, val.(*DoubleValue).Data)
		if err != nil {
			return err
		}
	case *BytesValue:
		_, err := buf.Write(val.(*BytesValue).Data)
		if err != nil {
			return err
		}
	case *StringValue:
		strv := val.(*StringValue).Data
		if stringPoolEnabled {
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
	case *ObjectValue:
		if def.Pooled {
			if _, ok := valuePools[myName]; !ok {
				valuePools[myName] = treemap.NewWith(ValueComparator)
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
		objv := val.(*ObjectValue).Data
		for fieldName, fieldDef := range def.Fields {
			innerVal := objv[fieldName]
			err := innerEncode(innerVal, fieldDef, fieldName, valuePools, stringPool, buf)
			if err != nil {
				return err
			}
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
			index, _ := myNameMap.Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
		}
		arrv := val.(*ArrayValue).Data
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
func isNullValue(val Value) bool {
	switch val.(type) {
	case *IntegerValue:
		return val.(*IntegerValue).Data == 0
	case *BytesValue:
		return len(val.(*BytesValue).Data) == 0
	case *StringValue:
		return len(val.(*StringValue).Data) == 0
	case *ArrayValue:
		return len(val.(*ArrayValue).Data) == 0
	}
	return false
}

func encodeInt(val int, buf *bytes.Buffer) error {
	if leb128Enabled {
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
