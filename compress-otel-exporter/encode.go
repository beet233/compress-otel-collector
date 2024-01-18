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
	valueEncodePools := make(map[string]map[int]*bytes.Buffer)
	stringPool := make(map[string]int)
	dataBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))
	err = innerEncode(val, def, "", valuePools, valueEncodePools, stringPool, dataBuffer)
	if err != nil {
		return
	}
	// 编码 valuePools 以及 stringPool 进 metaBuffer
	metaBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))
	// 解析需要的是 index -> value，所以编码进去的应该是 reverse map
	// 先编码 stringPool
	strings := sortMapByValue(stringPool)
	for i := 0; i < len(strings); i++ {
		err = encodeInt(len(strings[i]), metaBuffer)
		if err != nil {
			return err
		}
		_, err = metaBuffer.WriteString(strings[i])
		if err != nil {
			return err
		}
	}

	// 编码 valuePools 顺序
	// fmt.Println("valuePools 顺序：")
	// for _, field := range model.GetTopologicalTraceModelFields() {
	// 	fmt.Print(field, ",")
	// }
	// fmt.Println()

	for _, field := range model.GetTopologicalTraceModelFields() {
		valuePool, exist := valuePools[field]
		if exist {
			_, err = metaBuffer.WriteString(field)
			if err != nil {
				return err
			}
			values := sortTreeMapByValue(valuePool)
			for i := 0; i < len(values); i++ {
				_, err = metaBuffer.Write(valueEncodePools[field][i].Bytes())
				if err != nil {
					return err
				}
			}
		}
	}

	_, err = out.Write(metaBuffer.Bytes())
	if err != nil {
		return
	}
	_, err = out.Write(dataBuffer.Bytes())
	if err != nil {
		return
	}
	return nil
}

// 传承层级 myName 作为 valuePools 的 key，如 "resourceSpans item resource attributes" 中间用一个空格
func innerEncode(val model.Value, def *model.Definition, myName string, valuePools map[string]*treemap.Map, valueEncodePools map[string]map[int]*bytes.Buffer, stringPool map[string]int, buf *bytes.Buffer) (err error) {

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
		err := encodeInt(len(val.(*model.BytesValue).Data), buf)
		if err != nil {
			return err
		}
		_, err = buf.Write(val.(*model.BytesValue).Data)
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

		needEncode := false
		if def.Pooled {
			if _, ok := valuePools[myName]; !ok {
				valuePools[myName] = treemap.NewWith(model.ValueComparator)
			}
			myNameMap := valuePools[myName]
			if _, ok := myNameMap.Get(val); !ok {
				myNameMap.Put(val, myNameMap.Size())
				// 如果池化且第一次加入池子，则需要编码
				needEncode = true
			}
		} else {
			// 如果没池化则需要编码
			needEncode = true
		}

		tempBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))

		if needEncode {
			objv := val.(*model.ObjectValue).Data
			if len(myName) > 0 {
				myName = myName + " "
			}
			for fieldName, fieldDef := range def.Fields {
				innerVal := objv[fieldName]
				err := innerEncode(innerVal, fieldDef, myName+fieldName, valuePools, valueEncodePools, stringPool, tempBuffer)
				if err != nil {
					return err
				}
			}
			if len(myName) > 0 {
				myName = myName[:len(myName)-1]
			}
		}

		if def.Pooled {
			index, _ := valuePools[myName].Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
			// 存储 tempBuffer 的结果到 map
			if _, ok := valueEncodePools[myName]; !ok {
				valueEncodePools[myName] = make(map[int]*bytes.Buffer)
			}
			valueEncodePools[myName][index.(int)] = tempBuffer
		} else {
			_, err := buf.Write(tempBuffer.Bytes())
			if err != nil {
				return err
			}
		}
	case *model.ArrayValue:

		needEncode := false
		if def.Pooled {
			if _, ok := valuePools[myName]; !ok {
				valuePools[myName] = treemap.NewWith(model.ValueComparator)
			}
			myNameMap := valuePools[myName]
			if _, ok := myNameMap.Get(val); !ok {
				myNameMap.Put(val, myNameMap.Size())
				// 如果池化且第一次加入池子，则需要编码
				needEncode = true
			}
		} else {
			// 如果没池化则需要编码
			needEncode = true
		}

		tempBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))

		if needEncode {
			arrv := val.(*model.ArrayValue).Data
			err := encodeInt(len(arrv), buf)
			if err != nil {
				return err
			}
			if len(myName) > 0 {
				myName = myName + " "
			}
			for _, item := range arrv {
				err := innerEncode(item, def.ItemDefinition, myName+"item", valuePools, valueEncodePools, stringPool, tempBuffer)
				if err != nil {
					return err
				}
			}
			if len(myName) > 0 {
				myName = myName[:len(myName)-1]
			}
		}

		if def.Pooled {
			index, _ := valuePools[myName].Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
			// 存储 tempBuffer 的结果到 map
			if _, ok := valueEncodePools[myName]; !ok {
				valueEncodePools[myName] = make(map[int]*bytes.Buffer)
			}
			valueEncodePools[myName][index.(int)] = tempBuffer
		} else {
			_, err := buf.Write(tempBuffer.Bytes())
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

// 遍历 map 来重新构建一个 map[int]string，再从 0~size-1 取结果，复杂度只有 O(N)
func sortMapByValue(inputMap map[string]int) []string {

	var sortedKeys []string

	tempMap := make(map[int]string)
	for key, value := range inputMap {
		tempMap[value] = key
	}

	for i := 0; i < len(tempMap); i++ {
		sortedKeys = append(sortedKeys, tempMap[i])
	}

	return sortedKeys
}

func sortTreeMapByValue(inputMap *treemap.Map) []model.Value {

	var sortedValues []model.Value

	tempMap := make(map[int]model.Value)
	inputMap.Each(func(key interface{}, value interface{}) {
		tempMap[value.(int)] = key.(model.Value)
	})

	for i := 0; i < len(tempMap); i++ {
		sortedValues = append(sortedValues, tempMap[i])
	}

	return sortedValues
}
