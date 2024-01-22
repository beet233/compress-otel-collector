package compressotelexporter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/beet233/compressotelcollector/model"
	"github.com/emirpasic/gods/maps/treemap"
	"io"
	"sort"
)

const (
	initialCompressedBufferSize = 1024
	typeConflictErrMsg          = "value & definition type conflict"
	notNullableErrMsg           = "value is not nullable"
)

// Encode 将 Value 根据 Definition 进行编码，和字典一起编入 io.Writer
func Encode(val model.Value, def *model.Definition, out io.Writer) (err error) {
	// 作为时间戳等状态的容器
	status := make(map[string]any)
	valuePools := make(map[string]*treemap.Map)
	valueEncodePools := make(map[string]map[int]*bytes.Buffer)
	stringPool := make(map[string]int)
	dataBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))
	err = innerEncode(val, def, "", status, valuePools, valueEncodePools, stringPool, dataBuffer)
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
func innerEncode(val model.Value, def *model.Definition, myName string, status map[string]any, valuePools map[string]*treemap.Map, valueEncodePools map[string]map[int]*bytes.Buffer, stringPool map[string]int, buf *bytes.Buffer) (err error) {

	if def.Nullable {
		if val == nil || isNullValue(val) {
			// 编个 bit 0，由于 golang 即使 boolean 也是用一整个 byte 的，所以只好如此
			err := binary.Write(buf, binary.LittleEndian, false)
			if err != nil {
				return err
			}
			return nil
		} else {
			// 编个 bit 1
			err := binary.Write(buf, binary.LittleEndian, true)
			if err != nil {
				return err
			}
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
		intv := val.(*model.IntegerValue).Data
		if def.DiffEncode {
			if _, exist := status[myName]; !exist {
				status[myName] = intv
				err := encodeInt(intv, buf)
				if err != nil {
					return err
				}
			} else {
				err := encodeInt(intv-status[myName].(int), buf)
				if err != nil {
					return err
				}
				status[myName] = intv
			}
		} else {
			err := encodeInt(intv, buf)
			if err != nil {
				return err
			}
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

		needEncode := false

		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			if _, ok := valuePools[poolId]; !ok {
				valuePools[poolId] = treemap.NewWith(model.ValueComparator)
			}
			myPool := valuePools[poolId]
			if _, ok := myPool.Get(val); !ok {
				myPool.Put(val, myPool.Size())
				needEncode = true
			}
		} else {
			needEncode = true
		}

		tempBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))

		if needEncode {
			err := encodeInt(len(val.(*model.BytesValue).Data), tempBuffer)
			if err != nil {
				return err
			}
			_, err = tempBuffer.Write(val.(*model.BytesValue).Data)
			if err != nil {
				return err
			}
		}

		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			index, _ := valuePools[poolId].Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
			// 存储 tempBuffer 的结果到 map
			if _, ok := valueEncodePools[poolId]; !ok {
				valueEncodePools[poolId] = make(map[int]*bytes.Buffer)
			}
			valueEncodePools[poolId][index.(int)] = tempBuffer
		} else {
			_, err := buf.Write(tempBuffer.Bytes())
			if err != nil {
				return err
			}
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
		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			if _, ok := valuePools[poolId]; !ok {
				valuePools[poolId] = treemap.NewWith(model.ValueComparator)
			}
			myPool := valuePools[poolId]
			if _, ok := myPool.Get(val); !ok {
				myPool.Put(val, myPool.Size())
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
			if len(myName) >= len("attributes") && myName[len(myName)-len("attributes"):] == "attributes" {
				err := innerFreeMapEncode(objv, stringPool, tempBuffer)
				if err != nil {
					return nil
				}
			}
			if len(myName) > 0 {
				myName = myName + " "
			}
			// 这里有问题，编码各个 field 的顺序是随机的....
			// for fieldName, fieldDef := range def.Fields {
			// 	innerVal := objv[fieldName]
			// 	err := innerEncode(innerVal, fieldDef, myName+fieldName, valuePools, valueEncodePools, stringPool, tempBuffer)
			// 	if err != nil {
			// 		return err
			// 	}
			// }
			// 改成按字典序吧
			for _, fieldName := range getSortedKeys(def.Fields) {
				fieldDef := def.Fields[fieldName]
				innerVal := objv[fieldName]
				err := innerEncode(innerVal, fieldDef, myName+fieldName, status, valuePools, valueEncodePools, stringPool, tempBuffer)
				if err != nil {
					return err
				}
			}

			if len(myName) > 0 {
				myName = myName[:len(myName)-1]
			}
		}

		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			index, _ := valuePools[poolId].Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
			// 存储 tempBuffer 的结果到 map
			if _, ok := valueEncodePools[poolId]; !ok {
				valueEncodePools[poolId] = make(map[int]*bytes.Buffer)
			}
			valueEncodePools[poolId][index.(int)] = tempBuffer
		} else {
			_, err := buf.Write(tempBuffer.Bytes())
			if err != nil {
				return err
			}
		}
	case *model.ArrayValue:

		needEncode := false
		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			if _, ok := valuePools[poolId]; !ok {
				valuePools[poolId] = treemap.NewWith(model.ValueComparator)
			}
			myPool := valuePools[poolId]
			if _, ok := myPool.Get(val); !ok {
				myPool.Put(val, myPool.Size())
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
			err := encodeInt(len(arrv), tempBuffer)
			if err != nil {
				return err
			}
			if len(myName) > 0 {
				myName = myName + " "
			}
			for _, item := range arrv {
				err := innerEncode(item, def.ItemDefinition, myName+"item", status, valuePools, valueEncodePools, stringPool, tempBuffer)
				if err != nil {
					return err
				}
			}
			if len(myName) > 0 {
				myName = myName[:len(myName)-1]
			}
		}

		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			index, _ := valuePools[poolId].Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
			// 存储 tempBuffer 的结果到 map
			if _, ok := valueEncodePools[poolId]; !ok {
				valueEncodePools[poolId] = make(map[int]*bytes.Buffer)
			}
			valueEncodePools[poolId][index.(int)] = tempBuffer
		} else {
			_, err := buf.Write(tempBuffer.Bytes())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// 将自由的 map （其实只有 attributes 及其内部）编码进 buf，过程中 string 同样需要处理入池
func innerFreeMapEncode(freeMap map[string]model.Value, stringPool map[string]int, buf *bytes.Buffer) error {
	// freeMap 需要有 size，而有 def 的不需要
	err := encodeInt(len(freeMap), buf)
	if err != nil {
		return err
	}
	// freeMap 中我们不需要关心遍历 map 的顺序
	for key, value := range freeMap {
		if _, exist := stringPool[key]; !exist {
			stringPool[key] = len(stringPool)
		}
		err := encodeInt(stringPool[key], buf)
		if err != nil {
			return err
		}
		// 一个是否为 null 的标记位
		if value == nil {
			err := binary.Write(buf, binary.LittleEndian, false)
			if err != nil {
				return err
			}
		} else {
			err := binary.Write(buf, binary.LittleEndian, true)
			if err != nil {
				return err
			}
			err = encodeInt(int(value.GetType()), buf)
			if err != nil {
				return err
			}
			err = innerFreeValueEncode(value, stringPool, buf)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func innerFreeValueEncode(value model.Value, stringPool map[string]int, buf *bytes.Buffer) error {
	switch value.(type) {
	case *model.IntegerValue:
		err := encodeInt(value.(*model.IntegerValue).Data, buf)
		if err != nil {
			return err
		}
	case *model.BooleanValue:
		err := binary.Write(buf, binary.LittleEndian, value.(*model.BooleanValue).Data)
		if err != nil {
			return err
		}
	case *model.DoubleValue:
		err := binary.Write(buf, binary.LittleEndian, value.(*model.DoubleValue).Data)
		if err != nil {
			return err
		}
	case *model.BytesValue:
		err := encodeInt(len(value.(*model.BytesValue).Data), buf)
		if err != nil {
			return err
		}
		_, err = buf.Write(value.(*model.BytesValue).Data)
		if err != nil {
			return err
		}
	case *model.StringValue:
		strv := value.(*model.StringValue).Data
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
		objv := value.(*model.ObjectValue).Data
		err := innerFreeMapEncode(objv, stringPool, buf)
		if err != nil {
			return err
		}
	case *model.ArrayValue:
		arrv := value.(*model.ArrayValue).Data
		err := encodeInt(len(arrv), buf)
		if err != nil {
			return err
		}
		// 编码数组内元素的类型
		if len(arrv) > 0 {
			err := encodeInt(int(arrv[0].GetType()), buf)
			if err != nil {
				return err
			}
		}
		for i := 0; i < len(arrv); i++ {
			err := innerFreeValueEncode(arrv[i], stringPool, buf)
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

func getSortedKeys(m map[string]*model.Definition) []string {
	// 数组默认长度为map长度,后面append时,不需要重新申请内存和拷贝,效率很高
	j := 0
	keys := make([]string, len(m))
	for k := range m {
		keys[j] = k
		j++
	}
	sort.Strings(keys)
	return keys
}
