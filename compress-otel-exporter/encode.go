package compressotelexporter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/beet233/compressotelcollector/model"
	"github.com/emirpasic/gods/maps/treemap"
	"io"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	initialCompressedBufferSize = 1024
	typeConflictErrMsg          = "value & definition type conflict"
	notNullableErrMsg           = "value is not nullable"
)

// 定义一个全局的 Pool，用于存放 *bytes.Buffer 实例。
var bufferPool = sync.Pool{
	New: func() interface{} {
		// 池中没有对象时，自动创建一个 Buffer 并返回。
		return new(bytes.Buffer)
	},
}

// Encode 将 Value 根据 Definition 进行编码，和字典一起编入 io.Writer
func Encode(val model.Value, def *model.Definition, out io.Writer) (err error) {
	f, err := os.Create(strconv.FormatInt(time.Now().UnixNano(), 10) + "_pprof")
	// start to record CPU profile and write to file `f`
	_ = pprof.StartCPUProfile(f)
	// stop to record CPU profile
	defer pprof.StopCPUProfile()
	// 作为时间戳等状态的容器
	status := make(map[string]any)
	valuePools := make(map[string]*treemap.Map)
	valueEncodePools := make(map[string]map[int]*bytes.Buffer)
	stringPool := make(map[string]int)
	dataBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))
	dataBuffer.WriteString("cprval")
	err = innerEncode(val, def, "", &status, &valuePools, &valueEncodePools, &stringPool, dataBuffer)
	if err != nil {
		return
	}
	// 编码 valuePools 以及 stringPool 进 metaBuffer
	metaBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))
	// 解析需要的是 index -> value，所以编码进去的应该是 reverse map
	// 先编码 stringPool
	strings := sortMapByValue(stringPool)
	err = encodeInt(len(strings), metaBuffer)
	if err != nil {
		return err
	}
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

	err = encodeInt(len(valuePools), metaBuffer)
	if err != nil {
		return err
	}
	for _, field := range model.GetTopologicalTraceModelFields() {
		valuePool, exist := valuePools[field]
		if exist {
			err = encodeInt(len(field), metaBuffer)
			if err != nil {
				return err
			}
			_, err = metaBuffer.WriteString(field)
			if err != nil {
				return err
			}
			// values := sortTreeMapByValue(valuePool)
			err = encodeInt(valuePool.Size(), metaBuffer)
			if err != nil {
				return err
			}
			for i := 0; i < valuePool.Size(); i++ {
				// 不需要 bytes 的 len，bytes 本身是可根据 def 解析的
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
func innerEncode(val model.Value, def *model.Definition, myName string, status *map[string]any, valuePools *map[string]*treemap.Map, valueEncodePools *map[string]map[int]*bytes.Buffer, stringPool *map[string]int, buf *bytes.Buffer) (err error) {

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
			if _, exist := (*status)[myName]; !exist {
				(*status)[myName] = intv
				err := encodeInt(intv, buf)
				if err != nil {
					return err
				}
			} else {
				err := encodeInt(intv-(*status)[myName].(int), buf)
				if err != nil {
					return err
				}
				(*status)[myName] = intv
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
			if _, ok := (*valuePools)[poolId]; !ok {
				(*valuePools)[poolId] = treemap.NewWith(model.ValueComparator)
			}
			myPool := (*valuePools)[poolId]
			if _, ok := myPool.Get(val); !ok {
				// fmt.Println("add into pool", poolId, val, myPool.Size())
				myPool.Put(val, myPool.Size())
				needEncode = true
			}
		} else {
			needEncode = true
		}

		tempBuffer := bufferPool.Get().(*bytes.Buffer)
		// tempBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))

		if needEncode {
			// fmt.Println("bytes len:", len(val.(*model.BytesValue).Data))
			// fmt.Println("bytes:", val.(*model.BytesValue).Data)
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
			index, _ := (*valuePools)[poolId].Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
			// 存储 tempBuffer 的结果到 map
			if needEncode {
				if _, ok := (*valueEncodePools)[poolId]; !ok {
					(*valueEncodePools)[poolId] = make(map[int]*bytes.Buffer)
				}
				(*valueEncodePools)[poolId][index.(int)] = tempBuffer
				// fmt.Println("add into encode pool", poolId, tempBuffer.Bytes(), index.(int))
			}
		} else {
			_, err := buf.Write(tempBuffer.Bytes())
			if err != nil {
				return err
			}
			tempBuffer.Reset()
			bufferPool.Put(tempBuffer)
		}

	case *model.StringValue:
		// strv := val.(*model.StringValue).Data
		// if MyConfig.StringPoolEnabled {
		// 	if _, ok := stringPool[strv]; !ok {
		// 		stringPool[strv] = len(stringPool)
		// 	}
		// 	err := encodeInt(stringPool[strv], buf)
		// 	if err != nil {
		// 		return err
		// 	}
		// } else {
		// 	err := encodeInt(len(strv), buf)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	_, err = buf.WriteString(strv)
		// 	if err != nil {
		// 		return err
		// 	}
		// }

		needEncode := false

		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			if _, ok := (*valuePools)[poolId]; !ok {
				(*valuePools)[poolId] = treemap.NewWith(model.ValueComparator)
			}
			myPool := (*valuePools)[poolId]
			if _, ok := myPool.Get(val); !ok {
				myPool.Put(val, myPool.Size())
				needEncode = true
			}
		} else {
			needEncode = true
		}

		tempBuffer := bufferPool.Get().(*bytes.Buffer)
		// tempBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))

		if needEncode {
			err := encodeInt(len(val.(*model.StringValue).Data), tempBuffer)
			if err != nil {
				return err
			}
			_, err = tempBuffer.WriteString(val.(*model.StringValue).Data)
			if err != nil {
				return err
			}
		}

		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			index, _ := (*valuePools)[poolId].Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
			// 存储 tempBuffer 的结果到 map
			if needEncode {
				if _, ok := (*valueEncodePools)[poolId]; !ok {
					(*valueEncodePools)[poolId] = make(map[int]*bytes.Buffer)
				}
				(*valueEncodePools)[poolId][index.(int)] = tempBuffer
			}
		} else {
			_, err := buf.Write(tempBuffer.Bytes())
			if err != nil {
				return err
			}
			tempBuffer.Reset()
			bufferPool.Put(tempBuffer)
		}
	case *model.ObjectValue:

		needEncode := false
		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			if _, ok := (*valuePools)[poolId]; !ok {
				(*valuePools)[poolId] = treemap.NewWith(model.ValueComparator)
			}
			myPool := (*valuePools)[poolId]
			if _, ok := myPool.Get(val); !ok {
				myPool.Put(val, myPool.Size())
				// 如果池化且第一次加入池子，则需要编码
				needEncode = true
			}
		} else {
			// 如果没池化则需要编码
			needEncode = true
		}

		tempBuffer := bufferPool.Get().(*bytes.Buffer)
		// tempBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))

		if needEncode {
			objv := val.(*model.ObjectValue).Data
			// if len(myName) >= len("attributes") && myName[len(myName)-len("attributes"):] == "attributes" {
			if def.Fields == nil {
				err := innerFreeMapEncode(objv, stringPool, tempBuffer)
				if err != nil {
					return nil
				}
			} else {
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
		}

		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			index, _ := (*valuePools)[poolId].Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
			// 存储 tempBuffer 的结果到 map
			if needEncode {
				if _, ok := (*valueEncodePools)[poolId]; !ok {
					(*valueEncodePools)[poolId] = make(map[int]*bytes.Buffer)
				}
				(*valueEncodePools)[poolId][index.(int)] = tempBuffer
			}
		} else {
			_, err := buf.Write(tempBuffer.Bytes())
			if err != nil {
				return err
			}
			tempBuffer.Reset()
			bufferPool.Put(tempBuffer)
		}
	case *model.ArrayValue:

		needEncode := false
		if def.Pooled || def.SharePooled {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			if _, ok := (*valuePools)[poolId]; !ok {
				(*valuePools)[poolId] = treemap.NewWith(model.ValueComparator)
			}
			myPool := (*valuePools)[poolId]
			if _, ok := myPool.Get(val); !ok {
				myPool.Put(val, myPool.Size())
				// 如果池化且第一次加入池子，则需要编码
				needEncode = true
			}
		} else {
			// 如果没池化则需要编码
			needEncode = true
		}

		tempBuffer := bufferPool.Get().(*bytes.Buffer)
		// tempBuffer := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))

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
			index, _ := (*valuePools)[poolId].Get(val)
			err := encodeInt(index.(int), buf)
			if err != nil {
				return err
			}
			// 存储 tempBuffer 的结果到 map
			if needEncode {
				if _, ok := (*valueEncodePools)[poolId]; !ok {
					(*valueEncodePools)[poolId] = make(map[int]*bytes.Buffer)
				}
				(*valueEncodePools)[poolId][index.(int)] = tempBuffer
			}
		} else {
			_, err := buf.Write(tempBuffer.Bytes())
			if err != nil {
				return err
			}
			tempBuffer.Reset()
			bufferPool.Put(tempBuffer)
		}
	}
	return nil
}

// 将自由的 map （其实只有 attributes 及其内部）编码进 buf，过程中 string 同样需要处理入池
func innerFreeMapEncode(freeMap map[string]model.Value, stringPool *map[string]int, buf *bytes.Buffer) error {
	// freeMap 需要有 size，而有 def 的不需要
	err := encodeInt(len(freeMap), buf)
	if err != nil {
		return err
	}
	// freeMap 中我们不需要关心遍历 map 的顺序
	for key, value := range freeMap {
		if _, exist := (*stringPool)[key]; !exist {
			(*stringPool)[key] = len(*stringPool)
		}
		err := encodeInt((*stringPool)[key], buf)
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

func innerFreeValueEncode(value model.Value, stringPool *map[string]int, buf *bytes.Buffer) error {
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
			if _, ok := (*stringPool)[strv]; !ok {
				(*stringPool)[strv] = len(*stringPool)
			}
			err := encodeInt((*stringPool)[strv], buf)
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
		// if len(arrv) > 0 {
		// 	err := encodeInt(int(arrv[0].GetType()), buf)
		// 	if err != nil {
		// 		return err
		// 	}
		// }
		for i := 0; i < len(arrv); i++ {
			err := encodeInt(int(arrv[i].GetType()), buf)
			if err != nil {
				return err
			}
			err = innerFreeValueEncode(arrv[i], stringPool, buf)
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
	case *model.ObjectValue:
		return len(val.(*model.ObjectValue).Data) == 0
	case *model.ArrayValue:
		return len(val.(*model.ArrayValue).Data) == 0
	}
	return false
}

func encodeInt(val int, buf *bytes.Buffer) error {
	if MyConfig.Leb128Enabled {
		// 这里使用的是有符号的 sleb128，对于负数可以省略前置的 11111111，对于正数省略前置的 00000000
		more := true
		i := 0
		for i < 8 && more {
			// 取出val的最低7位
			byteVal := byte(val & 0x7F)
			// 去掉已处理的7位
			val >>= 7

			// 设置继续位(最高位): 如果val非零或字节的符号位与val的符号位不一致
			// 当val为正数且byteVal的第7位为1，或val为负数且byteVal的第7位为0，需要设置继续位，说明后面还有字节
			// 就是说符号位要留一个，不能全删了，不然正负就改变了
			// 继续位的判断依据了负数在内存中的符号扩展特性
			shouldContinue := (val != 0 && val != -1)                            // 检查是否还有更多位需要编码
			needsSignExtension := ((byteVal & 0x40) != 0) != ((val & 0x40) != 0) // 检查符号位扩展是否需要

			more = shouldContinue || needsSignExtension
			if more {
				byteVal |= 0x80 // 如果有更多的字节，则将byteVal的第8位设置为1
			}
			// 将处理后的字节写入到缓冲区
			if err := buf.WriteByte(byteVal); err != nil {
				return err
			}
			i += 1
		}

		// 如果需要最高位的 8 bit，那就直接 copy，不用取 7 bit 了
		if i == 8 && more {
			err := buf.WriteByte(byte(val & 0xFF))
			if err != nil {
				return err
			}
		}

		return nil
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
