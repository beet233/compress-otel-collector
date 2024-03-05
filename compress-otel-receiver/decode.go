package compressotelreceiver

import (
	"errors"
	"fmt"
	"github.com/beet233/compressotelcollector/model"
	"io"
	"sort"
	"strconv"
)

func Decode(def *model.Definition, in io.Reader) (model.Value, error) {
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
	valuePools := make(map[string][]model.Value)
	valuePoolsCount, err := reader.readLeb128Int()
	if err != nil {
		return nil, err
	}
	for i := 0; i < valuePoolsCount; i++ {
		fieldNameLen, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		fieldName, err := reader.readString(fieldNameLen)
		if err != nil {
			return nil, err
		}
		valuePools[fieldName] = []model.Value{}
		fieldDef := model.FieldStringToDefinition(fieldName, def)
		fmt.Println("fieldName:", fieldName, "fieldDef:", fieldDef)
		valuePoolSize, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		for j := 0; j < valuePoolSize; j++ {
			// decode bytes to valuePools[fieldName]
			value, err := innerDecode(fieldDef, fieldName, stringPool, valuePools, reader, false)
			if err != nil {
				return nil, err
			}
			valuePools[fieldName] = append(valuePools[fieldName], value)
			fmt.Println("add", value, "into valuePools", fieldName)
		}
	}
	fmt.Println("decoding data")
	magic, err := reader.readString(6)
	if err != nil {
		return nil, err
	}
	if magic != "cprval" {
		return nil, errors.New("magic error")
	}
	result, err = innerDecode(def, "", stringPool, valuePools, reader, true)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// usePool 标记本身是否可以使用 valuePools
func innerDecode(def *model.Definition, myName string, stringPool []string, valuePools map[string][]model.Value, reader *DataReader, usePool bool) (model.Value, error) {
	var result model.Value
	// 池子里的是不带 null 标记的
	if def.Nullable && usePool {
		exist, err := reader.readBoolean()
		if err != nil {
			return nil, err
		}
		if !exist {
			// fmt.Println("get null")
			// fmt.Println(string(reader.data))
			return nil, nil
		}
	}
	switch def.Type {
	case model.Integer:
		intv, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		// fmt.Println("intv:", intv)
		result = &model.IntegerValue{Data: intv}
	case model.Boolean:
		boolv, err := reader.readBoolean()
		if err != nil {
			return nil, err
		}
		// fmt.Println("boolv:", boolv)
		result = &model.BooleanValue{Data: boolv}
	case model.Double:
		dbv, err := reader.readFloat()
		if err != nil {
			return nil, err
		}
		result = &model.DoubleValue{Data: dbv}
	case model.Bytes:
		if (def.Pooled || def.SharePooled) && usePool {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			index, err := reader.readLeb128Int()
			if err != nil {
				return nil, err
			}
			valuePool := valuePools[poolId]
			result = valuePool[index]
		} else {
			len, err := reader.readLeb128Int()
			if err != nil {
				return nil, err
			}
			bv, err := reader.readBytes(len)
			// fmt.Println("bv:", bv)
			result = &model.BytesValue{Data: bv}
		}
	case model.String:
		if (def.Pooled || def.SharePooled) && usePool {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			index, err := reader.readLeb128Int()
			if err != nil {
				return nil, err
			}
			valuePool := valuePools[poolId]
			result = valuePool[index]
		} else {
			len, err := reader.readLeb128Int()
			if err != nil {
				return nil, err
			}
			strv, err := reader.readString(len)
			// fmt.Println("strv:", strv)
			result = &model.StringValue{Data: strv}
		}
	case model.Object:
		if (def.Pooled || def.SharePooled) && usePool {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			index, err := reader.readLeb128Int()
			if err != nil {
				return nil, err
			}
			valuePool := valuePools[poolId]
			result = valuePool[index]
		} else {
			if def.Fields == nil {
				// 对 attributes 自由解码
				objv, err := innerFreeMapDecode(stringPool, reader)
				if err != nil {
					return nil, err
				}
				// fmt.Println("objv(free):", objv)
				result = &model.ObjectValue{Data: objv}
			} else {
				if len(myName) > 0 {
					myName = myName + " "
				}
				objv := make(map[string]model.Value)
				for _, fieldName := range getSortedKeys(def.Fields) {
					fieldValue, err := innerDecode(def.Fields[fieldName], myName+fieldName, stringPool, valuePools, reader, true)
					if err != nil {
						return nil, err
					}
					objv[fieldName] = fieldValue
				}
				// fmt.Println("objv:", objv)
				result = &model.ObjectValue{Data: objv}
			}
		}
	case model.Array:
		if (def.Pooled || def.SharePooled) && usePool {
			poolId := myName
			if def.SharePooled {
				poolId = def.SharePoolId
			}
			index, err := reader.readLeb128Int()
			if err != nil {
				return nil, err
			}
			valuePool := valuePools[poolId]
			result = valuePool[index]
		} else {
			length, err := reader.readLeb128Int()
			if err != nil {
				return nil, err
			}
			if len(myName) > 0 {
				myName = myName + " "
			}
			var arrv []model.Value
			for i := 0; i < length; i++ {
				item, err := innerDecode(def.ItemDefinition, myName+"item", stringPool, valuePools, reader, true)
				if err != nil {
					return nil, err
				}
				arrv = append(arrv, item)
			}
			// fmt.Println("arrv:", arrv)
			result = &model.ArrayValue{Data: arrv}
		}
	}
	return result, nil
}

func innerFreeMapDecode(stringPool []string, reader *DataReader) (map[string]model.Value, error) {
	result := make(map[string]model.Value)
	freeMapSize, err := reader.readLeb128Int()
	if err != nil {
		return nil, err
	}
	for i := 0; i < freeMapSize; i++ {
		keyIndex, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		key := stringPool[keyIndex]
		// 读取 null 标记位
		exist, err := reader.readBoolean()
		if err != nil {
			return nil, err
		}
		if !exist {
			result[key] = nil
		} else {
			value, err := innerFreeValueDecode(stringPool, reader)
			if err != nil {
				return nil, err
			}
			result[key] = value
		}
	}
	return result, nil
}

func innerFreeValueDecode(stringPool []string, reader *DataReader) (model.Value, error) {
	valueTypeInt, err := reader.readLeb128Int()
	if err != nil {
		return nil, err
	}
	valueType := model.ValueType(valueTypeInt)
	switch valueType {
	case model.Integer:
		intv, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		return &model.IntegerValue{Data: intv}, nil
	case model.Boolean:
		boolv, err := reader.readBoolean()
		if err != nil {
			return nil, err
		}
		return &model.BooleanValue{Data: boolv}, nil
	case model.Double:
		dbv, err := reader.readFloat()
		if err != nil {
			return nil, err
		}
		return &model.DoubleValue{Data: dbv}, nil
	case model.Bytes:
		len, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		bv, err := reader.readBytes(len)
		return &model.BytesValue{Data: bv}, nil
	case model.String:
		index, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		strv := stringPool[index]
		return &model.StringValue{Data: strv}, nil
	case model.Object:
		objv, err := innerFreeMapDecode(stringPool, reader)
		if err != nil {
			return nil, err
		}
		return &model.ObjectValue{Data: objv}, nil
	case model.Array:
		var arrv []model.Value
		len, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		for i := 0; i < len; i++ {
			value, err := innerFreeValueDecode(stringPool, reader)
			if err != nil {
				return nil, err
			}
			arrv = append(arrv, value)
		}
		return &model.ArrayValue{Data: arrv}, nil
	default:
		return nil, errors.New("unknown value type in free value: " + strconv.Itoa(valueTypeInt))
	}
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
