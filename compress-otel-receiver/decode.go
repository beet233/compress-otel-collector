package compressotelreceiver

import (
	"fmt"
	"github.com/beet233/compressotelcollector/model"
	"io"
	"sort"
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
		fmt.Println(fieldName, fieldDef)
		valuePoolSize, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		for j := 0; j < valuePoolSize; j++ {
			// decode bytes to valuePools[fieldName]
			value, err := innerDecode(fieldDef, "", stringPool, valuePools, reader)
			if err != nil {
				return nil, err
			}
			valuePools[fieldName] = append(valuePools[fieldName], value)
		}
	}
	result, err = innerDecode(def, "", stringPool, valuePools, reader)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func innerDecode(def *model.Definition, myName string, stringPool []string, valuePools map[string][]model.Value, reader *DataReader) (model.Value, error) {
	var result model.Value
	if def.Nullable {
		isNull, err := reader.readBoolean()
		if err != nil {
			return nil, err
		}
		if isNull {
			return nil, nil
		}
	}
	switch def.Type {
	case model.Integer:
		intv, err := reader.readLeb128Int()
		if err != nil {
			return nil, err
		}
		result = &model.IntegerValue{Data: intv}
	case model.Boolean:
		boolv, err := reader.readBoolean()
		if err != nil {
			return nil, err
		}
		result = &model.BooleanValue{Data: boolv}
	case model.Double:
		dbv, err := reader.readFloat()
		if err != nil {
			return nil, err
		}
		result = &model.DoubleValue{Data: dbv}
	case model.Bytes:
		if def.Pooled || def.SharePooled {
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
			result = &model.BytesValue{Data: bv}
		}
	case model.String:
		if def.Pooled || def.SharePooled {
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
			result = &model.StringValue{Data: strv}
		}
	case model.Object:
		if def.Pooled || def.SharePooled {
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
				// TODO 自由解码
			} else {
				if len(myName) > 0 {
					myName = myName + " "
				}
				objv := make(map[string]model.Value)
				for _, fieldName := range getSortedKeys(def.Fields) {
					fieldValue, err := innerDecode(def.Fields[fieldName], myName+fieldName, stringPool, valuePools, reader)
					if err != nil {
						return nil, err
					}
					objv[fieldName] = fieldValue
				}
				result = &model.ObjectValue{Data: objv}
			}
		}
	case model.Array:
		if def.Pooled || def.SharePooled {
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
				item, err := innerDecode(def.ItemDefinition, myName+"item", stringPool, valuePools, reader)
				if err != nil {
					return nil, err
				}
				arrv = append(arrv, item)
			}
			result = &model.ArrayValue{Data: arrv}
		}
	}
	return result, nil
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
