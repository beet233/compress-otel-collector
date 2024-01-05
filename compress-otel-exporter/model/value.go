package model

import (
	"log"
	"strings"
)

// ValueType 是属性节点可能存储的值的枚举类型
type ValueType int

const (
	Integer ValueType = iota
	String
	Boolean
	Double
	Bytes
	Object
	Array
)

// Value 是属性节点可能存储的值的接口类型
type Value interface {
	GetType() ValueType // 获取值的类型
}

// IntegerValue 是整数值类型
type IntegerValue struct {
	Data int
}

func (iv *IntegerValue) GetType() ValueType {
	return Integer
}

// StringValue 是字符串值类型
type StringValue struct {
	Data string
}

func (sv *StringValue) GetType() ValueType {
	return String
}

// BooleanValue 是字符串值类型
type BooleanValue struct {
	Data bool
}

func (bv *BooleanValue) GetType() ValueType {
	return Boolean
}

// DoubleValue 是字符串值类型
type DoubleValue struct {
	Data float64
}

func (dv *DoubleValue) GetType() ValueType {
	return Double
}

// BytesValue 是字符串值类型
type BytesValue struct {
	Data []byte
}

func (bv *BytesValue) GetType() ValueType {
	return Bytes
}

// ObjectValue 是对象值类型
type ObjectValue struct {
	Data map[string]Value
}

func (ov *ObjectValue) GetType() ValueType {
	return Object
}

// ArrayValue 是对象数组值类型
type ArrayValue struct {
	Data []Value
}

func (av *ArrayValue) GetType() ValueType {
	return Array
}

func ValueComparator(a, b interface{}) int {

	// panic if type error
	v1 := a.(Value)
	v2 := b.(Value)

	type1 := v1.GetType()
	type2 := v2.GetType()

	if type1 != type2 {
		return int(type1 - type2)
	}

	switch type1 {
	case Integer:
		intv1 := v1.(*IntegerValue)
		intv2 := v2.(*IntegerValue)
		return intv1.Data - intv2.Data
	case String:
		strv1 := v1.(*StringValue)
		strv2 := v2.(*StringValue)
		return strings.Compare(strv1.Data, strv2.Data)
	case Boolean:
		boolv1 := v1.(*BooleanValue)
		boolv2 := v2.(*BooleanValue)
		if boolv1.Data == boolv2.Data {
			return 0
		} else if boolv1.Data {
			return 1
		} else {
			return -1
		}
	case Double:
		dbv1 := v1.(*DoubleValue)
		dbv2 := v2.(*DoubleValue)
		if dbv1.Data > dbv2.Data {
			return 1
		} else if dbv1.Data == dbv2.Data {
			return 0
		} else {
			return -1
		}
	case Bytes:
		bv1 := v1.(*BytesValue)
		bv2 := v2.(*BytesValue)
		if len(bv1.Data) != len(bv2.Data) {
			return len(bv1.Data) - len(bv2.Data)
		}
		for i := 0; i < len(bv1.Data); i++ {
			comp := int(bv1.Data[i] - bv2.Data[i])
			if comp != 0 {
				return comp
			}
		}
		return 0
	case Object:
		objv1 := v1.(*ObjectValue)
		objv2 := v2.(*ObjectValue)
		if len(objv1.Data) != len(objv2.Data) {
			return len(objv1.Data) - len(objv2.Data)
		}
		for key, value1 := range objv1.Data {
			if value2, exist := objv2.Data[key]; exist {
				comp := ValueComparator(value1, value2)
				if comp != 0 {
					return comp
				}
			} else {
				return -1
			}
		}
		return 0
	case Array:
		av1 := v1.(*ArrayValue)
		av2 := v2.(*ArrayValue)
		if len(av1.Data) != len(av2.Data) {
			return len(av1.Data) - len(av2.Data)
		}
		for i := 0; i < len(av1.Data); i++ {
			comp := ValueComparator(av1.Data[i], av2.Data[i])
			if comp != 0 {
				return comp
			}
		}
		return 0
	default:
		log.Fatalln("Unknown type: ", type1)
		return 0
	}
}

func mapToValue(m map[string]any) Value {
	result := &ObjectValue{Data: map[string]Value{}}
	for key, value := range m {
		result.Data[key] = AnyToValue(value)
	}
	return result
}

func arrayToValue(a []any) Value {
	result := &ArrayValue{Data: []Value{}}
	for _, value := range a {
		result.Data = append(result.Data, AnyToValue(value))
	}
	return result
}

func AnyToValue(a any) Value {
	var myValue Value
	switch a.(type) {
	case nil:
		myValue = nil
	case string:
		myValue = &StringValue{Data: a.(string)}
	case bool:
		myValue = &BooleanValue{Data: a.(bool)}
	case float64:
		myValue = &DoubleValue{Data: a.(float64)}
	case int64:
		myValue = &IntegerValue{Data: a.(int)}
	case []byte:
		myValue = &BytesValue{Data: a.([]byte)}
	case map[string]any:
		myValue = mapToValue(a.(map[string]any))
	case []any:
		myValue = arrayToValue(a.([]any))
	default:
		log.Fatalln("Unknown value: ", a)
	}
	return myValue
}
