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
	// TODO 这里定义了个 Bytes 而不是 Byte 是模仿 otel 里的 AnyValue 的，有待考虑
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

func (iv IntegerValue) GetType() ValueType {
	return Integer
}

// StringValue 是字符串值类型
type StringValue struct {
	Data string
}

func (sv StringValue) GetType() ValueType {
	return String
}

// ObjectValue 是对象值类型
type ObjectValue struct {
	Data map[string]Value
}

func (ov ObjectValue) GetType() ValueType {
	return Object
}

// ArrayValue 是对象数组值类型
type ArrayValue struct {
	Data []Value
}

func (av ArrayValue) GetType() ValueType {
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
		intv1 := v1.(IntegerValue)
		intv2 := v2.(IntegerValue)
		return intv1.Data - intv2.Data
	case String:
		strv1 := v1.(StringValue)
		strv2 := v2.(StringValue)
		return strings.Compare(strv1.Data, strv2.Data)
	case Object:
		objv1 := v1.(ObjectValue)
		objv2 := v2.(ObjectValue)
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
		av1 := v1.(ArrayValue)
		av2 := v2.(ArrayValue)
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
