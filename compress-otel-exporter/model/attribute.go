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
	Object
	ObjectArray
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

// ObjectArrayValue 是对象数组值类型
type ObjectArrayValue struct {
	Data []ObjectValue
}

func (oav ObjectArrayValue) GetType() ValueType {
	return ObjectArray
}

// Attribute 定义属性节点结构体
// type Attribute struct {
// 	Name  string // 属性名
// 	Value Value  // 属性值
// }

func ObjectValueComparator(objv1, objv2 ObjectValue) int {
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
		return ObjectValueComparator(objv1, objv2)
	case ObjectArray:
		objav1 := v1.(ObjectArrayValue)
		objav2 := v2.(ObjectArrayValue)
		if len(objav1.Data) != len(objav2.Data) {
			return len(objav1.Data) - len(objav2.Data)
		}
		for i := 0; i < len(objav1.Data); i++ {
			comp := ObjectValueComparator(objav1.Data[i], objav2.Data[i])
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
