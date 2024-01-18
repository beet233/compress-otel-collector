package model

import (
	"fmt"
	"strings"
	"sync"
)

type Definition struct {
	Type           ValueType
	Nullable       bool
	Pooled         bool                   // basic type like int, string will never be pooled, only use with Array and Object
	Fields         map[string]*Definition // need Fields when Type is Object
	ItemDefinition *Definition            // need ItemDefinition when Type is Array
}

// TODO: 后续 definition 也要改成根据配置生成
var traceModel = &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
	"resourceSpans": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
		"resource": {Type: Object, Nullable: false, Pooled: true, Fields: map[string]*Definition{
			// 以属性维度多重池化信息
			"attributes":             {Type: Object, Nullable: true, Pooled: true},
			"droppedAttributesCount": {Type: Integer, Nullable: true},
		}},
		"scopeSpans": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
			"scope": {Type: Object, Nullable: false, Pooled: true, Fields: map[string]*Definition{
				"name":                   {Type: String, Nullable: true},
				"version":                {Type: String, Nullable: true},
				"attributes":             {Type: Object, Nullable: true, Pooled: true},
				"droppedAttributesCount": {Type: Integer, Nullable: true},
			}},
			"spans": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
				"traceId":                {Type: String, Nullable: false},
				"spanId":                 {Type: String, Nullable: false},
				"traceState":             {Type: String, Nullable: true},
				"parentSpanId":           {Type: String, Nullable: true},
				"name":                   {Type: String, Nullable: false},
				"kind":                   {Type: Integer, Nullable: true},
				"startTimeUnixNano":      {Type: Integer, Nullable: false},
				"endTimeUnixNano":        {Type: Integer, Nullable: false},
				"attributes":             {Type: Object, Nullable: true, Pooled: true},
				"droppedAttributesCount": {Type: Integer, Nullable: true},
				"events": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
					"timeUnixNano":           {Type: Integer, Nullable: true},
					"name":                   {Type: String, Nullable: true},
					"attributes":             {Type: Object, Nullable: true, Pooled: true},
					"droppedAttributesCount": {Type: Integer, Nullable: true},
				}}},
				"droppedEventsCount": {Type: Integer, Nullable: true},
				"links": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: true, Fields: map[string]*Definition{
					"traceId":                {Type: String, Nullable: false},
					"spanId":                 {Type: String, Nullable: false},
					"traceState":             {Type: String, Nullable: true},
					"attributes":             {Type: Object, Nullable: true, Pooled: true},
					"droppedAttributesCount": {Type: Integer, Nullable: true},
				}}},
				"droppedLinksCount": {Type: Integer, Nullable: true},
				"status": {Type: Object, Nullable: false, Pooled: true, Fields: map[string]*Definition{
					"message": {Type: String, Nullable: true},
					"code":    {Type: Integer, Nullable: false},
				}},
			}}},
			"schemaUrl": {Type: String, Nullable: true},
		}}},
		"schemaUrl": {Type: String, Nullable: true},
	}}},
}}

var topologicalTraceModelFieldsLock sync.Mutex
var topologicalTraceModelFields = make([]string, 0)

func GetTraceModel() *Definition {
	return traceModel
}

func GetTopologicalTraceModelFields() []string {
	// 获取锁
	for !topologicalTraceModelFieldsLock.TryLock() {
	}
	// 如果还没初始化
	if len(topologicalTraceModelFields) == 0 {
		// 初始化模型的 fields 拓扑顺序
		topologicalTraceModelFields = getTopologicalFieldsByDefinition(GetTraceModel())
	}
	// 释放锁
	topologicalTraceModelFieldsLock.Unlock()
	return topologicalTraceModelFields
}

// 根据 definition，将所有 fields 以编码的拓扑顺序返回
// 但整个 definition 已经是拓扑的树形结构，其实只需要一个 dfs
func getTopologicalFieldsByDefinition(definition *Definition) []string {
	result := make([]string, 0)
	result = dfs(definition, "", result)
	return result
}

// string 无序处理，序列化时最优先序列化 stringPool
func dfs(definition *Definition, myName string, result []string) []string {
	if definition != nil {
		switch definition.Type {
		case Object:
			if len(myName) > 0 {
				myName = myName + " "
			}
			for fieldName, fieldDef := range definition.Fields {
				result = dfs(fieldDef, myName+fieldName, result)
			}
			if len(myName) > 0 {
				myName = myName[:len(myName)-1]
			}
			if definition.Pooled {
				result = append(result, myName)
			}
		case Array:
			if len(myName) > 0 {
				myName = myName + " "
			}
			result = dfs(definition.ItemDefinition, myName+"item", result)
			if len(myName) > 0 {
				myName = myName[:len(myName)-1]
			}
			if definition.Pooled {
				result = append(result, myName)
			}
		}
	}
	return result
}

// 目前没用
// 将定位 field 的 string 映射到 def 的实际子 Definition，如 "resourceSpans item resource attributes" 中间用一个空格
func FieldStringToDefinition(field string, def *Definition) *Definition {
	fieldPath := strings.Split(field, " ")
	currDef := def
	for i := 0; i < len(fieldPath); i++ {
		if fieldPath[i] == "item" {
			currDef = def.ItemDefinition
		} else {
			currDef = def.Fields[fieldPath[i]]
		}
	}
	if currDef == nil {
		fmt.Println(field)
		fmt.Println(fieldPath)
	}
	return currDef
}
