package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

type Definition struct {
	Type           ValueType
	Nullable       bool
	Pooled         bool                   // basic type like int, string will never be pooled, only use with Array and Object
	SharePooled    bool                   // share pool with other field
	SharePoolId    string                 // shared pool id
	DiffEncode     bool                   // for int, use difference with previous value of this field to encode
	Fields         map[string]*Definition // need Fields when Type is Object
	ItemDefinition *Definition            // need ItemDefinition when Type is Array
}

// TODO: 后续 definition 也要改成根据 JSON 配置生成
// var traceModel = &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
// 	"resourceSpans": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
// 		"resource": {Type: Object, Nullable: false, Pooled: true, Fields: map[string]*Definition{
// 			// 以属性维度多重池化信息
// 			"attributes":             {Type: Object, Nullable: true, Pooled: true},
// 			"droppedAttributesCount": {Type: Integer, Nullable: true},
// 		}},
// 		"scopeSpans": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
// 			"scope": {Type: Object, Nullable: false, Pooled: true, Fields: map[string]*Definition{
// 				"name":                   {Type: String, Nullable: true, Pooled: true},
// 				"version":                {Type: String, Nullable: true, Pooled: true},
// 				"attributes":             {Type: Object, Nullable: true, Pooled: true},
// 				"droppedAttributesCount": {Type: Integer, Nullable: true},
// 			}},
// 			"spans": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
// 				"traceId":                {Type: Bytes, Nullable: false, SharePooled: true, SharePoolId: "traceId"},
// 				"spanId":                 {Type: Bytes, Nullable: true, SharePooled: true, SharePoolId: "spanId"},
// 				"traceState":             {Type: String, Nullable: true, SharePooled: true, SharePoolId: "traceState"},
// 				"parentSpanId":           {Type: Bytes, Nullable: true, SharePooled: true, SharePoolId: "spanId"},
// 				"name":                   {Type: String, Nullable: false, Pooled: true},
// 				"kind":                   {Type: Integer, Nullable: true},
// 				"startTimeUnixNano":      {Type: Integer, Nullable: false, DiffEncode: true},
// 				"endTimeUnixNano":        {Type: Integer, Nullable: false, DiffEncode: true},
// 				"attributes":             {Type: Object, Nullable: true, Pooled: true},
// 				"droppedAttributesCount": {Type: Integer, Nullable: true},
// 				"events": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: false, Fields: map[string]*Definition{
// 					"timeUnixNano":           {Type: Integer, Nullable: true, DiffEncode: true},
// 					"name":                   {Type: String, Nullable: true, Pooled: true},
// 					"attributes":             {Type: Object, Nullable: true, Pooled: true},
// 					"droppedAttributesCount": {Type: Integer, Nullable: true},
// 				}}},
// 				"droppedEventsCount": {Type: Integer, Nullable: true},
// 				"links": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Type: Object, Nullable: false, Pooled: true, Fields: map[string]*Definition{
// 					"traceId":                {Type: Bytes, Nullable: false, SharePooled: true, SharePoolId: "traceId"},
// 					"spanId":                 {Type: Bytes, Nullable: true, SharePooled: true, SharePoolId: "spanId"},
// 					"traceState":             {Type: String, Nullable: true, SharePooled: true, SharePoolId: "traceState"},
// 					"attributes":             {Type: Object, Nullable: true, Pooled: true},
// 					"droppedAttributesCount": {Type: Integer, Nullable: true},
// 				}}},
// 				"droppedLinksCount": {Type: Integer, Nullable: true},
// 				"status": {Type: Object, Nullable: false, Pooled: true, Fields: map[string]*Definition{
// 					"message": {Type: String, Nullable: true, Pooled: true},
// 					"code":    {Type: Integer, Nullable: false},
// 				}},
// 			}}},
// 			"schemaUrl": {Type: String, Nullable: true, Pooled: true},
// 		}}},
// 		"schemaUrl": {Type: String, Nullable: true, Pooled: true},
// 	}}},
// }}

var traceModel *Definition

func GetDefinitionFromFile(path string) (*Definition, error) {
	byteValue, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading JSON file: %v", err)
	}

	var def Definition
	err = json.Unmarshal(byteValue, &def)
	if err != nil {
		return nil, fmt.Errorf("error parsing JSON to Definition: %v", err)
	}
	err = validateDefinition(&def)
	if err != nil {
		return nil, fmt.Errorf("error validating Definition: %v", err)
	}
	return &def, nil
}

func validateDefinition(def *Definition) error {
	sharedPool := make(map[string]*Definition)
	sharedPoolInPreviousPath := make(map[string]bool)

	return validateDfs(def, &sharedPool, &sharedPoolInPreviousPath)
}

func validateDfs(def *Definition, sharedPool *map[string]*Definition, sharedPoolInPreviousPath *map[string]bool) error {
	if def != nil {
		if def.SharePooled {
			if _, exist := (*sharedPoolInPreviousPath)[def.SharePoolId]; exist {
				return errors.New("existing illegal shared pool in same tree path -> SharedPoolId: " + def.SharePoolId)
			}
			(*sharedPoolInPreviousPath)[def.SharePoolId] = true
			if oldDef, exist := (*sharedPool)[def.SharePoolId]; exist {
				if !isEqual(oldDef, def) {
					return errors.New("existing different definitions sharing pool -> SharedPoolId: " + def.SharePoolId)
				}
			} else {
				(*sharedPool)[def.SharePoolId] = def
			}
		}
		if def.Type == Array {
			err := validateDfs(def.ItemDefinition, sharedPool, sharedPoolInPreviousPath)
			if err != nil {
				return err
			}
		}
		if def.Type == Object {
			for _, fieldDef := range def.Fields {
				err := validateDfs(fieldDef, sharedPool, sharedPoolInPreviousPath)
				if err != nil {
					return err
				}
			}
		}
		if def.SharePooled {
			delete(*sharedPoolInPreviousPath, def.SharePoolId)
		}
	}
	return nil
}

func isEqual(a *Definition, b *Definition) bool {
	if a == nil && b == nil {
		return true
	} else if a == nil || b == nil {
		return false
	} else {
		if a.DiffEncode != b.DiffEncode {
			return false
		}
		if a.Nullable != b.Nullable {
			return false
		}
		if a.Pooled != b.Pooled {
			return false
		}
		if a.SharePooled != b.SharePooled {
			return false
		}
		if a.SharePoolId != b.SharePoolId {
			return false
		}
		if a.Type != b.Type {
			return false
		}
		if a.Type == Object {
			if len(a.Fields) != len(b.Fields) {
				return false
			}
			for key, aVal := range a.Fields {
				bVal, exist := b.Fields[key]
				if !exist {
					return false
				}
				if !isEqual(aVal, bVal) {
					return false
				}
			}
		}
		if a.Type == Array {
			return isEqual(a.ItemDefinition, b.ItemDefinition)
		}
	}
	return true
}

var topologicalTraceModelFieldsLock sync.Mutex
var topologicalTraceModelFields = make([]string, 0)

func GetTraceModel() *Definition {
	if traceModel == nil {
		fmt.Println("get trace model from file...")
		tmp, err := GetDefinitionFromFile("./trace.json")
		if err != nil {
			log.Fatalln(err)
		}
		traceModel = tmp
	}
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
		if len(myName) > 0 {
			myName = myName + " "
		}
		switch definition.Type {
		case Object:
			for fieldName, fieldDef := range definition.Fields {
				result = dfs(fieldDef, myName+fieldName, result)
			}
		case Array:
			result = dfs(definition.ItemDefinition, myName+"item", result)
		}
		if len(myName) > 0 {
			myName = myName[:len(myName)-1]
		}
		if definition.Pooled {
			result = append(result, myName)
		}
		if definition.SharePooled {
			exist := false
			for _, name := range result {
				if name == definition.SharePoolId {
					exist = true
				}
			}
			if !exist {
				result = append(result, definition.SharePoolId)
			}
		}
	}
	return result
}

// 将定位 field 的 string 映射到 def 的实际子 Definition，如 "resourceSpans item resource attributes" 中间用一个空格
func FieldStringToDefinition(field string, def *Definition) *Definition {
	fieldPath := strings.Split(field, " ")
	currDef := def
	for i := 0; i < len(fieldPath); i++ {
		if fieldPath[i] == "item" {
			currDef = currDef.ItemDefinition
		} else {
			currDef = currDef.Fields[fieldPath[i]]
		}
	}
	if currDef == nil {
		// 找不到的应该是 sharedPoolId
		// dfs 寻找到第一个即可
		currDef = dfsSharedPoolId(field, def)
	}
	return currDef
}

func dfsSharedPoolId(sharedPoolId string, def *Definition) *Definition {
	if def != nil {
		if def.SharePooled && def.SharePoolId == sharedPoolId {
			return def
		}
		switch def.Type {
		case Object:
			for _, fieldDef := range def.Fields {
				result := dfsSharedPoolId(sharedPoolId, fieldDef)
				if result != nil {
					return result
				}
			}
		case Array:
			result := dfsSharedPoolId(sharedPoolId, def.ItemDefinition)
			if result != nil {
				return result
			}
		}
	}
	return nil
}
