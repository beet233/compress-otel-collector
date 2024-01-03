package model

type Definition struct {
	Type           ValueType
	Nullable       bool
	Pooled         bool                  // basic type like int, string will never be pooled, only use with Array and Object
	Fields         map[string]Definition // need Fields when Type is Object
	ItemDefinition *Definition           // need ItemDefinition when Type is Array
}

func GetTraceModel() Definition {
	return Definition{Fields: map[string]Definition{
		"resourceSpans": {Type: Array, Nullable: false, Pooled: false, ItemDefinition: &Definition{Fields: map[string]Definition{
			"resource": {Type: Object, Nullable: false, Pooled: true, Fields: map[string]Definition{
				// 以属性维度多重池化信息
				"attributes":             {Type: Object, Nullable: true, Pooled: true},
				"droppedAttributesCount": {Type: Integer, Nullable: true},
			}},
			"scopeSpans": {Type: Array, Nullable: false, Pooled: false, ItemDefinition: &Definition{Fields: map[string]Definition{
				"scope": {Type: Object, Nullable: false, Pooled: true, Fields: map[string]Definition{
					"name":                   {Type: String, Nullable: true},
					"version":                {Type: String, Nullable: true},
					"attributes":             {Type: Object, Nullable: true, Pooled: true},
					"droppedAttributesCount": {Type: Integer, Nullable: true},
				}},
				"spans": {Type: Array, Nullable: false, Pooled: false, ItemDefinition: &Definition{Fields: map[string]Definition{
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
					"events": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Fields: map[string]Definition{
						"timeUnixNano":           {Type: Integer, Nullable: true},
						"name":                   {Type: String, Nullable: true},
						"attributes":             {Type: Object, Nullable: true, Pooled: true},
						"droppedAttributesCount": {Type: Integer, Nullable: true},
					}}},
					"droppedEventsCount": {Type: Integer, Nullable: true},
					"links": {Type: Array, Nullable: true, Pooled: false, ItemDefinition: &Definition{Fields: map[string]Definition{
						"traceId":                {Type: String, Nullable: false},
						"spanId":                 {Type: String, Nullable: false},
						"traceState":             {Type: String, Nullable: true},
						"attributes":             {Type: Object, Nullable: true, Pooled: true},
						"droppedAttributesCount": {Type: Integer, Nullable: true},
					}}},
					"droppedLinksCount": {Type: Integer, Nullable: true},
					"status": {Type: Object, Nullable: false, Pooled: true, Fields: map[string]Definition{
						"message": {Type: String, Nullable: true},
						"code":    {Type: Integer, Nullable: false},
					}},
				}}},
				"schemaUrl": {Type: String, Nullable: true},
			}}},
			"schemaUrl": {Type: String, Nullable: true},
		}}},
	}}
}
