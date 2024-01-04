package compressotelexporter

import (
	"context"
	"fmt"
	"github.com/beet233/compressotelcollector/model"
	"github.com/emirpasic/gods/maps/treemap"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"log"
)

// No default function for this. It must be implemented
// Note: You can change the function name if you like
func pushTraces(
	ctx context.Context,
	td ptrace.Traces,
) (err error) {
	// marshaler := ptrace.ProtoMarshaler{}
	marshaler := ptrace.JSONMarshaler{}
	buf, err := marshaler.MarshalTraces(td)
	if err != nil {
		return err
	}
	fmt.Println(string(buf))

	// 将 td 转化为 model.Value 形式
	tracesValue := tracesToValue(td)

	// TODO: 将 model.Value 形式的 td 数据完成字典编码，然后打印或保存到文件
	valuePools := make(map[string]*treemap.Map)
	stringPool := make(map[string]int)
	traceModel := model.GetTraceModel()
	var currentModel, parentModel *model.Definition
	currentModel = traceModel

	return nil
}

func tracesToValue(td ptrace.Traces) model.Value {
	tracesValue := model.ObjectValue{Data: map[string]model.Value{}}
	resourceSpansValue := model.ArrayValue{Data: []model.Value{}}
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpanValue := model.ObjectValue{Data: map[string]model.Value{}}
		resourceSpan := td.ResourceSpans().At(i)
		resourceValue := model.ObjectValue{Data: map[string]model.Value{}}
		resource := resourceSpan.Resource()
		resourceValue.Data["attributes"] = mapToValue(resource.Attributes().AsRaw())
		resourceValue.Data["droppedAttributesCount"] = model.IntegerValue{Data: int(resource.DroppedAttributesCount())}
		resourceSpanValue.Data["resource"] = resourceValue
		scopeSpansValue := model.ArrayValue{Data: []model.Value{}}
		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpanValue := model.ObjectValue{Data: map[string]model.Value{}}
			scopeSpan := resourceSpan.ScopeSpans().At(j)
			scopeValue := model.ObjectValue{Data: map[string]model.Value{}}
			scope := scopeSpan.Scope()
			scopeValue.Data["name"] = model.StringValue{Data: scope.Name()}
			scopeValue.Data["version"] = model.StringValue{Data: scope.Version()}
			scopeValue.Data["attributes"] = mapToValue(scope.Attributes().AsRaw())
			scopeValue.Data["droppedAttributesCount"] = model.IntegerValue{Data: int(scope.DroppedAttributesCount())}
			scopeSpanValue.Data["scope"] = scopeValue
			spansValue := model.ArrayValue{Data: []model.Value{}}
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				spanValue := model.ObjectValue{Data: map[string]model.Value{}}
				span := scopeSpan.Spans().At(k)
				spanValue.Data["traceId"] = model.StringValue{Data: span.TraceID().String()}
				spanValue.Data["spanId"] = model.StringValue{Data: span.SpanID().String()}
				spanValue.Data["traceState"] = model.StringValue{Data: span.TraceState().AsRaw()}
				spanValue.Data["parentSpanId"] = model.StringValue{Data: span.ParentSpanID().String()}
				spanValue.Data["name"] = model.StringValue{Data: span.Name()}
				spanValue.Data["kind"] = model.IntegerValue{Data: int(span.Kind())}
				spanValue.Data["startTimeUnixNano"] = model.IntegerValue{Data: int(span.StartTimestamp().AsTime().UnixNano())}
				spanValue.Data["endTimeUnixNano"] = model.IntegerValue{Data: int(span.EndTimestamp().AsTime().UnixNano())}
				spanValue.Data["attributes"] = mapToValue(span.Attributes().AsRaw())
				spanValue.Data["droppedAttributesCount"] = model.IntegerValue{Data: int(span.DroppedAttributesCount())}
				eventsValue := model.ArrayValue{Data: []model.Value{}}
				for m := 0; m < span.Events().Len(); m++ {
					eventValue := model.ObjectValue{Data: map[string]model.Value{}}
					event := span.Events().At(m)
					eventValue.Data["timeUnixNano"] = model.IntegerValue{Data: int(event.Timestamp().AsTime().UnixNano())}
					eventValue.Data["name"] = model.StringValue{Data: event.Name()}
					eventValue.Data["attributes"] = mapToValue(event.Attributes().AsRaw())
					eventValue.Data["droppedAttributesCount"] = model.IntegerValue{Data: int(event.DroppedAttributesCount())}
					eventsValue.Data = append(eventsValue.Data, eventValue)
				}
				spanValue.Data["events"] = eventsValue
				spanValue.Data["droppedEventsCount"] = model.IntegerValue{Data: int(span.DroppedEventsCount())}
				linksValue := model.ArrayValue{Data: []model.Value{}}
				for m := 0; m < span.Links().Len(); m++ {
					linkValue := model.ObjectValue{Data: map[string]model.Value{}}
					link := span.Links().At(m)
					linkValue.Data["traceId"] = model.StringValue{Data: link.TraceID().String()}
					linkValue.Data["spanId"] = model.StringValue{Data: link.SpanID().String()}
					linkValue.Data["traceState"] = model.StringValue{Data: link.TraceState().AsRaw()}
					linkValue.Data["attributes"] = mapToValue(link.Attributes().AsRaw())
					linkValue.Data["droppedAttributesCount"] = model.IntegerValue{Data: int(link.DroppedAttributesCount())}
					linksValue.Data = append(linksValue.Data, linkValue)
				}
				spanValue.Data["links"] = linksValue
				spanValue.Data["droppedLinksCount"] = model.IntegerValue{Data: int(span.DroppedLinksCount())}
				statusValue := model.ObjectValue{Data: map[string]model.Value{}}
				status := span.Status()
				statusValue.Data["message"] = model.StringValue{Data: status.Message()}
				statusValue.Data["code"] = model.IntegerValue{Data: int(status.Code())}
				spanValue.Data["status"] = statusValue
				spansValue.Data = append(spansValue.Data, spanValue)
			}
			scopeSpanValue.Data["spans"] = spansValue
			scopeSpanValue.Data["schemaUrl"] = model.StringValue{Data: scopeSpan.SchemaUrl()}
			scopeSpansValue.Data = append(scopeSpansValue.Data, scopeSpanValue)
		}
		resourceSpanValue.Data["scopeSpans"] = scopeSpansValue
		resourceSpanValue.Data["schemaUrl"] = model.StringValue{Data: resourceSpan.SchemaUrl()}
		resourceSpansValue.Data = append(resourceSpansValue.Data, resourceSpanValue)
	}
	tracesValue.Data["resourceSpans"] = resourceSpansValue
	return tracesValue
}

func mapToValue(m map[string]any) model.Value {
	result := model.ObjectValue{Data: map[string]model.Value{}}
	for key, value := range m {
		result.Data[key] = anyToValue(value)
	}
	return result
}

func arrayToValue(a []any) model.Value {
	result := model.ArrayValue{Data: []model.Value{}}
	for _, value := range a {
		result.Data = append(result.Data, anyToValue(value))
	}
	return result
}

func anyToValue(a any) model.Value {
	var myValue model.Value
	switch a.(type) {
	case nil:
		myValue = nil
	case string:
		myValue = model.StringValue{Data: a.(string)}
	case bool:
		myValue = model.BooleanValue{Data: a.(bool)}
	case float64:
		myValue = model.DoubleValue{Data: a.(float64)}
	case int64:
		myValue = model.IntegerValue{Data: a.(int)}
	case []byte:
		myValue = model.BytesValue{Data: a.([]byte)}
	case map[string]any:
		myValue = mapToValue(a.(map[string]any))
	case []any:
		myValue = arrayToValue(a.([]any))
	default:
		log.Fatalln("Unknown value: ", a)
	}
	return myValue
}
