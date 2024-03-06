package compressotelreceiver

import (
	"context"
	"fmt"
	"github.com/beet233/compressotelcollector/model"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"net/http"
	"strconv"

	"go.opentelemetry.io/collector/component"
)

type trace struct {
	config       *Config
	nextConsumer consumer.Traces
}

func (comp *trace) Start(ctx context.Context, host component.Host) error {
	// 开启一个 http 服务，接收压缩的 trace 数据，还原后传递给下一波
	// 处理函数
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			// body, err := io.ReadAll(r.Body)
			// if err != nil {
			// 	http.Error(w, "Error reading request body", http.StatusInternalServerError)
			// } else {
			// 	// fmt.Fprintf(w, "打打你的: %s", body)
			// 	fmt.Println("received raw data:")
			// 	fmt.Println(string(body))
			//
			// 	// comp.nextConsumer.ConsumeTraces()
			// }
			value, err := Decode(model.GetTraceModel(), r.Body)
			if err != nil {
				fmt.Println("error during decoding: ", err.Error())
				http.Error(w, "Error decoding request body", http.StatusInternalServerError)
			}
			fmt.Println(value)
			comp.nextConsumer.ConsumeTraces(ctx, valueToTraces(value))
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	fmt.Println("HTTP server listening on", comp.config.Port)
	// 指定端口
	if err := http.ListenAndServe(":"+strconv.Itoa(comp.config.Port), nil); err != nil {
		return err
	}
	return nil

}

func (comp *trace) Shutdown(ctx context.Context) error {
	return nil
}

func valueToTraces(value model.Value) ptrace.Traces {
	traces := ptrace.NewTraces()
	tracesVal := value.(*model.ObjectValue)
	if tracesVal.Data["resourceSpans"] != nil {
		resourceSpansVal := tracesVal.Data["resourceSpans"].(*model.ArrayValue)
		for _, a := range resourceSpansVal.Data {
			resourceSpanVal := a.(*model.ObjectValue)
			resourceSpan := traces.ResourceSpans().AppendEmpty()
			if resourceSpanVal.Data["resource"].(*model.ObjectValue).Data["attributes"] != nil {
				resourceSpan.Resource().Attributes().FromRaw(valueToMap(resourceSpanVal.Data["resource"].(*model.ObjectValue).Data["attributes"]))
			}
			if resourceSpanVal.Data["resource"].(*model.ObjectValue).Data["droppedAttributesCount"] != nil {
				resourceSpan.Resource().SetDroppedAttributesCount(uint32(resourceSpanVal.Data["resource"].(*model.ObjectValue).Data["droppedAttributesCount"].(*model.IntegerValue).Data))
			}
			if resourceSpanVal.Data["scopeSpans"] != nil {
				for _, b := range resourceSpanVal.Data["scopeSpans"].(*model.ArrayValue).Data {
					scopeSpanVal := b.(*model.ObjectValue)
					scopeSpan := resourceSpan.ScopeSpans().AppendEmpty()
					if scopeSpanVal.Data["scope"].(*model.ObjectValue).Data["name"] != nil {
						scopeSpan.Scope().SetName(scopeSpanVal.Data["scope"].(*model.ObjectValue).Data["name"].(*model.StringValue).Data)
					}
					if scopeSpanVal.Data["scope"].(*model.ObjectValue).Data["version"] != nil {
						scopeSpan.Scope().SetVersion(scopeSpanVal.Data["scope"].(*model.ObjectValue).Data["version"].(*model.StringValue).Data)
					}
					if scopeSpanVal.Data["scope"].(*model.ObjectValue).Data["attributes"] != nil {
						scopeSpan.Scope().Attributes().FromRaw(valueToMap(scopeSpanVal.Data["scope"].(*model.ObjectValue).Data["attributes"]))
					}
					if scopeSpanVal.Data["scope"].(*model.ObjectValue).Data["droppedAttributesCount"] != nil {
						scopeSpan.Scope().SetDroppedAttributesCount(uint32(scopeSpanVal.Data["scope"].(*model.ObjectValue).Data["droppedAttributesCount"].(*model.IntegerValue).Data))
					}
					for _, c := range scopeSpanVal.Data["spans"].(*model.ArrayValue).Data {
						spanVal := c.(*model.ObjectValue)
						span := scopeSpan.Spans().AppendEmpty()
						var traceId [16]byte
						copy(traceId[:], spanVal.Data["traceId"].(*model.BytesValue).Data)
						span.SetTraceID(traceId)
						if spanVal.Data["spanId"] != nil {
							var spanId [8]byte
							copy(spanId[:], spanVal.Data["spanId"].(*model.BytesValue).Data)
							span.SetSpanID(spanId)
						}
						if spanVal.Data["traceState"] != nil {
							span.TraceState().FromRaw(spanVal.Data["traceState"].(*model.StringValue).Data)
						}
						if spanVal.Data["parentSpanId"] != nil {
							var parentSpanId [8]byte
							copy(parentSpanId[:], spanVal.Data["parentSpanId"].(*model.BytesValue).Data)
							span.SetParentSpanID(parentSpanId)
						}
						span.SetName(spanVal.Data["name"].(*model.StringValue).Data)
						if spanVal.Data["kind"] != nil {
							span.SetKind(ptrace.SpanKind(spanVal.Data["kind"].(*model.IntegerValue).Data))
						}
						span.SetStartTimestamp(pcommon.Timestamp(spanVal.Data["startTimeUnixNano"].(*model.IntegerValue).Data))
						span.SetEndTimestamp(pcommon.Timestamp(spanVal.Data["endTimeUnixNano"].(*model.IntegerValue).Data))
						if spanVal.Data["attributes"] != nil {
							span.Attributes().FromRaw(valueToMap(spanVal.Data["attributes"]))
						}
						if spanVal.Data["droppedAttributesCount"] != nil {
							span.SetDroppedAttributesCount(uint32(spanVal.Data["droppedAttributesCount"].(*model.IntegerValue).Data))
						}
						if spanVal.Data["events"] != nil {
							for _, d := range spanVal.Data["events"].(*model.ArrayValue).Data {
								eventVal := d.(*model.ObjectValue)
								event := span.Events().AppendEmpty()
								if eventVal.Data["timeUnixNano"] != nil {
									event.SetTimestamp(pcommon.Timestamp(eventVal.Data["timeUnixNano"].(*model.IntegerValue).Data))
								}
								if eventVal.Data["name"] != nil {
									event.SetName(eventVal.Data["name"].(*model.StringValue).Data)
								}
								if eventVal.Data["attributes"] != nil {
									event.Attributes().FromRaw(valueToMap(eventVal.Data["attributes"]))
								}
								if eventVal.Data["droppedAttributesCount"] != nil {
									event.SetDroppedAttributesCount(uint32(eventVal.Data["droppedAttributesCount"].(*model.IntegerValue).Data))
								}
							}
						}
						if spanVal.Data["droppedEventsCount"] != nil {
							span.SetDroppedEventsCount(uint32(spanVal.Data["droppedEventsCount"].(*model.IntegerValue).Data))
						}
						if spanVal.Data["links"] != nil {
							for _, e := range spanVal.Data["links"].(*model.ArrayValue).Data {
								linkVal := e.(*model.ObjectValue)
								link := span.Links().AppendEmpty()
								var traceId [16]byte
								copy(traceId[:], linkVal.Data["traceId"].(*model.BytesValue).Data)
								link.SetTraceID(traceId)
								if linkVal.Data["spanId"] != nil {
									var spanId [8]byte
									copy(spanId[:], linkVal.Data["spanId"].(*model.BytesValue).Data)
									link.SetSpanID(spanId)
								}
								if linkVal.Data["traceState"] != nil {
									link.TraceState().FromRaw(linkVal.Data["traceState"].(*model.StringValue).Data)
								}
								if linkVal.Data["attributes"] != nil {
									link.Attributes().FromRaw(valueToMap(linkVal.Data["attributes"]))
								}
								if linkVal.Data["droppedAttributesCount"] != nil {
									link.SetDroppedAttributesCount(uint32(linkVal.Data["droppedAttributesCount"].(*model.IntegerValue).Data))
								}
							}
						}
						if spanVal.Data["droppedLinksCount"] != nil {
							span.SetDroppedLinksCount(uint32(spanVal.Data["droppedLinksCount"].(*model.IntegerValue).Data))
						}
						if spanVal.Data["status"].(*model.ObjectValue).Data["message"] != nil {
							span.Status().SetMessage(spanVal.Data["status"].(*model.ObjectValue).Data["message"].(*model.StringValue).Data)
						}
						span.Status().SetCode(ptrace.StatusCode(spanVal.Data["status"].(*model.ObjectValue).Data["code"].(*model.IntegerValue).Data))
					}
					if scopeSpanVal.Data["schemaUrl"] != nil {
						scopeSpan.SetSchemaUrl(scopeSpanVal.Data["schemaUrl"].(*model.StringValue).Data)
					}
				}
			}
			if resourceSpanVal.Data["schemaUrl"] != nil {
				resourceSpan.SetSchemaUrl(resourceSpanVal.Data["schemaUrl"].(*model.StringValue).Data)
			}
		}
	}
	return traces
}

func valueToMap(value model.Value) map[string]any {
	result := make(map[string]any)
	for key, item := range value.(*model.ObjectValue).Data {
		result[key] = valueToAny(item)
	}
	return result
}

func valueToArray(value model.Value) []any {
	var result []any
	for _, item := range value.(*model.ArrayValue).Data {
		result = append(result, valueToAny(item))
	}
	return result
}

func valueToAny(value model.Value) any {
	if value != nil {
		switch value.GetType() {
		case model.Integer:
			return value.(*model.IntegerValue).Data
		case model.Boolean:
			return value.(*model.BooleanValue).Data
		case model.Double:
			return value.(*model.DoubleValue).Data
		case model.Bytes:
			return value.(*model.BytesValue).Data
		case model.String:
			return value.(*model.StringValue).Data
		case model.Object:
			return valueToMap(value)
		case model.Array:
			return valueToArray(value)
		}
	}
	return nil
}
