package compressotelexporter

import (
	"bytes"
	"context"
	"github.com/beet233/compressotelcollector/model"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"os"
	"strconv"
	"time"
)

// No default function for this. It must be implemented
// Note: You can change the function name if you like
func pushTraces(
	ctx context.Context,
	td ptrace.Traces,
) (err error) {

	// 创建一个 zstd writer.
	var zstdBuf1 bytes.Buffer
	zw1, err := zstd.NewWriter(&zstdBuf1)
	if err != nil {
		return err
	}

	// 创建一个 zstd writer.
	var gzipBuf1 bytes.Buffer
	gz1 := gzip.NewWriter(&gzipBuf1)
	if err != nil {
		return err
	}

	protoMarshaler := ptrace.ProtoMarshaler{}
	buf, err := protoMarshaler.MarshalTraces(td)
	if err != nil {
		return err
	}

	fileProto, err := os.Create(strconv.FormatInt(time.Now().UnixNano(), 10) + "_out_proto")
	_, err = fileProto.Write(buf)
	if err != nil {
		return err
	}

	_, err = zw1.Write(buf)
	if err != nil {
		return err
	}

	// 关闭 writer 用以完成压缩.
	err = zw1.Close()
	if err != nil {
		return err
	}

	fileProtoZstd, err := os.Create(strconv.FormatInt(time.Now().UnixNano(), 10) + "_out_proto_zstd")
	_, err = fileProtoZstd.Write(zstdBuf1.Bytes())
	if err != nil {
		return err
	}

	_, err = gz1.Write(buf)
	if err != nil {
		return err
	}

	// 关闭 writer 用以完成压缩.
	err = gz1.Close()
	if err != nil {
		return err
	}

	fileProtoGzip, err := os.Create(strconv.FormatInt(time.Now().UnixNano(), 10) + "_out_proto_gzip")
	_, err = fileProtoGzip.Write(gzipBuf1.Bytes())
	if err != nil {
		return err
	}

	jsonMarshaler := ptrace.JSONMarshaler{}
	buf, err = jsonMarshaler.MarshalTraces(td)
	if err != nil {
		return err
	}

	fileJSON, err := os.Create(strconv.FormatInt(time.Now().UnixNano(), 10) + "_out_json")
	_, err = fileJSON.Write(buf)
	if err != nil {
		return err
	}
	// fmt.Println(string(buf))

	// 将 td 转化为 model.Value 形式
	tracesValue := tracesToValue(td)
	if err != nil {
		return err
	}

	// 创建一个 zstd writer.
	var zstdBuf2 bytes.Buffer
	zw2, err := zstd.NewWriter(&zstdBuf2)
	if err != nil {
		return err
	}

	// 将 model.Value 形式的 td 数据完成字典编码，然后打印或保存到文件
	fileZstd, err := os.Create(strconv.FormatInt(time.Now().UnixNano(), 10) + "_out_zstd")
	var tempBuf bytes.Buffer
	err = Encode(tracesValue, model.GetTraceModel(), &tempBuf)
	if err != nil {
		return err
	}

	_, err = zw2.Write(tempBuf.Bytes())
	if err != nil {
		return err
	}

	// 关闭 writer 用以完成压缩.
	err = zw2.Close()
	if err != nil {
		return err
	}

	_, err = fileZstd.Write(zstdBuf2.Bytes())
	if err != nil {
		return err
	}

	// 创建一个 zstd writer.
	var gzipBuf2 bytes.Buffer
	gz2 := gzip.NewWriter(&gzipBuf2)
	if err != nil {
		return err
	}

	// 将 model.Value 形式的 td 数据完成字典编码，然后打印或保存到文件
	fileGzip, err := os.Create(strconv.FormatInt(time.Now().UnixNano(), 10) + "_out_gzip")
	var tempBuf2 bytes.Buffer
	err = Encode(tracesValue, model.GetTraceModel(), &tempBuf2)
	if err != nil {
		return err
	}

	_, err = gz2.Write(tempBuf2.Bytes())
	if err != nil {
		return err
	}

	// 关闭 writer 用以完成压缩.
	err = gz2.Close()
	if err != nil {
		return err
	}

	_, err = fileGzip.Write(gzipBuf2.Bytes())
	if err != nil {
		return err
	}

	// 将 model.Value 形式的 td 数据完成字典编码，然后打印或保存到文件
	file, err := os.Create(strconv.FormatInt(time.Now().UnixNano(), 10) + "_out")
	err = Encode(tracesValue, model.GetTraceModel(), file)
	if err != nil {
		return err
	}

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
		resourceValue.Data["attributes"] = model.AnyToValue(resource.Attributes().AsRaw())
		resourceValue.Data["droppedAttributesCount"] = &model.IntegerValue{Data: int(resource.DroppedAttributesCount())}
		resourceSpanValue.Data["resource"] = &resourceValue
		scopeSpansValue := model.ArrayValue{Data: []model.Value{}}
		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpanValue := model.ObjectValue{Data: map[string]model.Value{}}
			scopeSpan := resourceSpan.ScopeSpans().At(j)
			scopeValue := model.ObjectValue{Data: map[string]model.Value{}}
			scope := scopeSpan.Scope()
			scopeValue.Data["name"] = &model.StringValue{Data: scope.Name()}
			scopeValue.Data["version"] = &model.StringValue{Data: scope.Version()}
			scopeValue.Data["attributes"] = model.AnyToValue(scope.Attributes().AsRaw())
			scopeValue.Data["droppedAttributesCount"] = &model.IntegerValue{Data: int(scope.DroppedAttributesCount())}
			scopeSpanValue.Data["scope"] = &scopeValue
			spansValue := model.ArrayValue{Data: []model.Value{}}
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				spanValue := model.ObjectValue{Data: map[string]model.Value{}}
				span := scopeSpan.Spans().At(k)
				traceId := span.TraceID()
				traceIdBytes := traceId[:]
				spanValue.Data["traceId"] = &model.BytesValue{Data: traceIdBytes}
				spanId := span.SpanID()
				spanIdBytes := spanId[:]
				spanValue.Data["spanId"] = &model.BytesValue{Data: spanIdBytes}
				spanValue.Data["traceState"] = &model.StringValue{Data: span.TraceState().AsRaw()}
				parentSpanId := span.ParentSpanID()
				parentSpanIdBytes := parentSpanId[:]
				spanValue.Data["parentSpanId"] = &model.BytesValue{Data: parentSpanIdBytes}
				spanValue.Data["name"] = &model.StringValue{Data: span.Name()}
				spanValue.Data["kind"] = &model.IntegerValue{Data: int(span.Kind())}
				spanValue.Data["startTimeUnixNano"] = &model.IntegerValue{Data: int(span.StartTimestamp().AsTime().UnixNano())}
				spanValue.Data["endTimeUnixNano"] = &model.IntegerValue{Data: int(span.EndTimestamp().AsTime().UnixNano())}
				spanValue.Data["attributes"] = model.AnyToValue(span.Attributes().AsRaw())
				spanValue.Data["droppedAttributesCount"] = &model.IntegerValue{Data: int(span.DroppedAttributesCount())}
				eventsValue := model.ArrayValue{Data: []model.Value{}}
				for m := 0; m < span.Events().Len(); m++ {
					eventValue := model.ObjectValue{Data: map[string]model.Value{}}
					event := span.Events().At(m)
					eventValue.Data["timeUnixNano"] = &model.IntegerValue{Data: int(event.Timestamp().AsTime().UnixNano())}
					eventValue.Data["name"] = &model.StringValue{Data: event.Name()}
					eventValue.Data["attributes"] = model.AnyToValue(event.Attributes().AsRaw())
					eventValue.Data["droppedAttributesCount"] = &model.IntegerValue{Data: int(event.DroppedAttributesCount())}
					eventsValue.Data = append(eventsValue.Data, &eventValue)
				}
				spanValue.Data["events"] = &eventsValue
				spanValue.Data["droppedEventsCount"] = &model.IntegerValue{Data: int(span.DroppedEventsCount())}
				linksValue := model.ArrayValue{Data: []model.Value{}}
				for m := 0; m < span.Links().Len(); m++ {
					linkValue := model.ObjectValue{Data: map[string]model.Value{}}
					link := span.Links().At(m)
					traceId := link.TraceID()
					traceIdBytes := traceId[:]
					linkValue.Data["traceId"] = &model.BytesValue{Data: traceIdBytes}
					spanId := link.SpanID()
					spanIdBytes := spanId[:]
					linkValue.Data["spanId"] = &model.BytesValue{Data: spanIdBytes}
					linkValue.Data["traceState"] = &model.StringValue{Data: link.TraceState().AsRaw()}
					linkValue.Data["attributes"] = model.AnyToValue(link.Attributes().AsRaw())
					linkValue.Data["droppedAttributesCount"] = &model.IntegerValue{Data: int(link.DroppedAttributesCount())}
					linksValue.Data = append(linksValue.Data, &linkValue)
				}
				spanValue.Data["links"] = &linksValue
				spanValue.Data["droppedLinksCount"] = &model.IntegerValue{Data: int(span.DroppedLinksCount())}
				statusValue := model.ObjectValue{Data: map[string]model.Value{}}
				status := span.Status()
				statusValue.Data["message"] = &model.StringValue{Data: status.Message()}
				statusValue.Data["code"] = &model.IntegerValue{Data: int(status.Code())}
				spanValue.Data["status"] = &statusValue
				spansValue.Data = append(spansValue.Data, &spanValue)
			}
			scopeSpanValue.Data["spans"] = &spansValue
			scopeSpanValue.Data["schemaUrl"] = &model.StringValue{Data: scopeSpan.SchemaUrl()}
			scopeSpansValue.Data = append(scopeSpansValue.Data, &scopeSpanValue)
		}
		resourceSpanValue.Data["scopeSpans"] = &scopeSpansValue
		resourceSpanValue.Data["schemaUrl"] = &model.StringValue{Data: resourceSpan.SchemaUrl()}
		resourceSpansValue.Data = append(resourceSpansValue.Data, &resourceSpanValue)
	}
	tracesValue.Data["resourceSpans"] = &resourceSpansValue
	return &tracesValue
}
