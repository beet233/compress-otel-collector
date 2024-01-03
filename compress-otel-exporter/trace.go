package compressotelexporter

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/pdata/ptrace"
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
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpan := td.ResourceSpans().At(i)
		// TODO: 注册并编码
		// resourceSpan.Resource()
		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpan := resourceSpan.ScopeSpans().At(j)
			// TODO: 注册并编码
			// scopeSpan.Scope()
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				// TODO: 注册并编码
				// span
			}
		}
	}
	return nil

}
