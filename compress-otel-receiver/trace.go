package compressotelreceiver

import (
	"context"
	"fmt"
	"github.com/beet233/compressotelcollector/model"
	"go.opentelemetry.io/collector/consumer"
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
	// TODO
	return ptrace.NewTraces()
}
