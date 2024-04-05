package compressotelexporter

import (
	"github.com/beet233/compressotelcollector/model"
)

// 自定义的HashMap结构
type HashMap struct {
	size  int
	store map[int][]*entry
}

// entry 存储实际的键值对，其中键是Key类型
type entry struct {
	value model.Value
	index int
}

// NewHashMap 创建一个新的HashMap实例
func NewHashMap() *HashMap {
	return &HashMap{size: 0, store: make(map[int][]*entry)}
}

// Put 设置键值对
func (h *HashMap) Put(v model.Value, index int) {
	hash := v.Hash()
	if e, ok := h.getKeyEntry(v); ok {
		e.value = v
		return
	}
	h.store[hash] = append(h.store[hash], &entry{value: v, index: index})
	h.size += 1
}

// Get 获取与键关联的值
func (h *HashMap) Get(v model.Value) (int, bool) {
	if e, ok := h.getKeyEntry(v); ok {
		return e.index, true
	}
	return 0, false
}

func (h *HashMap) Size() int {
	return h.size
}

// getKeyEntry 工具函数，用来从存储中检索出匹配的Entry
func (h *HashMap) getKeyEntry(v model.Value) (*entry, bool) {
	hash := v.Hash()
	entries := h.store[hash]
	for _, e := range entries {
		if model.ValueComparator(v, e.value) == 0 {
			return e, true
		}
	}
	return nil, false
}
