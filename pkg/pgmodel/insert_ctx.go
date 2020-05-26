package pgmodel

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/timescale/timescale-prometheus/pkg/prompb"
)

var pool = sync.Pool{
	New: func() interface{} {
		return new(InsertCtx)
	},
}

type InsertCtx struct {
	WriteRequest prompb.WriteRequest
	Labels       []Labels
	refs         int64
}

func NewInsertCtx() *InsertCtx {
	return pool.Get().(*InsertCtx)
}

func (t *InsertCtx) clear() {
	for _, ts := range t.WriteRequest.Timeseries {
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]
	}
	t.WriteRequest.Timeseries = t.WriteRequest.Timeseries[:0]
	t.Labels = t.Labels[:0]
}

func (t *InsertCtx) resetLabels(l *Labels) {
	l.metricName = ""
	l.names = l.names[:0]
	l.values = l.values[:0]
	l.str = ""
}

func (t *InsertCtx) NewLabels(length int) *Labels {
	if len(t.Labels) < cap(t.Labels) {
		t.Labels = t.Labels[:len(t.Labels)+1]
		t.resetLabels(&t.Labels[len(t.Labels)-1])
	} else {
		t.Labels = append(t.Labels, Labels{
			names:  make([]string, 0, length),
			values: make([]string, 0, length),
		})
	}
	return &t.Labels[len(t.Labels)-1]
}

func (t *InsertCtx) Close() {
	newRefs := atomic.AddInt64(&t.refs, -1)
	if newRefs == 0 {
		fmt.Println("free InsertCtx")
		t.clear()
		pool.Put(t)
	}
}
