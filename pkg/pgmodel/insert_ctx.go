package pgmodel

import (
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
	for i := range t.WriteRequest.Timeseries {
		ts := &t.WriteRequest.Timeseries[i]
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]
	}
	t.WriteRequest.Timeseries = t.WriteRequest.Timeseries[:0]
	for i := range t.Labels {
		resetLabels(&t.Labels[i])
	}
	t.Labels = t.Labels[:0]
}

func resetLabels(l *Labels) {
	l.metricName = ""
	for i := range l.names {
		l.names[i] = ""
	}
	l.names = l.names[:0]
	for i := range l.values {
		l.values[i] = ""
	}
	l.values = l.values[:0]
	l.str = ""
}

func (t *InsertCtx) NewLabels(length int) *Labels {
	if len(t.Labels) < cap(t.Labels) {
		t.Labels = t.Labels[:len(t.Labels)+1]
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
		t.clear()
		pool.Put(t)
	}
}
