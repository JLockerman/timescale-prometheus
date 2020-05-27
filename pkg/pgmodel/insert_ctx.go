package pgmodel

import (
	"sync"

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
}

func NewInsertCtx() *InsertCtx {
	return pool.Get().(*InsertCtx)
}

func (t *InsertCtx) clear() {
	for i := range t.WriteRequest.Timeseries {
		ts := &t.WriteRequest.Timeseries[i]
		for j := range ts.Labels {
			ts.Labels[j] = prompb.Label{}
		}
		// TODO This won't be needed once Sample is a value type
		for j := range ts.Samples {
			ts.Samples[j] = prompb.Sample{}
		}
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]
	}
	t.WriteRequest.Timeseries = t.WriteRequest.Timeseries[:0]
	for i := range t.Labels {
		t.Labels[i].reset()
	}
	t.Labels = t.Labels[:0]
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
	t.clear()
	pool.Put(t)
}
