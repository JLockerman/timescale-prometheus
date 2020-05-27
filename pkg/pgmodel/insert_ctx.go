package pgmodel

import (
	"sync"

	"github.com/timescale/timescale-prometheus/pkg/prompb"
)

var wrPool = sync.Pool{
	New: func() interface{} {
		return new(prompb.WriteRequest)
	},
}

var lPool = sync.Pool{
	New: func() interface{} {
		return new(Labels)
	},
}

func NewWriteRequest() *prompb.WriteRequest {
	return wrPool.Get().(*prompb.WriteRequest)
}

func FinishWriteRequest(wr *prompb.WriteRequest) {
	for i := range wr.Timeseries {
		ts := &wr.Timeseries[i]
		for j := range ts.Labels {
			ts.Labels[j] = prompb.Label{}
		}
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]
		ts.XXX_unrecognized = nil
	}
	wr.Timeseries = wr.Timeseries[:0]
	wr.XXX_unrecognized = nil
	wrPool.Put(wr)
}

func NewLabels(length int) *Labels {
	l := lPool.Get().(*Labels)
	if cap(l.names) >= length {
		l.names = l.names[:length]
		l.values = l.values[:length]
	} else {
		l.names = make([]string, length)
		l.values = make([]string, length)
	}
	return l
}

func FinishLabels(l *Labels) {
	l.reset()
	lPool.Put(l)
}
