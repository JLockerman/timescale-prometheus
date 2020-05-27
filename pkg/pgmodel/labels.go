// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/timescale-prometheus/pkg/prompb"
)

// Labels stores a labels.Labels in its canonical string representation
type Labels struct {
	labels     []prompb.Label
	metricName string
	str        string
}

// EmptyLables returns an empty Labels object
func EmptyLables() Labels {
	return Labels{}
}

// LabelsFromSlice converts a labels.Labels to a Labels object
func LabelsFromSlice(ls labels.Labels) (Labels, error) {
	panic("TODO")
	labels := Labels{}

	labels.metricName = ""
	for _, l := range ls {
		if l.Name == MetricNameLabelName {
			labels.metricName = l.Value
		}
	}

	// err := initLabels(&labels)
	return labels, nil
}

// initLabels intializes labels
func getStr(labels []prompb.Label) (string, error) {

	if !sort.SliceIsSorted(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	}) {
		sort.Slice(labels, func(i, j int) bool {
			return labels[i].Name < labels[j].Name
		})
	}

	length := len(labels)
	expectedStrLen := length * 4 // 2 for the length of each key, and 2 for the lengthof each value
	for i := 0; i < length; i++ {
		l := labels[i]
		expectedStrLen += len(l.Name) + len(l.Value)
	}

	// BigCache cannot handle cases where the key string has a size greater than
	// 16bits, so we error on such keys here. Since we are restricted to a 16bit
	// total length anyway, we only use 16bits to store the legth of each substring
	// in our string encoding
	if expectedStrLen > math.MaxUint16 {
		return "", fmt.Errorf("series too long, combined series has length %d, max length %d", expectedStrLen, ^uint16(0))
	}

	// the string representation is
	//   (<key-len>key <val-len> val)* (<key-len>key <val-len> val)?
	// that is a series of the a sequence of key values pairs with each string
	// prefixed with it's length as a little-endian uint16
	builder := strings.Builder{}
	builder.Grow(expectedStrLen)

	lengthBuf := make([]byte, 2)
	for i := 0; i < length; i++ {
		l := labels[i]
		key := l.Name

		// this cast is safe since we check that the combined length of all the
		// strings fit within a uint16, each string's length must also fit
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(key)))
		builder.WriteByte(lengthBuf[0])
		builder.WriteByte(lengthBuf[1])
		builder.WriteString(key)

		val := l.Value

		// this cast is safe since we check that the combined length of all the
		// strings fit within a uint16, each string's length must also fit
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(val)))
		builder.WriteByte(lengthBuf[0])
		builder.WriteByte(lengthBuf[1])
		builder.WriteString(val)
	}

	return builder.String(), nil
}

func labelProtosToLabels(labelPairs []prompb.Label) (*Labels, string, error) {
	str, err := getStr(labelPairs)
	if err != nil {
		return nil, "", err
	}
	length := len(labelPairs)
	labels := GetLabels(str)
	storeLabels := false
	if labels == nil {
		storeLabels = true
		labels = new(Labels)
		labels.labels = make([]prompb.Label, length)
		labels.str = str
		copy(labels.labels, labelPairs)
	}

	labels.metricName = ""

	for _, l := range labels.labels {
		if l.Name == MetricNameLabelName {
			labels.metricName = l.Value
		}
	}

	if storeLabels {
		SetLabels(str, labels)
	}

	return labels, labels.metricName, err
}

func (l *Labels) isEmpty() bool {
	return l.labels == nil
}

func (l *Labels) String() string {
	return l.str
}

func (l *Labels) reset() {
	l.metricName = ""
	for i := range l.labels {
		l.labels[i] = prompb.Label{}
	}
	l.labels = l.labels[:0]
	l.str = ""
}

// Compare returns a comparison int between two Labels
func (l *Labels) Compare(b *Labels) int {
	return strings.Compare(l.str, b.str)
}

// Equal returns true if two Labels are equal
func (l *Labels) Equal(b *Labels) bool {
	return l.str == b.str
}

// Labels implements sort.Interface

func (l *Labels) Len() int {
	return len(l.labels)
}

func (l *Labels) Less(i, j int) bool {
	return l.labels[i].Name < l.labels[j].Name
}

func (l *Labels) Swap(i, j int) {
	tmp := l.labels[j]
	l.labels[j] = l.labels[i]
	l.labels[i] = tmp

	tmp = l.labels[j]
	l.labels[j] = l.labels[i]
	l.labels[i] = tmp
}
