package compression

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

func TestDeltaDeltaCompression(t *testing.T) {
	cases := []struct {
		name     string
		values   func() []uint64
		expected string
	}{
		{
			name: "single element",
			values: func() []uint64 {
				return []uint64{1}
			},
			expected: "BAAAAAAAAAAAAQAAAAAAAAABAAAAAQAAAAEAAAAAAAAAAgAAAAAAAAAC",
		},
		{
			name: "count 1000",
			values: func() []uint64 {
				values := make([]uint64, 0, 1000)
				for i := uint64(1); i <= 1000; i++ {
					values = append(values, i)
				}
				return values
			},
			expected: "BAAAAAAAAAAD6AAAAAAAAAABAAAD6AAAAAIAAAAAAAAA8gAAAAAAAAACAAA8gAAAAAA=",
		},
		{
			name: "big deltas",
			values: func() []uint64 {
				max := uint64(9223372036854775807)
				min := asUint64(-9223372036854775808)
				return []uint64{
					// big deltas
					(0), (max), (min), (max), (min),
					(0), (min), (32), (5), (min), asUint64(-52), (max),
					(1000),
					// big delta deltas
					(0), (max), (max), (min), (min), (max), (max),
					(0), (max - 1), (max - 1), (min), (min), (max - 1), (max - 1),
				}
			},
			expected: "BAB//////////gAAAAAAAAAAAAAAGwAAABHu6+7s7t7e7gAAAAAAAAADAAAAAAAAAAD//////////v/////////7AAAABAAAAAP//////////gAAAEAAAAAA/////////4r/////////0wAdsAAZwABd////////8F7////////4Mf/////////9AAIAAQABAAL//////////QAAAAAAAAAF//////////sAAAAAAAAI3A==",
		},
		//TODO random order int tests
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var compressor DeltaDeltaCompressor
			values := c.values()
			for i := 0; i < len(values); i++ {
				compressor.Append(values[i])
			}

			var b bytes.Buffer
			_, err := compressor.WriteTo(&b)
			if err != nil {
				t.Errorf("error compressing: %+v", err)
			}
			bb := b.Bytes()
			result := base64.StdEncoding.EncodeToString(bb)
			if result != c.expected {
				t.Errorf("compression error:\nexpected\n%s\ngot\n%s", c.expected, result)
				decoded, err := base64.StdEncoding.DecodeString(c.expected)
				if err != nil {
					t.Fatalf("decoding err: %+v", err)
				}
				t.Errorf("bytes different:\nexpected\n%v\ngot\n%v", decoded, bb)
				e, err := ddFieldsFromBytes(bb)
				r, err := ddFieldsFromBytes(decoded)
				t.Errorf("fields different:\nexpected\n%+v\ngot\n%+v", e, r)
			}
		})
	}
}

func TestGorillaCompression(t *testing.T) {
	cases := []struct {
		name     string
		values   func() []float64
		expected string
	}{
		{
			name: "single element",
			values: func() []float64 {
				return []float64{1.0}
			},
			expected: "AwA/8AAAAAAAAAAAAAEAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAEAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAEGAAAAAAAAAAIAAAABAAAAAQAAAAAAAAAEAAAAAAAAAAoAAAABCgAAAAAAAAP/",
		},
		{
			name: "big deltas",
			values: func() []float64 {
				return []float64{
					// special
					0.0, math.Inf(1), math.Inf(-1),
					// postgres uses a different NaN than go by default, so we use postgres's
					math.Float64frombits(0x7ff8000000000000),
					// big deltas
					0.0, math.Inf(1), math.Inf(-1), math.Inf(1), math.Inf(-1),
					0.0, math.Inf(-1), (32), (5), math.Inf(-1), (-52), math.Inf(1),
					(1000),
					//big delta_deltas
					0.0, math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1), math.Inf(1), math.Inf(1),
				}
			},
			expected: "AwB/8AAAAAAAAAAAABgAAAABAAAAAAAAAAEAAAAAAFf//wAAABUAAAABAAAAAAAAAAEAAAAAAAvwDwAAAAICAEIAIAkAAH8AAAAAAAAAAAAAAAsAAAABAAAAAAAAAAUABjB7XFaFYAAAAAQkgAP/n/8AH/9/b/9/+gAQAP39v7v92/5rAAAAD/+BAj0=",
		},
		{
			name: "all 0s",
			values: func() []float64 {
				return make([]float64, 1000)
			},
			expected: "AwAAAAAAAAAAAAAAA+gAAAACAAAAAAAAAPEAAAAAAAAAAQAAOoAAAAAAAAAAAQAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAQYAAAAAAAAAPwAAAAEAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAA",
		},
		//TODO random order float tests?
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var compressor GorillaCompressor
			values := c.values()
			for i := 0; i < len(values); i++ {
				compressor.Append(math.Float64bits(values[i]))
			}

			var b bytes.Buffer
			_, err := compressor.WriteTo(&b)
			if err != nil {
				t.Errorf("error compressing: %+v", err)
			}
			bb := b.Bytes()
			result := base64.StdEncoding.EncodeToString(bb)
			if result != c.expected {
				t.Errorf("compression error:\nexpected\n%s\ngot\n%s", c.expected, result)
				decoded, err := base64.StdEncoding.DecodeString(c.expected)
				if err != nil {
					t.Fatalf("decoding err: %+v", err)
				}
				t.Errorf("bytes different:\nexpected\n%v\ngot\n%v", decoded, bb)
				e, err := gFieldsFromBytes(decoded)
				r, err := gFieldsFromBytes(bb)
				t.Errorf("fields different:\nexpected\n%+v\ngot\n%+v", e, r)
			}
		})
	}
}

type noisyBuffer struct {
	inner *bytes.Buffer
}

func newNoisyBuffer(b []byte) noisyBuffer {
	return noisyBuffer{bytes.NewBuffer(b)}
}

func (self *noisyBuffer) Write(p []byte) (n int, err error) {
	fmt.Printf("write: %v\n", p)
	return self.inner.Write(p)
}

func (self *noisyBuffer) Bytes() []byte {
	return self.inner.Bytes()
}

func (self *noisyBuffer) ReadByte() (byte, error) {
	b, err := self.inner.ReadByte()
	if err == nil {
		fmt.Printf("read: %v\n", b)
	}
	return b, err
}

func (self *noisyBuffer) Read(p []byte) (n int, err error) {
	n, err = self.inner.Read(p)
	if err == nil {
		fmt.Printf("read: %v\n", p[:n])
	}
	return
}

func asUint64(in int64) uint64 {
	return uint64(in)
}

type ddFields struct {
	algoId    uint8
	hasNulls  uint8
	lastValue uint64
	lastDelta int64
	data      s8bFields
}

func ddFieldsFromBytes(b []byte) (ddFields, error) {
	var dd ddFields
	var err error
	bb := bytes.NewBuffer(b)
	dd.algoId, err = bb.ReadByte()
	if err != nil {
		return dd, err
	}
	dd.hasNulls, err = bb.ReadByte()
	err = binary.Read(bb, binary.BigEndian, &dd.lastValue)
	if err != nil {
		return dd, err
	}

	err = binary.Read(bb, binary.BigEndian, &dd.lastDelta)
	if err != nil {
		return dd, err
	}

	dd.data, err = s8bFieldsFromBytes(bb)

	return dd, err
}

type gFields struct {
	algoId        uint8
	hasNulls      uint8
	lastValue     float64
	tag0s         s8bFields
	tag1s         s8bFields
	leadingZeros  baFields
	numBitsPerXor s8bFields
	xors          baFields
}

func gFieldsFromBytes(b []byte) (gFields, error) {
	var g gFields
	var err error
	bb := bytes.NewBuffer(b)
	g.algoId, err = bb.ReadByte()
	if err != nil {
		return g, err
	}
	g.hasNulls, err = bb.ReadByte()
	var lastVal uint64
	err = binary.Read(bb, binary.BigEndian, &lastVal)
	if err != nil {
		return g, err
	}
	g.lastValue = math.Float64frombits(lastVal)

	g.tag0s, err = s8bFieldsFromBytes(bb)
	if err != nil {
		return g, err
	}

	g.tag1s, err = s8bFieldsFromBytes(bb)
	if err != nil {
		return g, err
	}

	g.leadingZeros, err = baFieldsFromBytes(bb)
	if err != nil {
		return g, err
	}

	g.numBitsPerXor, err = s8bFieldsFromBytes(bb)
	if err != nil {
		return g, err
	}

	g.xors, err = baFieldsFromBytes(bb)

	return g, err
}

type s8bFields struct {
	numElements uint32
	numBlocks   uint32
	selectors   []uint64
	blocks      []uint64
}

func s8bFieldsFromBytes(bb *bytes.Buffer) (s8bFields, error) {
	var s8b s8bFields
	var err error
	err = binary.Read(bb, binary.BigEndian, &s8b.numElements)
	if err != nil {
		return s8b, err
	}

	err = binary.Read(bb, binary.BigEndian, &s8b.numBlocks)
	if err != nil {
		return s8b, err
	}

	numSlots := (s8b.numBlocks / 16)
	if s8b.numBlocks%16 != 0 {
		numSlots += 1
	}

	s8b.selectors = make([]uint64, numSlots)
	s8b.blocks = make([]uint64, s8b.numBlocks)
	for i := uint32(0); i < numSlots; i++ {
		err = binary.Read(bb, binary.BigEndian, &s8b.selectors[i])
		if err != nil {
			return s8b, err
		}
	}
	for i := uint32(0); i < s8b.numBlocks; i++ {
		err = binary.Read(bb, binary.BigEndian, &s8b.blocks[i])
		if err != nil {
			return s8b, err
		}
	}
	return s8b, err
}

type baFields struct {
	bits           []uint64
	numBlocks      uint32
	bitsUsedInLast uint8
}

func baFieldsFromBytes(bb *bytes.Buffer) (baFields, error) {
	var ba baFields
	err := binary.Read(bb, binary.BigEndian, &ba.numBlocks)
	if err != nil {
		return ba, err
	}

	ba.bitsUsedInLast, err = bb.ReadByte()
	if err != nil {
		return ba, err
	}

	ba.bits = make([]uint64, ba.numBlocks)
	for i := uint32(0); i < ba.numBlocks; i++ {
		err = binary.Read(bb, binary.BigEndian, &ba.bits[i])
		if err != nil {
			return ba, err
		}
	}

	return ba, err
}
