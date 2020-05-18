package compression

import (
	"encoding/binary"
	"fmt"
	"io"
)

type DeltaDeltaCompressor struct {
	prevVal    uint64
	prevDelta  uint64
	deltaDelta Simple8bRleCompressor
}

func (self *DeltaDeltaCompressor) Append(next_val uint64) {
	// We perform all arithmetic using unsigned values due to go's overflow rules:
	// signed integer overflow is allowed to panic,
	// while unsigned overflow is 2's complement,
	// so even very large delta work the same as any other

	// step 1: delta of deltas
	delta := next_val - self.prevVal
	deltaDelta := delta - self.prevDelta

	self.prevVal = uint64(next_val)
	self.prevDelta = delta

	// step 2: ZigZag encode
	encoded := zigZagEncode(deltaDelta)

	// step 3: simple8b/RTE
	self.deltaDelta.Append(encoded)
}

func zigZagEncode(value uint64) uint64 {
	xor := uint64(0)
	if int64(value) < 0 {
		xor = 0xFFFFFFFFFFFFFFFF
	}
	return (value << 1) ^ xor
}

func (self *DeltaDeltaCompressor) CompressedLen() int64 {
	if self.deltaDelta.IsEmpty() {
		return 0
	}

	return 1 + //algorithm id
		1 + // uint8 has nulls
		8 + // last value
		8 + // last delta
		int64(self.deltaDelta.TotalBytesUsed())
}

func (self *DeltaDeltaCompressor) WriteTo(sink io.Writer) (int64, error) {
	if self.deltaDelta.IsEmpty() {
		return 0, nil
	}

	expectedLen := self.CompressedLen()

	var length int64
	l, err := sink.Write([]uint8{
		CompressionAlgorithmDeltaDelta, // algorithm id
		0,                              // has_nulls
	})
	length += int64(l)
	if err != nil {
		return length, err
	}

	err = binary.Write(sink, binary.BigEndian, self.prevVal)
	length += 8
	if err != nil {
		return length, err
	}

	err = binary.Write(sink, binary.BigEndian, self.prevDelta)
	length += 8
	if err != nil {
		return length, err
	}

	ll, err := self.deltaDelta.WriteTo(sink)
	length += ll

	if err != nil {
		return length, err
	}

	if length != expectedLen {
		return length, fmt.Errorf("Malformed compression\nexpected: %4d bytes\n     got: %4d bytes", length, expectedLen)
	}

	return length, nil
}
