package compression

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
)

type GorillaCompressor struct {
	tag0s             Simple8bRleCompressor
	tag1s             Simple8bRleCompressor
	leadingZeros      BitArray
	numBitsUsedPerXor Simple8bRleCompressor
	xors              BitArray

	prevVal            uint64
	prevLeadingZeroes  uint8
	prevTrailingZeroes uint8
}

const bitsPerLeadingZeros = 6

func (self *GorillaCompressor) Append(val uint64) {
	xor := self.prevVal ^ val
	// for the first value we store the bitsize even if the xor is all zeroes,
	// this ensures that the bits-per-xor isn't empty, and that we can calculate
	// the remaining offsets correctly.
	has_values := !self.numBitsUsedPerXor.IsEmpty()

	if has_values && xor == 0 {
		self.tag0s.Append(0)
	} else {
		// leftmost/rightmost 1 is not well-defined when all the bits in the number
		// are 0; the C implementations of these functions will ERROR, while the
		// assembly versions may return any value. We special-case 0 to to use
		// values for leading and trailing-zeroes that we know will work.
		leading_zeros := uint8(63)
		if xor != 0 {
			leading_zeros = uint8(bits.LeadingZeros64(xor))
		}
		trailing_zeros := uint8(1)
		if xor != 0 {
			trailing_zeros = uint8(bits.TrailingZeros64(xor))
		}
		// TODO this can easily get stuck with a bad value for trailing_zeroes
		// we use a new trailing_zeroes if th delta is too large, but the
		// threshold was picked in a completely unprincipled manner.
		// Needs benchmarking
		reuse_bitsizes := has_values &&
			leading_zeros >= self.prevLeadingZeroes &&
			trailing_zeros >= self.prevTrailingZeroes &&
			((leading_zeros-self.prevLeadingZeroes)+
				(trailing_zeros-self.prevTrailingZeroes)) <= 12

		self.tag0s.Append(1)
		var tag1 uint64
		if reuse_bitsizes {
			tag1 = 0
		} else {
			tag1 = 1
		}
		self.tag1s.Append(tag1)
		if !reuse_bitsizes {
			self.prevLeadingZeroes = leading_zeros
			self.prevTrailingZeroes = trailing_zeros
			num_bits_used := 64 - (leading_zeros + trailing_zeros)
			self.leadingZeros.Append(bitsPerLeadingZeros, uint64(leading_zeros))
			self.numBitsUsedPerXor.Append(uint64(num_bits_used))
		}

		num_bits_used := 64 - (self.prevLeadingZeroes + self.prevTrailingZeroes)
		self.xors.Append(num_bits_used, xor>>self.prevTrailingZeroes)
	}
	self.prevVal = val
}

func (self *GorillaCompressor) CompressedLen() int64 {
	if self.tag0s.IsEmpty() {
		return 0
	}

	return 1 + // algorithm id
		1 + // uint8 has nulls
		8 + // last value
		int64(self.tag0s.TotalBytesUsed()) +
		int64(self.tag1s.TotalBytesUsed()) +
		int64(self.leadingZeros.TotalBytesUsed()) +
		int64(self.numBitsUsedPerXor.TotalBytesUsed()) +
		int64(self.xors.TotalBytesUsed())
}

func (self *GorillaCompressor) WriteTo(sink io.Writer) (int64, error) {
	if self.tag0s.IsEmpty() {
		return 0, nil
	}

	var length int64

	expectedLen := self.CompressedLen()

	//FIXME need correct id size
	l, err := sink.Write([]uint8{CompressionAlgorithmGorilla, // algorithm id
		0, // has nulls
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

	ll, err := self.tag0s.WriteTo(sink)
	length += ll
	if err != nil {
		return length, err
	}

	ll, err = self.tag1s.WriteTo(sink)
	length += ll
	if err != nil {
		return length, err
	}

	ll, err = self.leadingZeros.WriteTo(sink)
	length += ll
	if err != nil {
		return length, err
	}

	ll, err = self.numBitsUsedPerXor.WriteTo(sink)
	length += ll
	if err != nil {
		return length, err
	}

	ll, err = self.xors.WriteTo(sink)
	length += ll

	if err != nil {
		return length, err
	}

	if length != expectedLen {
		return length, fmt.Errorf("Malformed compression\nexpected: %4d bytes\n     got: %4d bytes", length, expectedLen)
	}
	return length, err
}
