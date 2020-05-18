package compression

import (
	"encoding/binary"
	"io"
	"math"
)

type BitArray struct {
	buckets              []uint64
	bitsUsedInLastBucket uint8
}

func (self *BitArray) Append(num_bits uint8, bits uint64) {
	// Fill bits from LSB to MSB
	if num_bits > 64 {
		panic("num_bits must be <= 64")
	}
	if num_bits == 0 {
		return
	}

	if len(self.buckets) == 0 {
		self.appendBucket(0, 0)
	}

	bits &= lowBitsMask(num_bits)

	bits_remaining_in_last_bucket := 64 - self.bitsUsedInLastBucket
	if bits_remaining_in_last_bucket >= num_bits {
		bucket := last(self.buckets)
		// mask out any unused high bits, probably unneeded
		*bucket |= bits << self.bitsUsedInLastBucket
		self.bitsUsedInLastBucket += num_bits
		return
	}

	// When splitting an interger across buckets, the low-order bits go into the first bucket and
	// the high-order bits go into the second bucket
	num_bits_for_new_bucket := num_bits - bits_remaining_in_last_bucket
	if bits_remaining_in_last_bucket > 0 {
		bits_for_current_bucket := bits & lowBitsMask(bits_remaining_in_last_bucket)
		current_bucket := last(self.buckets)
		*current_bucket |= bits_for_current_bucket << self.bitsUsedInLastBucket
		bits >>= bits_remaining_in_last_bucket
	}

	// We zero out the high bits of the new bucket, to ensure that unused bits are always 0
	bits_for_new_bucket := bits & lowBitsMask(num_bits_for_new_bucket)
	self.appendBucket(num_bits_for_new_bucket, bits_for_new_bucket)
}

func (self *BitArray) appendBucket(bits_used uint8, bucket uint64) {
	self.buckets = append(self.buckets, bucket)
	self.bitsUsedInLastBucket = bits_used
}

func (self *BitArray) TotalBytesUsed() uint32 {
	return 4 + // size of uint32(len(self.buckets))
		1 + // size of self.bitsUsedInLastBucket
		self.DataBytesUsed()
}

func (self *BitArray) DataBytesUsed() uint32 {
	return uint32(len(self.buckets) * 8) // size of data
}

func (self *BitArray) NumBitsUsedInLastBucket() uint8 {
	return self.bitsUsedInLastBucket
}

func (self *BitArray) Len32() uint32 {
	return uint32(len(self.buckets))
}

func (self *BitArray) WriteTo(sink io.Writer) (int64, error) {
	err := binary.Write(sink, binary.BigEndian, uint32(len(self.buckets)))
	if err != nil {
		return 0, err
	}
	_, err = sink.Write([]byte{self.bitsUsedInLastBucket})
	if err != nil {
		return 0, err
	}
	_, err = self.WriteDataTo(sink)
	return int64(self.TotalBytesUsed()), err
}

func (self *BitArray) WriteDataTo(sink io.Writer) (int64, error) {
	err := binary.Write(sink, binary.BigEndian, self.buckets[:len(self.buckets)])
	return int64(self.DataBytesUsed()), err
}

func last(buckets []uint64) *uint64 {
	return &buckets[len(buckets)-1]
}

func lowBitsMask(bits_used uint8) uint64 {
	if bits_used == 64 {
		return math.MaxUint64
	} else {
		return (1 << bits_used) - 1
	}
}
