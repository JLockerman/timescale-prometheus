package compression

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type Simple8bRleCompressor struct {
	selectors       BitArray
	lastBlock       simple8bRleBlock
	lastBlockSet    bool
	alreadyFinished bool

	compressedData []uint64
	numElements    uint32

	// up to 64 elements
	uncompressedElements []uint64
}

type simple8bRleBlock struct {
	selector              uint8
	data                  uint64
	numElementsCompressed uint32
}

func (self *Simple8bRleCompressor) Append(val uint64) {
	if self.alreadyFinished {
		panic("cannot add data to an already finished Simple8bRleCompressor")
	}
	if len(self.uncompressedElements) >= 64 {
		self.flush()
	}

	self.uncompressedElements = append(self.uncompressedElements, val)
}

func (self *Simple8bRleCompressor) IsEmpty() bool {
	return self.numElements == 0 && len(self.uncompressedElements) == 0
}

func (self *Simple8bRleCompressor) TotalBytesUsed() uint32 {
	self.flush()
	if self.numElements == 0 {
		return 0
	}

	if self.lastBlockSet {
		self.pushBlock(simple8bRleBlock{})
		self.alreadyFinished = true
	}

	return 4 + // size of self.numElements
		4 + // size of uint32(len(self.compressedData))
		uint32(self.selectors.DataBytesUsed()) +
		uint32(len(self.compressedData)*8)
}

var EmptySimple8bError = fmt.Errorf("tired to serialize empty simple8b")

func (self *Simple8bRleCompressor) WriteTo(sink io.Writer) (int64, error) {
	self.flush()
	if self.numElements == 0 {
		return 0, EmptySimple8bError
	}

	if self.lastBlockSet {
		self.pushBlock(simple8bRleBlock{})
		self.alreadyFinished = true
	}

	err := binary.Write(sink, binary.BigEndian, self.numElements)
	if err != nil {
		return 4, err
	}
	err = binary.Write(sink, binary.BigEndian, uint32(len(self.compressedData)))
	if err != nil {
		return 8, err
	}

	length, err := self.selectors.WriteDataTo(sink)
	if err != nil {
		return length + 8, err
	}
	err = binary.Write(sink, binary.BigEndian, self.compressedData[:len(self.compressedData)])
	length += 8 + int64(len(self.compressedData))*8
	return length, err
}

type partiallyCompressedData struct {
	block simple8bRleBlock
	data  []uint64
}

const (
	maxCode = 15

	rleSelector     = maxCode
	rleMaxValueBits = 36
	rleMaxCountBits = 64 - rleMaxValueBits
	rleMaxValueMask = (1 << rleMaxValueBits) - 1
	rleMaxCountMask = (1 << rleMaxCountBits) - 1

	bitsPerSelector = 4
)

var (
	numElements = [15]uint8{0, 64, 32, 21, 16, 12, 10, 9, 8, 6, 5, 4, 3, 2, 1}
	bitLength   = [16]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 16, 21, 32, 64, 36}
)

func (self *Simple8bRleCompressor) flush() {
	var last_block simple8bRleBlock
	if self.lastBlockSet {
		last_block = self.popLastBlock()
	}
	if last_block.selector == 0 && len(self.uncompressedElements) == 0 {
		return
	}

	new_data := partiallyCompressedData{}
	if last_block.isRle() {
		// special case when the prev slot is RLE: we're always going to use RLE
		// again, and recompressing could be expensive if the RLE contains a large
		// amount of data
		appended_to_rle := last_block.appendToRle(self.uncompressedElements[:])
		self.pushBlock(last_block)
		new_data = partiallyCompressedData{
			data: self.uncompressedElements[appended_to_rle:],
		}
	} else {
		new_data = partiallyCompressedData{
			block: last_block,
			data:  self.uncompressedElements[:],
		}
	}

	self.appendPcd(&new_data)

	self.numElements += uint32(len(self.uncompressedElements))
	self.uncompressedElements = self.uncompressedElements[:0]
}

func (self *Simple8bRleCompressor) appendPcd(new_data *partiallyCompressedData) {
	idx := uint32(0)
	new_data_len := new_data.numElements()
	for idx < new_data_len {
		if new_data.get(idx) <= rleMaxValueMask {
			// runlength encode, if it would save space
			rle_count := uint32(1)
			rle_val := new_data.get(idx)
			for idx+rle_count < new_data_len && new_data.get(idx+rle_count) == rle_val {
				rle_count += 1
				if rle_count >= rleMaxCountMask {
					break
				}
			}
			bits_per_int := uint32(1)
			if rle_val != 0 {
				bits_per_int = bitsForValue(rle_val)
			}
			if bits_per_int*rle_count >= 64 {
				// RLE would save space over slot-based encodings
				block := createRleBlock(rle_count, rle_val)
				self.pushBlock(block)
				idx += rle_count
				continue
			}
		}

		block := simple8bRleBlock{
			selector: 1, data: 0, numElementsCompressed: 0,
		}
		mask := block.getBitmask()
		i := uint32(0)
		for idx+i < new_data_len && i < uint32(numElements[block.selector]) {
			val := new_data.get(idx + i)
			for val > mask {
				block.selector += 1
				mask = block.getBitmask()
				// subtle point: if we no longer have enough spaces left in the block for this
				// element, we should stop trying to fit it in. (even in that case, we still must
				// use the new selector to prevent gaps)
				if i >= uint32(numElements[block.selector]) {
					break
				}
			}
			i += 1
		}

		// assert!(block.selector < MAXCODE);
		// assert!(mask == block.get_bitmask());

		num_packed := uint32(0)
		for num_packed < uint32(numElements[block.selector]) && idx+num_packed < new_data_len {
			new_val := new_data.get(idx + num_packed)
			block.append(new_val)
			num_packed += 1
		}
		self.pushBlock(block)
		idx += num_packed
	}
}

func (self *Simple8bRleCompressor) pushBlock(block simple8bRleBlock) {
	if self.lastBlockSet {
		self.selectors.Append(bitsPerSelector, uint64(self.lastBlock.selector))
		self.compressedData = append(self.compressedData, self.lastBlock.data)
	}

	self.lastBlock = block
	self.lastBlockSet = true
}

func (self *Simple8bRleCompressor) popLastBlock() simple8bRleBlock {
	if !self.lastBlockSet {
		panic("tried to pop but no last block")
	}
	block := self.lastBlock
	self.lastBlockSet = false
	self.lastBlock = simple8bRleBlock{}
	return block
}

func (self *partiallyCompressedData) numElements() uint32 {
	return self.block.numElementsCompressed + uint32(len(self.data))
}

func (self *partiallyCompressedData) get(element_pos uint32) uint64 {
	if element_pos < self.block.numElementsCompressed {
		return self.block.get(element_pos)
	} else {
		return self.data[element_pos-self.block.numElementsCompressed]
	}
}

func createRleBlock(count uint32, val uint64) simple8bRleBlock {
	// assert!(val <= RLE_MAX_VALUE_MASK);
	// assert!(count <= RLE_MAX_COUNT_MASK);
	data := (uint64(count) << rleMaxValueBits) | val
	return simple8bRleBlock{
		selector:              rleSelector,
		data:                  data,
		numElementsCompressed: count,
	}
}

func (self *simple8bRleBlock) getBitmask() uint64 {
	bitlen := bitLength[self.selector]
	if bitlen < 64 {
		return (1 << bitlen) - 1
	} else {
		return math.MaxUint64
	}
}

func (self *simple8bRleBlock) isRle() bool {
	return self.selector == rleSelector
}

func (self *simple8bRleBlock) appendToRle(data []uint64) int {
	// assert!(self.is_rle());
	repeated_value := self.rleValue()
	repeat_count := self.rleCount()
	i := 0
	for i < len(data) && data[i] == repeated_value && repeat_count < rleMaxCountMask {
		repeat_count += 1
		i += 1
	}

	self.data = (uint64(repeat_count) << rleMaxValueBits) | repeated_value

	return i
}

func (self *simple8bRleBlock) rleValue() uint64 {
	// assert!(self.is_rle());
	return rleValue(self.data)
}

func (self *simple8bRleBlock) rleCount() uint32 {
	// assert!(self.is_rle());
	return rleCount(self.data)
}

func (self *simple8bRleBlock) append(val uint64) {
	// assert!(!self.is_rle());
	// assert!(val <= self.get_bitmask(), "{} <= {}", val, self.get_bitmask());
	// assert!(self.num_elements_compressed < NUM_ELEMENTS[self.selector as usize] as u32);
	self.data |= val << (bitLength[self.selector] * uint8(self.numElementsCompressed))
	self.numElementsCompressed += 1
}

func (self *simple8bRleBlock) get(pos uint32) uint64 {
	// assert_ne!(self.selector, 0);

	if self.isRle() {
		// assert!(self.rle_count() > pos);
		return self.rleValue()
	} else {
		compressed_value := self.data
		bits_per_val := uint32(bitLength[self.selector])
		// assert!(pos < NUM_ELEMENTS[self.selector as usize] as _, "{} < {}", pos, NUM_ELEMENTS[self.selector as usize]);
		compressed_value >>= bits_per_val * pos
		compressed_value &= self.getBitmask()
		return compressed_value
	}
}

func rleValue(data uint64) uint64 {
	return data & rleMaxValueMask
}

func rleCount(data uint64) uint32 {
	return uint32(data>>rleMaxValueBits) & rleMaxCountMask
}

func bitsForValue(v uint64) uint32 {
	r := uint32(0)
	if v >= (1 << 31) {
		v >>= 32
		r += 32
	}
	if v >= (1 << 15) {
		v >>= 16
		r += 16
	}
	if v >= (1 << 7) {
		v >>= 8
		r += 8
	}
	if v >= (1 << 3) {
		v >>= 4
		r += 4
	}
	if v >= (1 << 1) {
		v >>= 2
		r += 2
	}
	if v >= (1 << 0) {
		// v >>= 1;
		r += 1
	}
	return r
}
