package filter

import (
	"encoding/binary"
)

type BloomFilter struct {
	bitsPerKey int
	hashKeys   []uint32
}

// 添加键到过滤器
func (b *BloomFilter) Add(key []byte) {
	b.hashKeys = append(b.hashKeys, MurmurHash3(key, 0xbc9f1d34))
}

// 通过双重hash生成布隆过滤器位数组
func (b *BloomFilter) Hash() []byte {

	n := len(b.hashKeys)
	k := uint8(b.bitsPerKey * 69 / (100 * n))

	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}
	// 布隆过滤器bit数组长度
	nBits := uint32(n * b.bitsPerKey)

	if nBits < 64 {
		nBits = 64
	}

	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8

	dest := make([]byte, nBytes+1)
	dest[nBytes] = k

	// hash1(key)+i*hash2(key)
	for _, h := range b.hashKeys {
		delta := (h >> 17) | (h << 15)
		for i := uint8(0); i < k; i++ {
			bitpos := h % nBits
			dest[bitpos/8] |= 1 << (bitpos % 8)
			h += delta
		}
	}

	// b.hashKeys = b.hashKeys[:0]
	return dest
}

// 计算布隆过滤器位数组长度
func (b *BloomFilter) Size() int {
	n := len(b.hashKeys)
	k := uint8(b.bitsPerKey * 69 / (100 * n))

	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}
	return int(n * b.bitsPerKey)
}

// 过滤器键长度
func (b *BloomFilter) KeyLen() int {
	return len(b.hashKeys)
}

// 充值过滤器
func (b *BloomFilter) Reset() {
	b.hashKeys = b.hashKeys[:0]
}

func NewBloomFilter(bitsPerKey int) *BloomFilter {
	return &BloomFilter{
		bitsPerKey: bitsPerKey,
	}
}

// 检查指定布隆过滤器位数组中键是否存在
func Contains(filter, key []byte) bool {

	nBytes := len(filter) - 1

	if nBytes < 1 {
		return false
	}

	nBits := uint32(nBytes * 8)
	k := filter[nBytes]

	if k > 30 {
		return true
	}

	h := MurmurHash3(key, 0xbc9f1d34)

	delta := (h >> 17) | (h << 15)
	for i := uint8(0); i < k; i++ {
		bitpos := h % nBits

		if (uint32(filter[bitpos/8]) & (1 << (bitpos % 8))) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func MurmurHash3(data []byte, seed uint32) uint32 {
	const (
		c1 = uint32(0xcc9e2d51)
		c2 = uint32(0x1b873593)
		n  = uint32(0xe6546b64)
	)

	h := seed
	l := len(data)
	i := 0

	for i+4 < l {
		k := binary.LittleEndian.Uint32(data[i:])
		k *= c1
		k = k<<15 | k>>17
		k *= c2

		h ^= k
		h = h<<13 | h>>19
		h = (h * 5) + n
		i += 4
	}

	k := uint32(0)
	switch l - i {
	case 3:
		k ^= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		k ^= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		k ^= uint32(data[i])
		k *= c1
		k = k<<15 | k>>17
		k *= c2
	}

	h ^= uint32(l)
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16

	return h
}

func LeveldbHash(data []byte, seed uint32) uint32 {
	const (
		m = uint32(0xc6a4a793)
	)

	l := uint32(len(data))
	h := seed ^ (l * m)
	i := uint32(0)

	for i+4 < l {
		k := binary.LittleEndian.Uint32(data[i:])

		h += k
		h *= m
		h ^= (h >> 16)
		i += 4
	}

	switch l - i {
	case 3:
		h += uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h += uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h += uint32(data[i])
		h *= m
		h ^= (h >> 24)
	}
	return h
}
