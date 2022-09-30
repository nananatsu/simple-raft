package filter

import "kvdb/pkg/utils"

type BloomFilter struct {
	bitsPerKey int
	hashKeys   []uint32
}

// 添加键到过滤器
func (b *BloomFilter) Add(key []byte) {
	b.hashKeys = append(b.hashKeys, utils.Hash(key, 0xa1b2c3d4))
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

	h := utils.Hash(key, 0xa1b2c3d4)

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
