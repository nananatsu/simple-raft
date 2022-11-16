package bplus

import (
	"encoding/binary"
	"fmt"
	"kvdb/pkg/utils"
)

type PageType uint8

const (
	_ PageType = iota
	META_PAGE
	INDEX_PAGE
	LEAF_PAGE
)

type IndexType uint8

const (
	_ IndexType = iota
	UINT_INDEX
	STRING_INDEX
)

const (
	pageHeaderSize = 25
	pageFooterSize = 4

	pageIdOffset       = 0
	pageTypeOffset     = 8
	overflowPageOffset = 9
	nextPageOffset     = 17
	freePageOffset     = 21
)

// header: |8 byte(id)|1 byte(page type)|8 byte(overflow page)|8 byte(next page)| :25
// footer: |4 byte(checksum)| :4
type Page struct {
	Id       uint64
	pgType   PageType
	overflow uint64 // 数据溢出页
	next     uint64 // 下一页
	meta     *MetaPage
	index    *IndexPage
	leaf     *LeafPage
	Checksum uint32
}

// |4 byte(degree)| 8 byte(root page)|8 byte(max page)|1 byte(index type)|8 byte(free page) ...... | :21 + n *8
type MetaPage struct {
	Degree   int
	Root     uint64
	MaxPage  uint64
	idxType  IndexType
	FreePage []uint64
}

// uint64 key
// |8 byte(key)| ...... : 8 * (order-1)
// |8 byte(ptr)| ...... : 8 * order

// string key
// |4 byte(pos)|4 byte(ksize)|8 byte(child ptr)| ...... ： 16 * order
// | key | ......
type IndexPage struct {
	// Node *Node
}

// uint64 key
// |8 byte(key)|4 byte(pos)|4 byte(size)| ...... :16 * order
// | value | ......
// string key
// |4 byte(pos)|4 byte(ksize)|4 byte(vsize)| ...... : 12 * order
// | key | value | ......
type LeafPage struct {
	// Node *Node
}

func GetMaxDegree(pageSize int) int {
	return (pageSize - pageHeaderSize - pageFooterSize) / (8 * 2 * 2)
}

func GetPageParam(idxType IndexType, order int) (int, int) {

	var indexDataOffset int
	var leafDataOffset int

	switch idxType {
	case UINT_INDEX:
		indexDataOffset = 8 * (order - 1)
		leafDataOffset = 16 * order
	case STRING_INDEX:
		indexDataOffset = 16 * (order - 1)
		leafDataOffset = 12 * order
	}
	return indexDataOffset, leafDataOffset

}

func GetTypedPageParam[T uint64 | string](hold T, order int) (IndexType, int, int) {

	var idxType IndexType
	var indexDataOffset int
	var leafDataOffset int

	switch any(hold).(type) {
	case uint64:
		idxType = UINT_INDEX
		indexDataOffset = 8 * (order - 1)
		leafDataOffset = 16 * order
	case string:
		idxType = STRING_INDEX
		indexDataOffset = 16 * (order - 1)
		leafDataOffset = 12 * order
	}

	return idxType, indexDataOffset, leafDataOffset
}

func EncodePage(buf []byte, id, overflow, next uint64, pgType PageType) []byte {

	binary.BigEndian.PutUint64(buf[pageIdOffset:], id)
	buf[pageTypeOffset] = byte(pgType)
	binary.BigEndian.PutUint64(buf[overflowPageOffset:], overflow)
	binary.BigEndian.PutUint64(buf[nextPageOffset:], next)

	crc := utils.Checksum(buf)
	buf = append(buf, make([]byte, 4)...)
	binary.BigEndian.PutUint32(buf[len(buf)-4:], crc)
	return buf
}

func EncodeMetaPage(buf []byte, meta *MetaPage, offset int) []byte {
	binary.BigEndian.PutUint32(buf[offset:], uint32(meta.Degree))
	offset += 4
	binary.BigEndian.PutUint64(buf[offset:], meta.Root)
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], meta.MaxPage)
	offset += 8
	buf[offset] = byte(meta.idxType)
	offset += 1

	total := len(meta.FreePage) * 8
	current := len(buf) - offset
	if total > current {
		end := current / 8
		for _, v := range meta.FreePage[:end] {
			binary.BigEndian.PutUint64(buf[offset:], v)
			offset += 8
		}
		overflow := make([]byte, total-current)
		next := make([]byte, 8)
		binary.BigEndian.PutUint64(next, meta.FreePage[end])
		overflowOffset := len(buf) - offset
		copy(buf, next[:overflowOffset])
		copy(overflow, next[overflowOffset:])

		overflowOffset = 8 - overflowOffset
		if len(meta.FreePage) > end+1 {
			for _, v := range meta.FreePage[end+1:] {
				binary.BigEndian.PutUint64(overflow[overflowOffset:], v)
				overflowOffset += 8
			}
		}
		return overflow
	}

	for _, v := range meta.FreePage {
		binary.BigEndian.PutUint64(buf[offset:], v)
		offset += 8
	}

	return nil
}

func EncodeInddexPage[T uint64 | string](buf []byte, idxType IndexType, key []T, children []*Node[T], offset, dataOffset int) []byte {

	overflowBuf := make([]byte, 0, len(buf))
	if idxType == UINT_INDEX {
		dataOffset += offset
		for _, v := range key {
			k, _ := any(v).(uint64)
			binary.BigEndian.PutUint64(buf[offset:], k)
			offset += 8
		}

		for _, v := range children {
			binary.BigEndian.PutUint64(buf[dataOffset:], v.pgid)
			dataOffset += 8
		}
	} else if idxType == STRING_INDEX {
		for i, n := range children {
			if i == 0 {
				binary.BigEndian.PutUint32(buf[offset:], uint32(dataOffset))
				offset += 4
				binary.BigEndian.PutUint32(buf[offset:], uint32(0))
				offset += 4
				binary.BigEndian.PutUint64(buf[offset:], n.pgid)
				offset += 8
			} else {
				k, _ := any(key[i-1]).(string)
				kByte := []byte(k)
				kSize := len(kByte)

				binary.BigEndian.PutUint32(buf[offset:], uint32(dataOffset))
				offset += 4
				binary.BigEndian.PutUint32(buf[offset:], uint32(kSize))
				offset += 4
				binary.BigEndian.PutUint64(buf[offset:], n.pgid)
				offset += 8

				overflowBuf = append(overflowBuf, kByte...)
				dataOffset += kSize
			}
		}
	}
	return overflowBuf
}

func EncodeLeafPage[T uint64 | string](buf []byte, item []*Item[T], keyOffset, dataOffset int) []byte {
	overflowBuf := make([]byte, 0, len(buf))
	for _, v := range item {
		size := len(v.value)
		switch (any)(v.key).(type) {
		case uint64:
			k, _ := (any)(v.key).(uint64)

			binary.BigEndian.PutUint64(buf[keyOffset:], k)
			keyOffset += 8
			binary.BigEndian.PutUint32(buf[keyOffset:], uint32(dataOffset))
			keyOffset += 4
			binary.BigEndian.PutUint32(buf[keyOffset:], uint32(size))
			keyOffset += 4
		case string:
			k, _ := (any)(v.key).(string)
			kByte := []byte(k)
			kSize := len(kByte)

			binary.BigEndian.PutUint32(buf[keyOffset:], uint32(dataOffset))
			keyOffset += 4
			binary.BigEndian.PutUint32(buf[keyOffset:], uint32(kSize))
			keyOffset += 4
			binary.BigEndian.PutUint32(buf[keyOffset:], uint32(size))
			keyOffset += 4

			overflowBuf = append(overflowBuf, kByte...)
			dataOffset += kSize
		}

		overflowBuf = append(overflowBuf, v.value...)
		dataOffset += size
	}

	return overflowBuf
}

func EncodeOverflowPage(pageSize int, id uint64, overflow uint64, b []byte) []byte {

	buf := make([]byte, pageSize-pageFooterSize, pageSize)

	binary.BigEndian.PutUint64(buf[pageIdOffset:], id)
	buf[pageTypeOffset] = byte(0)
	binary.BigEndian.PutUint64(buf[overflowPageOffset:], overflow)

	binary.BigEndian.PutUint64(buf[nextPageOffset:], 0)

	copy(buf[pageHeaderSize:], b)
	crc := utils.Checksum(buf)
	buf = append(buf, make([]byte, 4)...)
	binary.BigEndian.PutUint32(buf[pageSize-4:], crc)

	return buf
}

func DecodeHeader(buf []byte) *Page {
	return &Page{
		Id:     binary.BigEndian.Uint64(buf),
		pgType: PageType(buf[pageTypeOffset]),
		// idxType:  IndexType(buf[indexTypeOffset]),
		overflow: binary.BigEndian.Uint64(buf[overflowPageOffset:]),
		next:     binary.BigEndian.Uint64(buf[nextPageOffset:]),
	}
}

func DecodeMetaPage(b []byte) *MetaPage {

	offset := 0
	degree := binary.BigEndian.Uint32(b[offset:])
	offset += 4
	root := binary.BigEndian.Uint64(b[offset:])
	offset += 8
	maxpage := binary.BigEndian.Uint64(b[offset:])
	offset += 8
	idxType := IndexType(b[offset])
	offset += 1

	freePage := make([]uint64, 0)
	for {
		page := binary.BigEndian.Uint64(b[offset:])
		if page == 0 {
			break
		}
		freePage = append(freePage, page)
		offset += 8
	}

	return &MetaPage{Root: root, Degree: int(degree), MaxPage: maxpage, idxType: idxType, FreePage: freePage}
}

func DecodeIndexPage[T uint64 | string](b, overflow []byte, idxType IndexType, node *Node[T], getNode func(uint64) *Node[T], readPage func(uint64) ([]byte, error), conf *Config) error {

	keys := make([]T, 0, conf.order-1)
	children := make([]*Node[T], 0, conf.order)

	offset := 0
	var prev *Node[T]
	if idxType == UINT_INDEX {
		for {
			key := binary.BigEndian.Uint64(b[offset:])
			offset += 8
			if key == 0 || offset >= conf.indexDataOffset {
				break
			}

			tKey, _ := any(key).(T)
			keys = append(keys, tKey)
		}

		offset = conf.indexDataOffset
		for {
			page := binary.BigEndian.Uint64(b[offset:])
			offset += 8
			if page == 0 || offset >= len(b)-4 {
				break
			}

			childNode := getNode(page)
			childNode.parent = node

			err := Load(page, getNode, readPage, conf)
			if err != nil {
				return err
			}

			if prev != nil {
				prev.next = childNode
			}
			prev = childNode

			children = append(children, childNode)
		}
		node.key = keys
		node.children = children

	} else if idxType == STRING_INDEX {

		b = append(b, overflow...)
		for {
			keyOffset := binary.BigEndian.Uint32(b[offset:])
			offset += 4
			keySize := binary.BigEndian.Uint32(b[offset:])
			offset += 4
			page := binary.BigEndian.Uint64(b[offset:])
			offset += 8

			if keyOffset == 0 || offset >= conf.indexDataOffset {
				break
			}

			key := string(b[keyOffset : keyOffset+keySize])
			tKey, _ := any(key).(T)
			keys = append(keys, tKey)

			childNode := getNode(page)
			childNode.parent = node

			err := Load(page, getNode, readPage, conf)
			if err != nil {
				return err
			}

			if prev != nil {
				prev.next = childNode
			}
			prev = childNode
			children = append(children, childNode)
		}
		node.key = keys[1:]
		node.children = children
	}

	return nil
}

func DecodeLeafPage[T uint64 | string](b []byte, overflowBuf []byte, idxType IndexType, node *Node[T], conf *Config) error {

	keys := make([]T, 0, conf.order)
	items := make([]*Item[T], 0, conf.order)
	offset := 0

	b = append(b, overflowBuf...)

	if idxType == UINT_INDEX {
		for {
			key := binary.BigEndian.Uint64(b[offset:])
			offset += 8
			valueOffset := binary.BigEndian.Uint32(b[offset:])
			offset += 4
			valueSize := binary.BigEndian.Uint32(b[offset:])
			offset += 4

			if key == 0 || offset > conf.LeafDataOffset {
				break
			}

			tKey, _ := any(key).(T)
			item := &Item[T]{key: tKey, value: b[valueOffset : valueOffset+valueSize]}
			keys = append(keys, item.key)
			items = append(items, item)
		}
	} else if idxType == STRING_INDEX {
		for {
			keyOffset := binary.BigEndian.Uint32(b[offset:])
			offset += 4
			keySize := binary.BigEndian.Uint32(b[offset:])
			offset += 4
			valueSize := binary.BigEndian.Uint32(b[offset:])
			offset += 4

			if keyOffset == 0 || offset >= conf.LeafDataOffset {
				break
			}

			valueOffset := keyOffset + keySize

			key := b[keyOffset:valueOffset]
			value := b[valueOffset : valueOffset+valueSize]

			tKey, _ := any(key).(T)
			keys = append(keys, tKey)
			items = append(items, &Item[T]{key: tKey, value: value})
		}
	}

	node.key = keys[1:]
	node.item = items

	return nil
}

func GetOverflowPage(overflowPageId uint64, readPage func(uint64) ([]byte, error)) ([]uint64, []byte, error) {
	overflowPages := make([]uint64, 0)
	overflowBuf := make([]byte, 0)
	for overflowPageId != 0 {
		overflowPages = append(overflowPages, overflowPageId)
		overflow, err := readPage(overflowPageId)
		if err != nil {
			return nil, nil, err
		}

		h := DecodeHeader(overflow)
		if h.overflow != overflowPageId {
			overflowPageId = h.overflow
		} else {
			overflowPageId = 0
		}
		overflowBuf = append(overflowBuf, overflow[pageHeaderSize:len(overflow)-pageFooterSize]...)
	}
	return overflowPages, overflowBuf, nil
}

func Load[T uint64 | string](pgid uint64, getNode func(uint64) *Node[T], readPage func(uint64) ([]byte, error), conf *Config) error {

	b, err := readPage(pgid)
	if err != nil {
		return err
	}

	page := DecodeHeader(b)
	node := getNode(page.Id)

	overflowPages, overflowBuf, err := GetOverflowPage(page.overflow, readPage)
	if err != nil {
		return err
	}

	node.overflowPageId = overflowPages

	if page.pgType == INDEX_PAGE {
		node.isLeaf = false
		return DecodeIndexPage(b[pageHeaderSize:len(b)-pageFooterSize], overflowBuf, conf.idxType, node, getNode, readPage, conf)
	} else if page.pgType == LEAF_PAGE {
		node.isLeaf = true
		node.next = getNode(page.next)

		err := DecodeLeafPage(b[pageHeaderSize:len(b)-pageFooterSize], overflowBuf, conf.idxType, node, conf)
		if err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("不支持的页类型")
}
