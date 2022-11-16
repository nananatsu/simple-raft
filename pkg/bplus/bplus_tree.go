package bplus

import (
	"encoding/binary"
	"fmt"
	"io"
	"kvdb/pkg/utils"
	"math"
	"os"
	"strconv"
	"sync/atomic"
)

type Config struct {
	degree          int
	order           int // 阶数
	pageSize        int
	availableSize   int
	indexDataOffset int
	LeafDataOffset  int
	idxType         IndexType
	fullRewrite     bool
}

type OperateType int

const (
	_ OperateType = iota
	UPDATE
	INSERT
	DELETE
)

type Operate[T uint64 | string] struct {
	node      *Node[T]
	idx       int
	operate   OperateType
	isNewNode bool
}

type Operates[T uint64 | string] map[uint64]*Operate[T]

func (op *Operate[T]) CalcOffset(conf *Config) ([][2]int, int, int) {
	diff := make([][2]int, 0)

	var dataOffeset int
	var eleSize int
	var eleEnd int
	if op.node.isLeaf && conf.idxType == UINT_INDEX {
		dataOffeset = conf.LeafDataOffset
		eleEnd = conf.LeafDataOffset
		for _, i := range op.node.item[:op.idx] {
			dataOffeset += len(i.value)
		}
		eleSize = 16
	} else if op.node.isLeaf && conf.idxType == STRING_INDEX {
		dataOffeset = conf.LeafDataOffset
		eleEnd = conf.LeafDataOffset
		for _, i := range op.node.item[:op.idx] {
			sk, _ := (any)(i.key).(string)
			dataOffeset += len(sk) + len(i.value)
		}
		eleSize = 12
	} else {
		dataOffeset = conf.indexDataOffset
		eleEnd = conf.indexDataOffset
		for _, i := range op.node.item[:op.idx] {
			sk, _ := (any)(i.key).(string)
			dataOffeset += len(sk)
		}
		eleSize = 16
	}

	KeyOffset := op.idx*eleSize + pageHeaderSize

	if op.operate == UPDATE {
		diff = append(diff, [2]int{KeyOffset, KeyOffset + eleSize})
	} else if op.operate == INSERT {
		diff = append(diff, [2]int{overflowPageOffset, pageHeaderSize})
		diff = append(diff, [2]int{KeyOffset, (len(op.node.key)+1)*eleSize + pageHeaderSize})
	} else {
		diff = append(diff, [2]int{overflowPageOffset, pageHeaderSize})
		diff = append(diff, [2]int{KeyOffset, pageHeaderSize + eleEnd})
	}

	var rewriteIdx int
	var firstOffset int
	if dataOffeset < conf.availableSize {
		dataOffeset += pageHeaderSize
		diff = append(diff, [2]int{dataOffeset, conf.pageSize})
		rewriteIdx = 0
		firstOffset = 0
	} else {
		diff = append(diff, [2]int{conf.pageSize - pageFooterSize, conf.pageSize})
		rewriteIdx = dataOffeset/conf.availableSize - 1
		firstOffset = dataOffeset % conf.availableSize
	}

	return diff, rewriteIdx, firstOffset
}

func (op *Operate[T]) CalcUintIndexOffset(conf *Config) [][2]int {
	diff := make([][2]int, 0)
	if op.operate == UPDATE {
		if op.idx > 0 {
			KeyOffset := (op.idx-1)*8 + pageHeaderSize
			diff = append(diff, [2]int{KeyOffset, KeyOffset + 8})
		}
		childOffset := pageHeaderSize + conf.indexDataOffset + op.idx*8
		diff = append(diff, [2]int{childOffset, childOffset + 8})
		diff = append(diff, [2]int{conf.pageSize - pageFooterSize, conf.pageSize})
	} else if op.operate == INSERT {
		diff = append(diff, [2]int{overflowPageOffset, pageHeaderSize})
		if op.idx > 0 {
			KeyOffset := (op.idx-1)*8 + pageHeaderSize
			diff = append(diff, [2]int{KeyOffset, len(op.node.key)*8 + pageHeaderSize})
		}
		childOffset := pageHeaderSize + conf.indexDataOffset
		diff = append(diff, [2]int{childOffset + op.idx*8, childOffset + len(op.node.children)*8})
		diff = append(diff, [2]int{conf.pageSize - pageFooterSize, conf.pageSize})
	} else {
		diff = append(diff, [2]int{overflowPageOffset, pageHeaderSize})
		if op.idx > 0 {
			KeyOffset := (op.idx-1)*8 + pageHeaderSize
			diff = append(diff, [2]int{KeyOffset, conf.indexDataOffset + pageHeaderSize})
		}
		childOffset := pageHeaderSize + conf.indexDataOffset + op.idx*8
		diff = append(diff, [2]int{childOffset, conf.pageSize})
	}
	return diff
}

func (o *Operates[T]) Add(op *Operate[T]) {
	prevOp, exist := (*o)[op.node.pgid]

	if exist {
		if prevOp.idx > op.idx {
			prevOp.idx = op.idx
		}

		if prevOp.operate < op.operate {
			prevOp.operate = op.operate
		}

		if op.isNewNode {
			prevOp.isNewNode = true
		}
	} else if op.idx > -1 {
		(*o)[op.node.pgid] = op
	}
}

type PageBuffer struct {
	buf       []byte
	pgid      uint64
	isNewPage bool
}

type MetaInfo struct {
	page       *Page
	pageNum    atomic.Uint64
	overflow   []uint64
	rewriteIdx int
	dirty      bool
}

func (m *MetaInfo) GetPageId() (id uint64) {
	size := len(m.page.meta.FreePage)
	if size > 0 {
		id = m.page.meta.FreePage[size-1]
		m.page.meta.FreePage = m.page.meta.FreePage[:size-1]
		if m.rewriteIdx == -1 || size < m.rewriteIdx {
			m.rewriteIdx = size
		}
	} else {
		id = m.pageNum.Add(1)
		if m.page.meta != nil {
			m.page.meta.MaxPage = id
		}
	}
	return
}

func (m *MetaInfo) ReleasePage(pgids []uint64) {
	size := len(m.page.meta.FreePage)
	m.page.meta.FreePage = append(m.page.meta.FreePage, pgids...)
	if m.rewriteIdx == 0 || size < m.rewriteIdx {
		m.rewriteIdx = size
	}
}

type BPlusTree[T uint64 | string] struct {
	typeHold T
	fd       *os.File
	root     *Node[T]
	meta     *MetaInfo
	cache    map[uint64]*Node[T]
	operates Operates[T]
	conf     *Config
}

func (t *BPlusTree[T]) splitPage(buf []byte, node *Node[T], rewriteIdx int) map[uint64]*PageBuffer {

	size := len(buf)
	var prevId uint64
	overflow := int(math.Ceil(float64(size) / float64(t.conf.availableSize)))

	pb := make(map[uint64]*PageBuffer)

	for overflow > len(node.overflowPageId) {
		node.overflowPageId = append(node.overflowPageId, 0)
	}

	for i := overflow; i > 0; i-- {
		if i < rewriteIdx {
			break
		}
		var id uint64
		var isNewPage bool

		id = node.overflowPageId[i-1]
		if id == 0 {
			id = t.meta.GetPageId()
			node.overflowPageId[i-1] = id
			isNewPage = true
		}

		var overBuf []byte

		if i < overflow {
			overBuf = EncodeOverflowPage(t.conf.pageSize, id, prevId, buf[(i-1)*t.conf.availableSize:i*t.conf.availableSize])
		} else {
			overBuf = EncodeOverflowPage(t.conf.pageSize, id, prevId, buf[(i-1)*t.conf.availableSize:])
		}
		prevId = id
		pb[id] = &PageBuffer{pgid: id, buf: overBuf, isNewPage: isNewPage}
	}

	if len(node.overflowPageId) > overflow {
		t.meta.ReleasePage(node.overflowPageId[overflow:])
		node.overflowPageId = node.overflowPageId[:overflow]
	}
	return pb
}

func (t *BPlusTree[T]) write(offset int64, buf []byte) error {
	_, err := t.fd.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = t.fd.Write(buf)

	return err
}

func (t *BPlusTree[T]) toPageBuffer(op *Operate[T], rewriteIdx int) map[uint64]*PageBuffer {

	node := op.node
	var overflowBuf []byte
	buf := make([]byte, t.conf.pageSize-pageFooterSize, t.conf.pageSize)

	binary.BigEndian.PutUint64(buf[pageIdOffset:], node.pgid)

	var overflow uint64
	var next uint64
	var pgType PageType
	if len(node.overflowPageId) > 0 {
		overflow = node.overflowPageId[0]
	}

	if node.next != nil {
		next = node.next.pgid
	}

	var overflowSplit int
	if node.isLeaf {
		pgType = LEAF_PAGE
		overflowBuf = EncodeLeafPage(buf, node.item, pageHeaderSize, t.conf.LeafDataOffset)
		if t.conf.LeafDataOffset < t.conf.availableSize && len(overflowBuf) > 0 {
			overflowSplit = t.conf.availableSize - t.conf.LeafDataOffset
		}
	} else {
		pgType = INDEX_PAGE
		overflowBuf = EncodeInddexPage(buf, t.conf.idxType, node.key, node.children, pageHeaderSize, t.conf.indexDataOffset)
		if t.conf.indexDataOffset < t.conf.availableSize && len(overflowBuf) > 0 {
			overflowSplit = t.conf.availableSize - t.conf.indexDataOffset
		}
	}

	if overflowSplit > 0 {
		copy(buf[t.conf.LeafDataOffset+pageHeaderSize:], overflowBuf[:overflowSplit])
		overflowBuf = overflowBuf[overflowSplit:]
	}

	buf = EncodePage(buf, node.pgid, overflow, next, pgType)
	var bufs map[uint64]*PageBuffer
	if len(overflowBuf) > 0 {
		bufs = t.splitPage(overflowBuf, node, rewriteIdx)
		bufs[node.pgid] = &PageBuffer{pgid: node.pgid, buf: buf, isNewPage: op.isNewNode}
	} else {
		bufs = map[uint64]*PageBuffer{node.pgid: {pgid: node.pgid, buf: buf, isNewPage: op.isNewNode}}
	}
	return bufs
}

func (t *BPlusTree[T]) String(hight int) string {
	var str string
	pedding := []*Node[T]{t.root}

	var lv int
	var preLv = -1
	for len(pedding) > 0 {
		n := pedding[0]
		pedding = pedding[1:]
		if n == nil {
			lv++

			if lv > hight {
				return str
			}
			str += "\n"
			continue
		}

		str += fmt.Sprintf("@%d | ", n.pgid)
		for i, v := range n.key {
			if n.isLeaf {
				if i < len(n.item) {
					str += fmt.Sprintf("(%v) %v ", n.item[i].key, v)
				} else {
					str += fmt.Sprintf("(%s) %v ", "?", v)
				}
			} else {
				str += fmt.Sprintf(" %v ", v)
			}
		}

		if n.isLeaf {
			if len(n.item) > 0 {
				str += fmt.Sprintf("(%v)", n.item[len(n.item)-1].key)
			} else {
				str += fmt.Sprintf("(%s)", "?")
			}

		}
		str += " | "

		if len(n.children) > 0 {
			if lv != preLv {
				pedding = append(pedding, nil)
				preLv = lv
			}

			pedding = append(pedding, n.children...)
		}
	}
	return str
}

func (t *BPlusTree[T]) ListAll(file string) {

	fd, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		fmt.Printf("创建导出文件失败 %v ", err)
	}

	start := t.root
	for !start.isLeaf {
		start = start.children[0]
	}

	for start != nil {
		for _, i2 := range start.item {

			_, err := fd.WriteString(fmt.Sprintf("%v %s \n", i2.key, string(i2.value)))

			if err != nil {
				fmt.Printf("创建导出文件失败 %v ", err)
			}

		}
		start = start.next
	}

}

func (t *BPlusTree[T]) NodeCount() uint64 {
	var total uint64

	start := t.root
	if start == nil {
		return 0
	}

	for !start.isLeaf {
		start = start.children[0]
	}

	for start != nil {
		total++
		start = start.next
	}

	return total
}

func (t *BPlusTree[T]) Size() uint64 {

	var total uint64

	start := t.root

	if start == nil {
		return 0
	}

	for !start.isLeaf {
		start = start.children[0]
	}

	for start != nil {
		total += uint64(len(start.item))
		start = start.next
	}

	return total
}

func (t *BPlusTree[T]) Check(pgid uint64, buf []byte) bool {

	offset := int64(pgid-1) * int64(t.conf.pageSize)
	ret := make([]byte, t.conf.pageSize)
	_, err := t.fd.Seek(offset, io.SeekStart)
	if err != nil {
		fmt.Println(err)
	}

	_, err = t.fd.Read(ret)
	if err != nil {
		fmt.Println(err)
	}

	crc := utils.Checksum(ret[:t.conf.pageSize-pageFooterSize])
	crcMem := binary.BigEndian.Uint32(buf[t.conf.pageSize-pageFooterSize:])
	if crc != crcMem {
		for i, v := range ret {
			if v != buf[i] {
				fmt.Printf("页 %d 偏移 %d 硬盘值 %v 内存值 %v \n", pgid, i, v, buf[i])
			}
		}
		return false
	}
	return true
}

func (t *BPlusTree[T]) flushMeta() error {
	offset := t.meta.rewriteIdx*8 + freePageOffset
	overPageIdx := offset/t.conf.availableSize - 1
	overPageOffset := offset%t.conf.availableSize + pageHeaderSize
	size := len(t.meta.page.meta.FreePage)*8 + freePageOffset

	var overflow int
	var newPage int
	if overPageIdx > -1 {
		overflow = int(math.Ceil(float64(size)/float64(t.conf.availableSize))) - 1
		newPage = overflow - 1 - len(t.meta.overflow)
		if overflow > 0 {
			for i := 0; i < newPage; i++ {
				t.meta.overflow = append(t.meta.overflow, t.meta.GetPageId())
			}
		} else if overflow < 0 {
			t.meta.ReleasePage(t.meta.overflow[overflow-1:])
			t.meta.overflow = t.meta.overflow[:overflow-1]
		}
	}

	buf := make([]byte, t.conf.pageSize-pageFooterSize, t.conf.pageSize)
	overflowBuf := EncodeMetaPage(buf, t.meta.page.meta, pageHeaderSize)
	pageOffset := int64((t.meta.page.Id - 1)) * int64(t.conf.pageSize)

	buf = EncodePage(buf, t.meta.page.Id, t.meta.page.overflow, t.meta.page.next, t.meta.page.pgType)
	err := t.write(pageOffset, buf[:pageHeaderSize+freePageOffset])
	if err != nil {
		return err
	}

	var footerWrited bool
	if overPageIdx == -1 && t.meta.rewriteIdx > -1 {
		end := overPageOffset + size
		if end > t.conf.pageSize {
			end = t.conf.pageSize
			footerWrited = true
		}
		err = t.write(pageOffset+int64(overPageOffset), buf[overPageOffset:end])
		if err != nil {
			return err
		}
	}
	if !footerWrited {
		err = t.write(pageOffset+int64(t.conf.pageSize)-pageFooterSize, buf[t.conf.pageSize-pageFooterSize:])
		if err != nil {
			return err
		}
	}

	var prevId uint64
	for i := overflow - 1; i > 0; i-- {
		id := t.meta.overflow[i-1]
		var buf []byte
		if i == overflow {
			buf = EncodeOverflowPage(t.conf.pageSize, id, prevId, overflowBuf[(i-1)*t.conf.availableSize:])
		} else {
			buf = EncodeOverflowPage(t.conf.pageSize, id, prevId, overflowBuf[(i-1)*t.conf.availableSize:i*t.conf.availableSize])
		}
		prevId = id
		if newPage > 0 || i > overPageIdx {
			err = t.write(int64((id-1))*int64(t.conf.pageSize), buf)
			if err != nil {
				return err
			}
			newPage--
		} else if i == overPageIdx {
			err = t.write(int64((id-1))*int64(t.conf.pageSize)+overflowPageOffset, buf[overPageOffset:])
			if err != nil {
				return err
			}
		} else if i < overPageIdx {
			break
		}
	}
	t.meta.dirty = false
	return nil
}

func (t *BPlusTree[T]) flushDirty() error {
	var err error
	for k, o := range t.operates {
		if t.conf.fullRewrite || o.isNewNode {
			bufs := t.toPageBuffer(o, 0)
			for _, pb := range bufs {
				err = t.write(int64(pb.pgid-1)*int64(t.conf.pageSize), pb.buf)
				if err != nil {
					return err
				}
			}
		} else {
			if !o.node.isLeaf && t.conf.idxType == UINT_INDEX {
				bufs := t.toPageBuffer(o, 0)
				baseOffset := int64((o.node.pgid - 1)) * int64(t.conf.pageSize)
				buf := bufs[o.node.pgid].buf

				diff := o.CalcUintIndexOffset(t.conf)
				for _, v := range diff {
					err = t.write(baseOffset+int64(v[0]), buf[v[0]:v[1]])
					if err != nil {
						return err
					}
				}
			} else {
				diff, rewriteIdx, firstOffset := o.CalcOffset(t.conf)
				bufs := t.toPageBuffer(o, rewriteIdx)

				baseOffset := int64((o.node.pgid - 1)) * int64(t.conf.pageSize)
				buf := bufs[o.node.pgid].buf

				for _, v := range diff {
					err = t.write(baseOffset+int64(v[0]), buf[v[0]:v[1]])
					if err != nil {
						return err
					}
				}

				for i, id := range o.node.overflowPageId[rewriteIdx:] {
					pb := bufs[id]
					buf := pb.buf

					baseOffset := int64((id - 1)) * int64(t.conf.pageSize)
					if pb.isNewPage || i != 0 {
						err = t.write(baseOffset, buf)
						if err != nil {
							return err
						}
					} else {
						err = t.write(baseOffset+int64(overflowPageOffset), buf[overflowPageOffset:pageHeaderSize])
						if err != nil {
							return err
						}
						err = t.write(baseOffset+int64(firstOffset), buf[firstOffset:])
					}

					if err != nil {
						return err
					}
				}
			}
		}
		delete(t.operates, k)
	}

	if t.meta.dirty {
		t.flushMeta()
	}
	return err
}

func (t *BPlusTree[T]) Rebuild() {

}

func (t *BPlusTree[T]) Add(key T, value []byte) error {

	item := &Item[T]{key: key, value: value}
	if t.root == nil {
		t.root = NewNode(t.meta.GetPageId(), []uint64{t.meta.GetPageId()}, true, nil, []T{}, []*Item[T]{item}, nil, nil)
		t.meta.page.meta.Root = t.root.pgid

		t.operates.Add(&Operate[T]{node: t.root, idx: 0, operate: INSERT, isNewNode: true})
		t.meta.dirty = true

		return t.flushDirty()
	}

	node := t.root.findLeaf(key)
	if len(node.key) >= t.conf.order-1 {
		splitKey, newNode := t.spliteNode(node)
		if key >= splitKey {
			node = newNode
		}
	}

	pos, isInsert := node.InsertItem(key, item)
	if isInsert {
		t.operates.Add(&Operate[T]{node: node, idx: pos, operate: INSERT})
	} else {
		t.operates.Add(&Operate[T]{node: node, idx: pos, operate: UPDATE})
	}
	return t.flushDirty()
}

func (t *BPlusTree[T]) Remove(key T) error {
	if t.root == nil {
		return nil
	}

	node := t.root.findLeaf(key)
	pos := node.Remove(key)

	t.operates.Add(&Operate[T]{node: node, idx: pos, operate: DELETE})
	t.balance(node)

	return t.flushDirty()
}

func (t *BPlusTree[T]) balance(node *Node[T]) {
	size := len(node.key)
	if size < t.conf.degree-1 {
		p := node.parent
		if p != nil {
			for i, n := range p.children {
				if n.pgid == node.pgid {
					t.mergeNode(p, i)
					break
				}
			}
		} else if size == 0 {
			if len(node.children) > 0 {
				t.root = node.children[0]
				t.root.parent = nil
				node.children = nil
				t.meta.page.meta.Root = t.root.pgid
			} else if len(node.item) == 0 {
				t.root = nil
				t.meta.page.meta.Root = 0
			}
			t.meta.dirty = true
		}
	}

	if size > t.conf.order-1 { // 分裂节点
		t.spliteNode(node)
	}
}

func (t *BPlusTree[T]) mergeNode(p *Node[T], pos int) {
	node := p.children[pos]
	size := len(node.key)
	borrowSize := t.conf.degree - size - 1
	var borrowed bool

	if pos > 0 { // 借用左兄弟节点
		bro := p.children[pos-1]
		if (len(bro.key) + size) >= t.conf.order-2 {
			key := node.Borrow(true, bro, borrowSize)
			p.key[pos-1] = key
			borrowed = true

			t.operates.Add(&Operate[T]{node: bro, idx: len(bro.key) + 1 - borrowSize, operate: DELETE})
			t.operates.Add(&Operate[T]{node: node, idx: 0, operate: INSERT})
			t.operates.Add(&Operate[T]{node: p, idx: pos, operate: UPDATE})
		}
	}

	if !borrowed && pos < len(p.key) { // 借用右兄弟节点
		bro := p.children[pos+1]
		if (len(bro.key) + size) >= t.conf.order-2 {
			key := node.Borrow(false, bro, borrowSize)
			p.key[pos] = key
			borrowed = true

			t.operates.Add(&Operate[T]{node: bro, idx: 0, operate: DELETE})
			t.operates.Add(&Operate[T]{node: node, idx: len(node.key) + 1 - borrowSize, operate: INSERT})
			t.operates.Add(&Operate[T]{node: p, idx: pos + 1, operate: UPDATE})
		}
	}

	if !borrowed { // 合并节点
		if pos > 0 {
			bro := p.children[pos-1]
			t.meta.page.meta.FreePage = append(t.meta.page.meta.FreePage, node.pgid)
			t.meta.page.meta.FreePage = append(t.meta.page.meta.FreePage, node.overflowPageId...)

			t.operates.Add(&Operate[T]{node: bro, idx: len(bro.key) + 1, operate: INSERT})
			t.operates.Add(&Operate[T]{node: p, idx: pos, operate: DELETE})

			bro.Merge(node)
			p.RemoveAt(pos)
		} else {
			bro := p.children[pos+1]
			t.meta.page.meta.FreePage = append(t.meta.page.meta.FreePage, bro.pgid)
			t.meta.page.meta.FreePage = append(t.meta.page.meta.FreePage, bro.overflowPageId...)

			t.operates.Add(&Operate[T]{node: node, idx: len(node.key) + 1, operate: INSERT})
			t.operates.Add(&Operate[T]{node: p, idx: pos + 1, operate: DELETE})

			node.Merge(bro)
			p.RemoveAt(pos + 1)
		}
	}

	t.balance(p)
}

func (t *BPlusTree[T]) spliteNode(node *Node[T]) (T, *Node[T]) {

	id := t.meta.GetPageId()
	var overflowPageId uint64
	if node.isLeaf {
		overflowPageId = t.meta.GetPageId()
	}
	key, n := node.Split(id, overflowPageId)
	p := node.parent

	t.operates.Add(&Operate[T]{node: node, idx: len(node.key), operate: DELETE})
	t.operates.Add(&Operate[T]{node: n, idx: 0, operate: INSERT, isNewNode: true})

	if p != nil { // 更新父节点索引
		pos := p.InsertChild(key, n)
		t.operates.Add(&Operate[T]{node: p, idx: pos, operate: INSERT})

		t.meta.dirty = true
		t.balance(p)
	} else { // 创建新根节点
		p = NewNode(t.meta.GetPageId(), nil, false, nil, []T{key}, nil, []*Node[T]{node, n}, nil)
		n.parent = p
		node.parent = p
		t.root = p
		t.meta.page.meta.Root = t.root.pgid

		t.operates.Add(&Operate[T]{node: p, idx: 0, operate: INSERT, isNewNode: true})
		t.meta.dirty = true
	}

	return key, n
}

func (t *BPlusTree[T]) Get(key T) []byte {

	node := t.root.findLeaf(key)
	pos := node.findIdx(key)

	if key == node.item[pos].key {
		return node.item[pos].value
	}
	return nil
}

func (t *BPlusTree[T]) ReadPage(pgid uint64) ([]byte, error) {

	b := make([]byte, t.conf.pageSize)

	_, err := t.fd.Seek(int64((pgid-1))*int64(t.conf.pageSize), io.SeekStart)

	if err != nil {
		return nil, err
	}

	n, err := t.fd.Read(b)

	if err != nil {
		return nil, err
	}

	if n != t.conf.pageSize {
		return nil, fmt.Errorf("文件大小异常")
	}

	return b, nil
}

func NewBPlusTree[T uint64 | string](file string, pageSize int) (*BPlusTree[T], error) {

	if _, err := os.Stat(file); err == nil {
		fd, err := os.OpenFile(file, os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("打开文件 %s 失败 %v", file, err)
		}

		tree, err := RestoreTree[T](fd, pageSize)
		if err != nil {
			fmt.Printf("还原b+ 树失败 %+v \n", err)
			return nil, err
		}
		return tree, nil
	}

	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开文件 %s 失败 %v", file, err)
	}

	degree := GetMaxDegree(pageSize)
	var pageNum atomic.Uint64
	pid := pageNum.Add(1)

	meta := &MetaInfo{
		page:       &Page{Id: pid, pgType: META_PAGE, meta: &MetaPage{Degree: degree, MaxPage: pid}},
		pageNum:    pageNum,
		rewriteIdx: -1,
	}

	order := degree * 2
	conf := &Config{
		degree:        degree,
		order:         order,
		pageSize:      pageSize,
		availableSize: pageSize - pageHeaderSize - pageFooterSize,
	}

	tree := &BPlusTree[T]{
		fd:       fd,
		meta:     meta,
		cache:    make(map[uint64]*Node[T]),
		operates: make(map[uint64]*Operate[T]),
		conf:     conf,
	}

	idxType, indexDataOffset, leafDataOffset := GetTypedPageParam(tree.typeHold, order)
	tree.conf.idxType = idxType
	tree.conf.indexDataOffset = indexDataOffset
	tree.conf.LeafDataOffset = leafDataOffset
	tree.meta.page.meta.idxType = idxType

	return tree, nil
}

func LoadMetaInfo(readPage func(uint64) ([]byte, error)) (*MetaInfo, error) {

	meta := &MetaInfo{rewriteIdx: -1}
	// 读取meta页面
	b, err := readPage(0)
	if err != nil {
		return nil, err
	}
	p := DecodeHeader(b)
	if p.overflow > 0 {
		overflowPages, overflowBuf, err := GetOverflowPage(p.overflow, readPage)
		if err != nil {
			return nil, err
		}
		b = append(b, overflowBuf...)
		meta.overflow = overflowPages
	}

	if p.pgType != META_PAGE {
		return nil, fmt.Errorf("不支持的文件格式")
	}
	p.meta = DecodeMetaPage(b[pageHeaderSize:])
	meta.page = p

	var pageNum atomic.Uint64
	pageNum.Store(p.meta.MaxPage)
	meta.pageNum = pageNum

	return meta, nil
}

func RestoreTree[T uint64 | string](fd *os.File, pageSize int) (*BPlusTree[T], error) {

	cache := make(map[uint64]*Node[T])
	getNode := func(id uint64) *Node[T] {
		node := cache[id]
		if node == nil {
			node = &Node[T]{pgid: id}
			cache[id] = node
		}
		return node
	}

	readPage := GetReadPage(fd, pageSize)

	meta, err := LoadMetaInfo(readPage)
	if err != nil {
		return nil, err
	}

	order := meta.page.meta.Degree * 2
	indexDataOffset, leafDataOffset := GetPageParam(meta.page.meta.idxType, order)
	conf := &Config{
		degree:          meta.page.meta.Degree,
		order:           order,
		pageSize:        pageSize,
		availableSize:   pageSize - pageHeaderSize - pageFooterSize,
		idxType:         meta.page.meta.idxType,
		indexDataOffset: indexDataOffset,
		LeafDataOffset:  leafDataOffset,
	}

	if meta.page.meta.Root > 0 {
		// 读取树
		err = Load(meta.page.meta.Root, getNode, GetReadPage(fd, pageSize), conf)
		if err != nil {
			return nil, err
		}
	}

	return &BPlusTree[T]{
		fd:       fd,
		meta:     meta,
		root:     getNode(meta.page.meta.Root),
		cache:    cache,
		operates: make(map[uint64]*Operate[T]),
		conf:     conf,
	}, nil
}

func GetReadPage(fd *os.File, pageSize int) func(uint64) ([]byte, error) {

	return func(pgid uint64) ([]byte, error) {
		b := make([]byte, pageSize)
		var err error
		if pgid == 0 {
			_, err = fd.Seek(0, io.SeekStart)
		} else {
			_, err = fd.Seek(int64((pgid-1))*int64(pageSize), io.SeekStart)
		}

		if err != nil {
			return nil, err
		}

		n, err := fd.Read(b)

		if err != nil {
			return nil, err
		}

		if n != pageSize {
			return nil, fmt.Errorf("文件大小异常")
		}

		footerOffset := len(b) - pageFooterSize
		crc := binary.BigEndian.Uint32(b[footerOffset:])
		crcActual := utils.Checksum(b[:footerOffset])

		if crc != crcActual {
			return nil, fmt.Errorf("页 %d 校验不一致,文件记录 %s 实际 %s", pgid, strconv.FormatUint(uint64(crc), 16), strconv.FormatUint(uint64(crcActual), 16))
		}

		return b, nil
	}

}
