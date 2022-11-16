package bplus

import "fmt"

type Item[T uint64 | string] struct {
	key   T
	value []byte
}

type Node[T uint64 | string] struct {
	pgid           uint64
	overflowPageId []uint64
	isLeaf         bool
	parent         *Node[T]
	key            []T
	item           []*Item[T]
	children       []*Node[T]
	next           *Node[T]
}

// 分裂节点返回 key、分裂后新节点
func (n *Node[T]) Split(pageId, overflowPageId uint64) (key T, node *Node[T]) {
	size := len(n.key)
	start := (size + 1) / 2
	keySize := size - start

	newNodeKey := make([]T, keySize)
	copy(newNodeKey, n.key[start:])

	if n.isLeaf {
		newNodeItem := make([]*Item[T], keySize+1)
		copy(newNodeItem, n.item[start:])
		node = NewNode(pageId, []uint64{overflowPageId}, true, n.parent, newNodeKey, newNodeItem, nil, n.next)

		key = node.item[0].key
		n.next = node
		n.item = n.item[:start]
	} else {
		newNodeChild := make([]*Node[T], keySize+1)
		copy(newNodeChild, n.children[start:])
		node = NewNode(pageId, []uint64{overflowPageId}, false, n.parent, newNodeKey, nil, newNodeChild, nil)

		for _, c := range node.children {
			c.parent = node
		}

		key = node.getMinKey()
		n.children = n.children[:start]
	}

	n.key = n.key[:start-1]
	return
}

func (n *Node[T]) getMinKey() T {
	if n.isLeaf {
		return n.item[0].key
	} else {
		return n.children[0].getMinKey()
	}
}

// 借用兄弟节点数据
func (n *Node[T]) Borrow(isLeftBrother bool, node *Node[T], borrowSize int) T {

	if isLeftBrother {
		size := len(node.key) + 1
		start := size - borrowSize

		key := make([]T, borrowSize)
		key[0] = n.getMinKey()
		if len(key) > 1 {
			copy(key[1:], node.key[start+1:])
		}

		n.key = append(key, n.key...)
		node.key = node.key[:start-1]

		if n.isLeaf {
			item := make([]*Item[T], borrowSize)
			copy(item, node.item[start:])
			n.item = append(item, n.item...)
			node.item = node.item[:start]
		} else {
			for _, c := range node.children[start:] {
				c.parent = n
				n.children = append(n.children, c)
			}
			node.children = node.children[:start]
		}
		return n.getMinKey()
	} else {
		n.key = append(n.key, node.getMinKey())
		n.key = append(n.key, node.key[:borrowSize-1]...)
		node.key = node.key[borrowSize:]

		if n.isLeaf {
			n.item = append(n.item, node.item[:borrowSize]...)
			node.item = node.item[borrowSize:]
		} else {
			for _, c := range node.children[:borrowSize] {
				c.parent = n
				n.children = append(n.children, c)
			}
			node.children = node.children[borrowSize:]
		}
		return node.getMinKey()
	}
}

func (n *Node[T]) Merge(node *Node[T]) {
	if n.isLeaf {
		n.key = append(n.key, node.item[0].key)
		n.key = append(n.key, node.key...)
		n.item = append(n.item, node.item...)
		n.next = node.next
		node.next = nil
		node.parent = nil
	} else {
		n.key = append(n.key, node.getMinKey())
		n.key = append(n.key, node.key...)

		for _, c := range node.children {
			c.parent = n
			n.children = append(n.children, c)
		}
		node.parent = nil
	}
}

func (n *Node[T]) InsertChild(key T, child *Node[T]) int {

	if n.isLeaf {
		return -1
	}

	size := len(n.key)
	pos := n.findIdx(key)

	if pos == size && key > n.children[pos].key[0] {
		n.InsertAt(pos+1, key, nil, child)
	} else {
		n.InsertAt(pos, key, nil, child)
	}

	return pos
}

func (n *Node[T]) InsertItem(key T, item *Item[T]) (int, bool) {

	if !n.isLeaf {
		return -1, false
	}

	pos := n.findIdx(key)
	if key == n.item[pos].key {
		n.item[pos] = item
		return pos, false
	} else if key > n.item[pos].key {
		n.InsertAt(pos+1, key, item, nil)
		return pos + 1, true
	} else {
		n.InsertAt(pos, key, item, nil)
		return pos, true
	}
}

func (n *Node[T]) Remove(key T) int {
	pos := n.findIdx(key)

	if key == n.item[pos].key {
		n.RemoveAt(pos)
		return pos
	} else {
		fmt.Println("未找到index对应数据", key)
		return -1
	}
}

func (n *Node[T]) RemoveAt(pos int) {
	if n.isLeaf {
		if pos == 0 {
			n.item = n.item[1:]
			n.key = n.key[1:]
		} else if pos == len(n.key) {
			n.item = n.item[:pos]
			n.key = n.key[:pos-1]
		} else {
			n.item = append(n.item[:pos], n.item[pos+1:]...)
			n.key = append(n.key[:pos-1], n.key[pos:]...)
		}
	} else {
		remove := n.children[pos]
		remove.parent = nil
		remove.next = nil

		if pos == 0 {
			n.children = n.children[1:]
			n.key = n.key[1:]
		} else if pos == len(n.key) {
			n.children = n.children[:pos]
			n.key = n.key[:pos-1]
		} else {
			n.children = append(n.children[:pos], n.children[pos+1:]...)
			n.key = append(n.key[:pos-1], n.key[pos:]...)
		}

	}
}

func (n *Node[T]) InsertAt(pos int, key T, item *Item[T], child *Node[T]) {

	size := len(n.key)
	if pos < size+1 {
		if pos == 0 {
			if size == 0 {
				n.key = append(n.key, n.item[0].key)
			} else {
				n.key = append(n.key[:1], n.key...)
				n.key[0] = key
			}
		} else {
			n.key = append(n.key[:pos], n.key[pos-1:]...)
			n.key[pos-1] = key
		}

		if n.isLeaf {
			n.item = append(n.item[:pos+1], n.item[pos:]...)
			n.item[pos] = item
		} else {
			n.children = append(n.children[:pos+1], n.children[pos:]...)
			n.children[pos] = child
		}
	} else {
		n.key = append(n.key, key)

		if n.isLeaf {
			n.item = append(n.item, item)
		} else {
			n.children = append(n.children, child)
		}
	}
}

func (n *Node[T]) findIdx(key T) int {
	var pos int
	for i := 0; i < len(n.key); i++ {
		if key >= n.key[i] {
			pos++
		} else {
			break
		}
	}
	return pos
}

func (n *Node[T]) findLeaf(key T) *Node[T] {
	if !n.isLeaf {
		return n.children[n.findIdx(key)].findLeaf(key)
	} else {
		return n
	}
}

func NewNode[T uint64 | string](pageId uint64, overflowPageId []uint64, isLeaf bool, parent *Node[T], key []T, item []*Item[T], children []*Node[T], next *Node[T]) *Node[T] {

	return &Node[T]{
		pgid:           pageId,
		overflowPageId: overflowPageId,
		isLeaf:         isLeaf,
		parent:         parent,
		key:            key,
		item:           item,
		children:       children,
		next:           next,
	}
}
