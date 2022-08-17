package table

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/snappy"
)

type LevelNode struct {
	Path     string
	File     string
	fd       *os.File
	reader   *bufio.Reader
	filter   []byte
	index    []*IndexMetaData
	Level    int
	SeqNo    int
	FileSize int64

	compressScratch []byte
	curBlock        int
	curBuf          *bytes.Buffer
	prevKey         []byte

	compacting bool
}

type IndexMetaData struct {
	Key    []byte
	Offset int64
	Size   int64
}

func (ln *LevelNode) load() error {
	fd, err := os.OpenFile(path.Join(ln.Path, ln.File), os.O_RDONLY, 0644)

	if err != nil {
		log.Println("打开sst文件失败", err)
	}

	fileInfo, err := fd.Stat()

	if err != nil {
		log.Println("获取sst文件信息失败", err)
	}

	fileInfo.Size()

	reader := bufio.NewReader(fd)

	_, err = fd.Seek(-footerLen, io.SeekEnd)

	if err != nil {
		log.Println("移动读取位置到meta data失败", err)
	}

	filterOffset, err := binary.ReadUvarint(reader)

	if err != nil {
		log.Println("读取过滤器偏移失败", err)
	}

	indexOffset, err := binary.ReadUvarint(reader)

	if err != nil {
		log.Println("读取索引偏移失败", err)
	}

	indexSize, err := binary.ReadUvarint(reader)

	if err != nil {
		log.Println("读取索引长度失败", err)
	}

	log.Println("加载", ln.File)
	// log.Println("加载", ln.File, "过滤器偏移:", filterOffset, "索引偏移:", indexOffset, "索引长度", indexSize)

	_, err = fd.Seek(int64(filterOffset), io.SeekStart)

	if err != nil {
		log.Println("移动读取位置到过滤器失败", err)
	}
	reader.Reset(fd)

	if filterOffset == 0 {
		return err
	}

	filter := make([]byte, indexOffset-filterOffset)

	_, err = io.ReadFull(reader, filter)

	if err != nil {
		log.Println("读取过滤块失败", err)
	}

	index := make([]byte, indexSize-4)

	_, err = io.ReadFull(reader, index)

	if err != nil {
		log.Println("读取索引失败", err)
	}

	d, err := snappy.Decode(nil, index)

	if err != nil {
		log.Println("解压索引失败", err)
	}

	ln.fd = fd
	ln.reader = reader
	ln.filter = filter
	ln.index = DecodeIndexBlock(d)
	ln.FileSize = fileInfo.Size()

	return err
}

func (ln *LevelNode) NextRecord() ([]byte, []byte) {

	if ln.curBuf == nil {

		if ln.curBlock > len(ln.index)-1 {
			return nil, nil
		}

		blockOffset := ln.index[ln.curBlock].Offset - ln.index[ln.curBlock].Size

		ln.fd.Seek(blockOffset, io.SeekStart)
		ln.reader.Reset(ln.fd)

		compressed := make([]byte, ln.index[ln.curBlock].Size-4)

		_, err := io.ReadFull(ln.reader, compressed)

		if err != nil {
			log.Println("读取数据块失败", err)
			return nil, nil
		}

		dataLen, err := snappy.DecodedLen(compressed)

		if err != nil {
			log.Println("获取解压大小失败", err)
		}

		if len(ln.compressScratch) < dataLen {
			ln.compressScratch = make([]byte, dataLen)
		}

		data, err := snappy.Decode(ln.compressScratch, compressed)

		if err != nil {
			log.Println("解压数据块失败", err)
		}

		record, _ := DecodeBlock(data)
		ln.curBuf = bytes.NewBuffer(record)
		ln.prevKey = make([]byte, 0)
		ln.curBlock++
	}

	key, value, err := ReadRecord(ln.prevKey, ln.curBuf)

	if err == nil {
		ln.prevKey = key
		return key, value
	}

	ln.curBuf = nil
	return ln.NextRecord()
}

func (ln *LevelNode) Destory() {

	log.Println("移除", ln.File)

	ln.Level = -1
	ln.reader.Reset(ln.fd)
	ln.fd.Close()
	ln.filter = nil
	ln.index = nil
	ln.compressScratch = nil
	ln.curBuf = nil
	ln.prevKey = nil
	ln.FileSize = 0
	os.Remove(path.Join(ln.Path, ln.File))
}

func DecodeBlock(block []byte) ([]byte, []int) {

	n := len(block)

	nRestartPoint := int(binary.LittleEndian.Uint32(block[n-4:]))
	oRestartPoint := n - (nRestartPoint * 4) - 4
	restartPoint := make([]int, nRestartPoint)

	for i := 0; i < nRestartPoint; i++ {
		restartPoint[i] = int(binary.LittleEndian.Uint32(block[oRestartPoint+i*4:]))
	}

	return block[:oRestartPoint], restartPoint
}

func ReadRecord(prevKey []byte, buf *bytes.Buffer) ([]byte, []byte, error) {

	keyPrefixLen, err := binary.ReadUvarint(buf)

	if err != nil {
		// log.Println("读取key共享长度失败", err)
		return nil, nil, err
	}

	keyLen, err := binary.ReadUvarint(buf)

	if err != nil {
		// log.Println("读取key长度失败", err)
		return nil, nil, err
	}

	valueLen, err := binary.ReadUvarint(buf)

	if err != nil {
		// log.Println("读取Value长度失败", err)
		return nil, nil, err
	}

	key := make([]byte, keyLen)

	_, err = io.ReadFull(buf, key)
	if err != nil {
		// log.Println("读取Key失败", err)
		return nil, nil, err
	}

	value := make([]byte, valueLen)

	_, err = io.ReadFull(buf, value)
	if err != nil {
		// log.Println("读取Value失败", err)
		return nil, nil, err
	}

	actualKey := make([]byte, keyPrefixLen)
	copy(actualKey, prevKey[0:keyPrefixLen])
	actualKey = append(actualKey, key...)

	return actualKey, value, nil
}

func DecodeIndexBlock(index []byte) []*IndexMetaData {

	data, _ := DecodeBlock(index)
	indexBuf := bytes.NewBuffer(data)

	indexes := make([]*IndexMetaData, 0)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, indexBuf)

		if err != nil {
			break
		}

		buf := bytes.NewBuffer(value)

		offset, err := binary.ReadUvarint(buf)

		if err != nil {
			log.Println("读取块偏移失败", err)
		}

		size, err := binary.ReadUvarint(buf)

		if err != nil {
			log.Println("读取块大小失败", err)
		}

		indexes = append(indexes, &IndexMetaData{
			Key:    key,
			Offset: int64(offset),
			Size:   int64(size),
		})
		prevKey = key
	}
	return indexes
}

type LevelTree struct {
	mu sync.RWMutex

	filePath       string
	tree           [][]*LevelNode
	seqNo          []int
	compactionChan chan int
}

func (lt *LevelTree) AddLevelNode(sstFileName string, level, seqNo int) *LevelNode {

	ln := &LevelNode{
		Path:  lt.filePath,
		File:  sstFileName,
		Level: level,
		SeqNo: seqNo,
	}

	if ln.load() != nil {
		return nil
	}

	lt.mu.Lock()
	defer lt.mu.Unlock()

	length := len(lt.tree[level]) - 1
	idx := length
	for ; idx >= 0; idx-- {
		if seqNo > lt.tree[level][idx].SeqNo {
			break
		}
	}
	if idx == length {
		lt.tree[level] = append(lt.tree[level], ln)
	} else {
		var newLevel []*LevelNode
		if idx == -1 {
			newLevel = make([]*LevelNode, 1)
			newLevel = append(newLevel, lt.tree[level]...)
		} else {
			newLevel = append(lt.tree[level][:idx+1], lt.tree[level][idx:]...)
		}
		newLevel[idx+1] = ln
		lt.tree[level] = newLevel
	}

	return ln
}

func (lt *LevelTree) removeNode(nodes []*LevelNode) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lv := nodes[0].Level

	for _, node := range nodes {
		node.Destory()
	}

	for i := lv; i <= lv+1; i++ {
		newLevel := make([]*LevelNode, 0)
		for _, node := range lt.tree[i] {
			if node.Level != -1 {
				newLevel = append(newLevel, node)
			}
		}
		lt.tree[i] = newLevel
	}
}

func (lt *LevelTree) NewSstFile(level int) (*os.File, string, int) {

	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.seqNo[level]++

	name := strconv.Itoa(level) + "." + strconv.Itoa(lt.seqNo[level]) + ".sst"
	fd, err := os.OpenFile(path.Join(lt.filePath, name), os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		log.Println("创建sst文件失败", err)
	}

	return fd, name, lt.seqNo[level]
}

func (lt *LevelTree) NewWriter(ln *LevelNode) *SstWriter {

	fd, err := os.OpenFile(path.Join(ln.Path, ln.File), os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		log.Println("创建sst文件失败", err)
	}

	return NewSstWriter(fd)
}

func (lt *LevelTree) Compaction(level int) error {

	nodes := lt.PickupCompactionNode(level)

	if len(nodes) == 0 {
		return nil
	}

	newLevel := level + 1

	sstFiles := make([]string, 0)
	seqNos := make([]int, 0)

	fd, fileName, seqNo := lt.NewSstFile(newLevel)
	writer := NewSstWriter(fd)

	sstFiles = append(sstFiles, fileName)
	seqNos = append(seqNos, seqNo)

	maxNodeSize := maxMemTableSize * int(math.Pow10(newLevel))

	var record *Record

	for i, node := range nodes {
		k, v := node.NextRecord()
		record = record.push(k, v, i)
	}

	for record != nil {
		i := record.Idx
		writer.Append(record.Key, record.Value)
		record = record.next
		k, v := nodes[i].NextRecord()
		if k != nil {
			record = record.push(k, v, i)
		}

		if writer.Size() > maxNodeSize {
			writer.Finish()

			fd, fileName, seqNo = lt.NewSstFile(newLevel)
			writer = NewSstWriter(fd)
			sstFiles = append(sstFiles, fileName)
			seqNos = append(seqNos, seqNo)
		}

	}
	writer.Finish()

	for i, file := range sstFiles {
		lt.AddLevelNode(file, newLevel, seqNos[i])
	}

	lt.removeNode(nodes)

	return nil
}

func (lt *LevelTree) PickupCompactionNode(level int) []*LevelNode {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	compactionNode := make([]*LevelNode, 0)

	if len(lt.tree[level]) == 0 {
		return compactionNode
	}

	var startKey []byte
	var endKey []byte

	if level == 0 {
		index := lt.tree[level][0].index
		startKey = index[0].Key
		endKey = index[len(index)-1].Key
	} else {
		index := lt.tree[level][0].index
		startKey = index[0].Key
		endKey = index[len(index)-1].Key

		index = lt.tree[level][(len(lt.tree[level])-1)/2].index

		if bytes.Compare(index[0].Key, startKey) < 0 {
			startKey = index[0].Key
		}

		if bytes.Compare(index[len(index)-1].Key, endKey) > 0 {
			endKey = index[len(index)-1].Key
		}
	}

	files := make([]string, 0)

	for i := level; i <= level+1; i++ {
		for _, node := range lt.tree[i] {

			if node.index == nil {
				continue
			}
			nodeStartKey := node.index[0].Key
			nodeEndKey := node.index[len(node.index)-1].Key

			// log.Println("Level:", i, "SeqNum:", node.SeqNo, "Start:", string(nodeStartKey), "End:", string(nodeEndKey))
			if bytes.Compare(startKey, nodeEndKey) <= 0 && bytes.Compare(endKey, nodeStartKey) >= 0 && !node.compacting {
				compactionNode = append(compactionNode, node)
				files = append(files, node.File)
				node.compacting = true
			}
		}
	}

	log.Printf("合并Level: %d 文件：%v", level, files)

	return compactionNode
}

func (lt *LevelTree) CheckCompaction() {

	levelChan := make([]chan int, maxLevel)

	for i := 0; i < maxLevel; i++ {

		ch := make(chan int, 100)
		levelChan[i] = ch

		if i == 0 {
			go func(ch, to chan int) {
				for {
					<-ch
					if len(lt.tree[0]) > 4 {
						lt.Compaction(0)
					}
					to <- 1
				}
			}(ch, lt.compactionChan)

		} else {

			go func(ch, to chan int, lv int) {
				for {
					<-ch
					var prevSize int64
					maxNodeSize := int64(maxMemTableSize * math.Pow10(lv+1))
					for {
						var totalSize int64
						for _, node := range lt.tree[lv] {
							totalSize += node.FileSize
						}
						// log.Printf("Level %d 当前大小: %d M, 最大大小: %d M\n", lv, totalSize/(1024*1024), maxNodeSize/(1024*1024))

						if totalSize > maxNodeSize && (prevSize == 0 || totalSize < prevSize) {
							lt.Compaction(lv)
							to <- lv + 1
							prevSize = totalSize
						} else {
							break
						}
					}
				}
			}(ch, lt.compactionChan, i)
		}
	}

	for {
		lv := <-lt.compactionChan

		log.Printf("检查Level : %d是否可合并,待执行检查数量 : %d", lv, len(lt.compactionChan))

		levelChan[lv] <- 1
	}
}

func NewLevelTree(filePath string) *LevelTree {

	entries, err := os.ReadDir(filePath)

	if err != nil {
		log.Println("打开db文件夹失败", err)
	}

	compactionChan := make(chan int, 100)

	levelTree := make([][]*LevelNode, maxLevel)

	for i := range levelTree {
		levelTree[i] = make([]*LevelNode, 0)
	}

	seqNos := make([]int, maxLevel)

	lt := &LevelTree{
		filePath:       filePath,
		tree:           levelTree,
		seqNo:          seqNos,
		compactionChan: compactionChan,
	}

	for _, entry := range entries {

		if entry.IsDir() {
			continue
		}

		fileInfo := strings.Split(entry.Name(), ".")

		if len(fileInfo) != 3 || fileInfo[2] != "sst" {
			continue
		}

		level, err := strconv.Atoi(fileInfo[0])

		if err != nil {
			continue
		}

		seqNo, err := strconv.Atoi(fileInfo[1])

		if err != nil {
			continue
		}

		if level < maxLevel {
			lt.AddLevelNode(entry.Name(), level, seqNo)
			seqNos[level] = seqNo
		}
	}

	// for i:=0;i< runtime.NumCPU() -1;i++{
	go lt.CheckCompaction()
	// }

	lt.compactionChan <- 0

	return lt
}
