package bplus

import (
	"fmt"
	"kvdb/pkg/utils"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestAddToDiskTime(t *testing.T) {

	rand.Seed(time.Now().Unix())
	num := 1000000

	var diffCount int64
	var fullCount int64

	metricFull := make(chan struct{})
	metricDiff := make(chan struct{})

	go func() {
		tree, err := NewBPlusTree[uint64]("../../build/bplus/test"+strconv.Itoa(1), 16*1024)
		if err != nil {
			t.Error(err)
		}

		for i := 1; i < num; i++ {
			err = tree.Add(uint64(i), []byte(strconv.Itoa(i)+":"+string(utils.RandStringBytesRmndr(50))))
			if err != nil {
				t.Error(err)
			}
			metricDiff <- struct{}{}
		}
	}()

	go func() {
		tree, err := NewBPlusTree[uint64]("../../build/bplus/test"+strconv.Itoa(2), 16*1024)
		tree.conf.fullRewrite = true
		if err != nil {
			t.Error(err)
		}

		for i := 1; i < num; i++ {
			err = tree.Add(uint64(i), []byte(strconv.Itoa(i)+":"+string(utils.RandStringBytesRmndr(50))))
			if err != nil {
				t.Error(err)
			}
			metricFull <- struct{}{}
		}
	}()

	var i = 0
	ticker := time.NewTicker(10 * time.Second)

	var fullChan chan struct{}
	var diffChan chan struct{}

	fullChan = nil
	diffChan = metricDiff

	for {
		select {
		case <-ticker.C:
			i++
			if diffCount > 0 {
				fmt.Println("diff rewrite", diffCount)
			}

			if fullCount > 0 {
				fmt.Println("full rewrite", fullCount)
			}

			if i%2 == 0 {
				fullChan = nil
				diffChan = metricDiff
			} else {
				diffChan = nil
				fullChan = metricFull
			}

			diffCount = 0
			fullCount = 0
		case <-fullChan:
			fullCount++
		case <-diffChan:
			diffCount++
		}
	}
}

func TestAddToDisk(t *testing.T) {

	rand.Seed(time.Now().Unix())
	num := 1000000

	tree, err := NewBPlusTree[uint64]("../../build/bplus/test.db", 16*1024)
	if err != nil {
		t.Error(err)
	}

	for i := 1; i < num; i++ {
		err = tree.Add(uint64(i), []byte(strconv.Itoa(i)+":"+string(utils.RandStringBytesRmndr(50))))
		if err != nil {
			t.Error(err)
		}
		// fmt.Printf("%d: \n%s \n\n\n", i, tree.String())
	}

	t.Logf("tree %+v , 总计: %d ,节点总数: %d ", tree.meta.page.meta, tree.Size(), tree.NodeCount())
	t.Logf("tree.Get(1233) = %s", string(tree.Get(1233)))
	// for i := 2; i < num; i += 2 {
	// tree.Remove(uint64(i))
	// t.Logf("%d: \n%s \n\n\n", i, tree.String(5))
	// }

	t.Logf("tree %+v , 总计: %d ,节点总数: %d ", tree.meta.page.meta, tree.Size(), tree.NodeCount())
	t.Logf("tree.Get(1233) = %s", string(tree.Get(1233)))
}

func TestResotreFromDisk(t *testing.T) {

	tree, err := NewBPlusTree[uint64]("../../build/bplus/test.db", 16*1024)

	if err != nil {
		t.Error(err)
	}

	t.Logf("tree %+v , 总计: %d ,节点总数: %d ", tree.meta.page.meta, tree.Size(), tree.NodeCount())
	tree.ListAll("../../build/bplus/export.txt")

	t.Logf("tree.Get(1233) = %s", string(tree.Get(1233)))
}

func TestResotreDelete(t *testing.T) {
	num := 1000000
	tree, err := NewBPlusTree[uint64]("../../build/bplus/test.db", 16*1024)

	if err != nil {
		t.Error(err)
	}

	t.Logf("tree %+v , 总计: %d ,节点总数: %d ", tree.meta.page.meta, tree.Size(), tree.NodeCount())

	for i := 2; i < num; i += 2 {
		tree.Remove(uint64(i))
	}

	t.Logf("tree %+v , 总计: %d ,节点总数: %d ", tree.meta.page.meta, tree.Size(), tree.NodeCount())
}

func TestAdd(t *testing.T) {

	num := 1000000

	tree, err := NewBPlusTree[uint64]("../../build/bplus/test.db", 16*1024)

	if err != nil {
		t.Error(err)
	}

	for i := 1; i < num; i++ {
		tree.Add(uint64(i), []byte(strconv.Itoa(i)))
		// t.Logf("%d: \n%s \n\n\n", i, tree.String())
	}

	total := tree.Size()
	if total != uint64(num-1) {
		t.Errorf("新增数据出错, 总数 %d, 实际 %d", num-1, total)
	}

	if string(tree.Get(993999)) != strconv.Itoa(993999) {
		t.Errorf("获取数据出错, 期望 %s, 实际 %s", strconv.Itoa(993999), string(tree.Get(993999)))
	}

}

func TestRemove(t *testing.T) {

	tree, err := NewBPlusTree[uint64]("../../build/bplus/test.db", 16*1024)

	if err != nil {
		t.Error(err)
	}

	for i := 2; i < 1000000; i++ {
		tree.Add(uint64(i), []byte(strconv.Itoa(i)))
	}
	// t.Logf("%s \n\n\n", tree.String(5))

	for i := 2; i < 1000000; i++ {
		tree.Remove(uint64(i))
		// t.Logf("%d: \n%s \n\n\n", i, tree.String(5))
	}

	remian := tree.Size()
	if remian != 1 || string(tree.Get(1)) != "1" {
		t.Errorf("删除数据出错,期望剩余 1,实际剩余 %d ,值 %s", remian, string(tree.Get(1)))
	}
}
