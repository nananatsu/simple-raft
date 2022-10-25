package filter

import (
	"fmt"
	"kvdb/pkg/utils"
	"testing"
)

func BenchmarkHashMurmur3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100000; j++ {
			MurmurHash3(utils.RandStringBytesRmndr(10), 0xbc9f1d34)
		}

	}
}

func BenchmarkHashLevldb(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100000; j++ {
			LeveldbHash(utils.RandStringBytesRmndr(10), 0xbc9f1d34)
		}
	}
}

func TestBloomFilter(t *testing.T) {

	bf := NewBloomFilter(10)

	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))
	bf.Add([]byte("iris"))
	bf.Add([]byte("freesia"))
	bf.Add([]byte("fire"))
	bf.Add([]byte("hell"))

	filter := bf.Hash()

	fmt.Println(Contains(filter, []byte("world")))
	fmt.Println(Contains(filter, []byte("world1")))
	fmt.Println(Contains(filter, []byte("adcxs")))

}
