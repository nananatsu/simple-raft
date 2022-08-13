package filter

import (
	"fmt"
	"testing"
)

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
