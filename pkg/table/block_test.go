package table

import (
	"fmt"
	"testing"
)

func TestCompress(t *testing.T) {

	b := NewBlock()

	b.Append([]byte("heelo"), []byte("woorld"))
	b.Append([]byte("heal@"), []byte("w00rld"))
	b.Append([]byte("he1lo"), []byte("woor1d"))
	b.Append([]byte("h-elo"), []byte("w@@rld"))

	fmt.Println(string(b.compress()))

}
