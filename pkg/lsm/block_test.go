package lsm

import (
	"log"
	"testing"
)

func TestCompress(t *testing.T) {

	b := NewBlock()

	b.Append([]byte("heelo"), []byte("woorld"))
	b.Append([]byte("heal@"), []byte("w00rld"))
	b.Append([]byte("he1lo"), []byte("woor1d"))
	b.Append([]byte("h-elo"), []byte("w@@rld"))

	log.Println(string(b.compress()))

}

func TestGetSeparator(t *testing.T) {

	a := []byte("hello")
	b := []byte("hello n")

	log.Println(string(GetSeparator(a, b)))
}
