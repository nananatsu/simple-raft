package utils

import "math/rand"

const letterBytes = "abcdefghijklmnopqrstuvwxyz"

func RandStringBytesRmndr(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return b
}
