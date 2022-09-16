package main

import (
	"os"
	"testing"
)

func TestMain(t *testing.T) {

	os.Args = append(os.Args, "-f")
	os.Args = append(os.Args, "../build/conf1.yaml")
	main()
}
