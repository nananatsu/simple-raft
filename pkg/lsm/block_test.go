package lsm

import (
	"fmt"
	"log"
	"regexp"
	"testing"

	"go.uber.org/zap"
)

func TestCompress(t *testing.T) {

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	b := NewBlock(NewConfig("../../build/sst/", sugar))

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

func TestFile(t *testing.T) {

	reg := regexp.MustCompile(`^(?P<level>\d+)_(?P<seqNo>\d+)_(?P<extra>.*)\.sst$`)
	fmt.Println(len(reg.SubexpNames()), reg.SubexpNames())

	file := "1_5_222.sst"
	match := reg.FindStringSubmatch(file)

	fmt.Println(len(match), match)

	// for i, name := range SstFileNameReg.SubexpNames() {
	// 	if i != 0 && name != "" {
	// 		match[i]
	// 	}
	// }

	// var i uint64
	// var j uint64
	// for i = 0; i < 2; i++ {
	// 	for j = 0; j < 60; j++ {
	// 		name := fmt.Sprintf("%s.%s(%d.%d).sst", strconv.FormatUint(i, 16), strconv.FormatUint(j, 16), i, j)
	// 		os.OpenFile("../../build/sst/"+name, os.O_CREATE, 0644)
	// 	}
	// }
}
