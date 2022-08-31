package utils

import (
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"
)

func CheckDir(dir string, cbs []func(int, int, string, fs.FileInfo)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	maxSeqNo := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info := strings.Split(entry.Name(), ".")
		if len(info) != 3 {
			continue
		}

		level, err := strconv.Atoi(info[0])
		if err != nil {
			continue
		}

		seqNo, err := strconv.Atoi(info[1])
		if err != nil {
			continue
		}

		fileInfo, err := entry.Info()
		if err != nil {
			continue
		}

		if fileInfo.Size() == 0 {
			os.Remove(path.Join(dir, entry.Name()))
			continue
		}

		if level == 0 && seqNo > maxSeqNo {
			maxSeqNo = seqNo
		}

		for _, cb := range cbs {
			cb(level, seqNo, info[2], fileInfo)
		}
	}
	return nil
}
