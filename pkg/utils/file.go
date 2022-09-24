package utils

import (
	"io/fs"
	"os"
	"path"
)

func CheckDir(dir string, cbs []func(string, fs.FileInfo)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
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

		for _, cb := range cbs {
			cb(entry.Name(), fileInfo)
		}
	}
	return nil
}
