package backups

import "os"

type FileType int

const (
	IsFile FileType = iota
	IsDir
	NotExist
)

func getLocalFileCondition(path string) (FileType, error) {

	stats, err := os.Stat(path)

	if err != nil {
		if os.IsNotExist(err) {
			return NotExist, nil
		}
		return NotExist, err
	}

	if stats.IsDir() {
		return IsDir, nil
	}

	return IsFile, nil
}
