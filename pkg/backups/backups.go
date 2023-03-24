package backups

import (
	"fmt"
	"time"
)

type FileTransfer struct {
	Remote string
	Local  string
}

func (ft FileTransfer) String() string {
	return fmt.Sprintf("[Remote: %s, Local: %s]", ft.Remote, ft.Local)
}

type RestorerArgs struct {
	Force bool
}

type BackupperArgs struct {
	SkipNotMofiedFor time.Duration
}

type Transferrer interface {
	Backup(ft FileTransfer) error
	Restore(ft FileTransfer) error
}
