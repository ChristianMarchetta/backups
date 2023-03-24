package backups

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/cmar0027/backups/pkg/concurrency"
	ctxerr "github.com/cmar0027/backups/pkg/ctx-err"
	"github.com/cmar0027/backups/pkg/retries"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Args struct {
	AccessKeyID     string
	SecretAccessKey string
	EndPoint        string
	Bucket          string
	Region          string
}

const mkDirMode = os.ModeDir | 0755

// S3Transferrer is a Transferrer that backups and restores files to and from S3 compatible storage
type S3Transferrer struct {
	s3     *minio.Client
	s3Args S3Args
	*RestorerArgs
	*BackupperArgs
	concurrency *concurrency.ConcurrencyHandler

	rateLimiter <-chan time.Time
	limiter     retries.RateLimiter
	ctxerr.CtxErrCanceler
}

func NewS3Transferrer(s3Args S3Args, concurrentTransfers int, f context.CancelFunc, rateLimitingArgs retries.RateLimiterArgs, restorerArgs *RestorerArgs, backuperArgs *BackupperArgs) *S3Transferrer {
	client, err := minio.New(s3Args.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3Args.AccessKeyID, s3Args.SecretAccessKey, ""),
		Secure: true,
	})
	if err != nil {
		log.Fatalln(err)
	}

	return &S3Transferrer{
		s3:          client,
		s3Args:      s3Args,
		concurrency: concurrency.NewConcurrency(concurrentTransfers),
		CtxErrCanceler: ctxerr.CtxErrCanceler{
			CancelFunc: f,
		},
		limiter:       *retries.NewRateLimiter(rateLimitingArgs),
		RestorerArgs:  restorerArgs,
		BackupperArgs: backuperArgs,
	}
}

func (t *S3Transferrer) Wait() {
	t.concurrency.Wait()
	t.limiter.Stop()
}

// Restore restores one or more files from S3
//   - If ft.Remote ends with a '/', it is considered a directory and all files in that directory will be restored.
//     In this case, ft.Local must be a directory as well.
//   - If ft.Remote ends with a '/*', it is considered a prefix and all files with that prefix will be restored.
//     In this case, ft.Local must be a directory as well.
//   - If ft.Remote does not end with any of '/' and '/*', it will be considered as a file and will be restored
//     In this case, ft.Local must be a file as well.
func (t *S3Transferrer) Restore(ctx context.Context, ft FileTransfer) {
	fileType, err := getLocalFileCondition(ft.Local)
	if err != nil {
		t.Cancel(err)
		return
	}

	if strings.HasSuffix(ft.Remote, "/") || strings.HasSuffix(ft.Remote, "/*") {
		switch fileType {
		case IsFile:
			t.Cancel(fmt.Errorf("Cannot restore %v because local path is not a directory", ft))
			return
		case NotExist:
			err = os.MkdirAll(ft.Local, mkDirMode)
			if err != nil {
				t.Cancel(fmt.Errorf("Cannot restore %v because cannot create destination directory %s: %w", ft, ft.Local, err))
				return
			}
		}
	}

	if strings.HasSuffix(ft.Remote, "/") {
		err = t.restoreDir(ctx, ft)
	} else if strings.HasSuffix(ft.Remote, "/*") {
		err = t.restorePrefix(ctx, FileTransfer{
			Remote: strings.TrimSuffix(ft.Remote, "*"),
			Local:  ft.Local,
		})
	} else {
		err = t.restoreFile(ctx, ft, fileType)
	}

	if err != nil {
		t.Cancel(fmt.Errorf("Cannot restore %v: %w", ft, err))
	}
}

func (t *S3Transferrer) restoreDir(ctx context.Context, ft FileTransfer) error {

	objs := t.s3.ListObjects(ctx, t.s3Args.Bucket, minio.ListObjectsOptions{
		Prefix:    ft.Remote,
		Recursive: false,
	})

	for obj := range objs {
		if obj.Err != nil {
			return obj.Err
		}
		if strings.HasSuffix(obj.Key, "/") {
			continue
		}
		func(obj minio.ObjectInfo) {
			t.concurrency.NewTask(func() {
				objName := path.Base(obj.Key)

				// Construct the destination path
				dst := path.Join(ft.Local, objName)

				if !t.Force {
					fileType, err := getLocalFileCondition(dst)
					if err != nil {
						t.Cancel(err)
						return
					}

					if fileType != NotExist {
						log.Printf("Ignored remote file %s from restore %v because local file already exists.", objName, ft)
						return
					}
				}
				err := t.restore(ctx, FileTransfer{Remote: obj.Key, Local: dst})
				if err != nil {
					t.Cancel(err)
					return
				}
				log.Println("Restored", obj.Key, "to", dst, "of", ft)
			})
		}(obj)
	}

	return nil
}

func (t *S3Transferrer) restorePrefix(ctx context.Context, ft FileTransfer) error {

	objs := t.s3.ListObjects(ctx, t.s3Args.Bucket, minio.ListObjectsOptions{
		Prefix:    ft.Remote,
		Recursive: true,
	})

	for obj := range objs {
		if obj.Err != nil {
			return obj.Err
		}

		if strings.HasSuffix(obj.Key, "/") {
			continue
		}
		func(obj minio.ObjectInfo) {
			t.concurrency.NewTask(func() {
				objName := strings.TrimPrefix(obj.Key, ft.Remote)

				if strings.HasPrefix(objName, "/") {
					objName = strings.TrimPrefix(objName, "/")
				}

				if strings.HasSuffix(objName, "/") {
					objName = strings.TrimSuffix(objName, "/")
				}

				// Construct the destination path
				dst := path.Join(ft.Local, objName)

				parent := path.Dir(dst)

				err := os.MkdirAll(parent, mkDirMode)
				if err != nil {
					t.Cancel(fmt.Errorf("Cannot prefix restore %v because cannot create destination directory %s: %w", ft, parent, err))
				}

				if !t.Force {
					fileType, err := getLocalFileCondition(dst)
					if err != nil {
						t.Cancel(err)
						return
					}

					if fileType != NotExist {
						log.Printf("Ignored remote file %s from prefix restore %v because local file already exists.", objName, ft)
						return
					}
				}

				err = t.restore(ctx, FileTransfer{Remote: obj.Key, Local: dst})
				if err != nil {
					t.Cancel(err)
					return
				}
				log.Println("Restored", obj.Key, "to", dst, "of", ft)
			})
		}(obj)
	}

	return nil
}

func (t *S3Transferrer) restoreFile(ctx context.Context, ft FileTransfer, fileType FileType) error {

	switch fileType {
	case IsFile:
		if !t.Force {
			log.Printf("Ignored restore %v because local file already exists.", ft)
			return nil
		}
	case IsDir:
		return fmt.Errorf("local path is a directory")
	}

	t.concurrency.NewTask(func() {
		parent := path.Dir(ft.Local)
		err := os.MkdirAll(parent, mkDirMode)
		if err != nil {
			t.Cancel(fmt.Errorf("cannot create parent directory %s: %w", parent, err))
			return
		}

		err = t.restore(ctx, ft)
		if err != nil {
			t.Cancel(err)
			return
		}
		log.Println("Restored", ft)
	})
	return nil
}

func (t *S3Transferrer) restore(ctx context.Context, ft FileTransfer) error {
	var obj *minio.Object
	var err error
	err = t.limiter.Retry(func() error {
		obj, err = t.s3.GetObject(ctx, t.s3Args.Bucket, ft.Remote, minio.GetObjectOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("cannot get object %s: %w", ft.Remote, err)
	}
	defer obj.Close()

	dst, err := os.Create(ft.Local)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = dst.ReadFrom(obj)
	if err != nil {
		return fmt.Errorf("cannot write %s to %s: %w", ft.Remote, ft.Local, err)
	}
	return nil
}

// Backup backups one or more files to S3
//   - If ft.Local ends with a '/', it is considered a directory and all files in that directory will be restored.
//   - If ft.Local ends with a '*', it is considered a directory and all files in that directory will be restored recursively.
//   - If ft.Local does not end with any of '/' and '*', it will be considered a file.
func (t *S3Transferrer) Backup(ctx context.Context, ft FileTransfer) {
	var err error
	if strings.HasSuffix(ft.Local, "/") {
		err = t.backupDir(ctx, ft, false)
	} else if strings.HasSuffix(ft.Local, "/*") {
		err = t.backupDir(ctx, FileTransfer{
			Remote: ft.Remote,
			Local:  strings.TrimSuffix(ft.Local, "*"),
		}, true)
	} else {
		err = t.backupFile(ctx, ft)
	}

	if err != nil {
		t.Cancel(fmt.Errorf("Cannot backup %v: %w", ft, err))
	}
}

func (t *S3Transferrer) backupDir(ctx context.Context, ft FileTransfer, recursive bool) error {

	files, err := t.walkDir(ctx, ft.Local, recursive, 50)
	if err != nil {
		return err
	}

	for file := range files {
		func(file string) {
			t.concurrency.NewTask(func() {
				remote := path.Join(ft.Remote, strings.TrimPrefix(file, ft.Local))
				err := t.backup(ctx, FileTransfer{
					Remote: remote,
					Local:  file,
				})
				if err != nil {
					t.Cancel(err)
					return
				}
				log.Println("Backed up", file, "to", remote, "of", ft)
			})
		}(file)
	}

	return nil
}

func (t *S3Transferrer) backupFile(ctx context.Context, ft FileTransfer) error {
	t.concurrency.NewTask(func() {
		err := t.backup(ctx, ft)
		if err != nil {
			t.Cancel(err)
			return
		}
		log.Println("Backed up", ft)
	})
	return nil
}

func (t *S3Transferrer) backup(ctx context.Context, ft FileTransfer) error {
	f, err := os.Open(ft.Local)
	if err != nil {
		return err
	}
	defer f.Close()

	stats, err := f.Stat()
	if err != nil {
		return err
	}

	if stats.IsDir() {
		return fmt.Errorf("local path is a directory")
	}

	if t.SkipNotMofiedFor > 0 {
		// check if remote file exists
		shouldSkip := false
		err := t.limiter.Retry(func() error {
			remoteStat, err := t.s3.StatObject(ctx, t.s3Args.Bucket, ft.Remote, minio.StatObjectOptions{})
			if err != nil {
				errResponse := minio.ToErrorResponse(err)
				if errResponse.Code == "NoSuchKey" {
					// remote file does not exist
					log.Printf("Backing up %v because remote does not exist.", ft)
				} else {
					return fmt.Errorf("cannot stat object %s: %w", ft.Remote, err)
				}
			} else {
				// remote file exists
				if stats.ModTime().After(remoteStat.LastModified) {
					log.Printf("Backing up %v because local file is newer.", ft)
				} else if stats.ModTime().After(time.Now().Add(t.BackupperArgs.SkipNotMofiedFor * -1)) {
					log.Printf("Backing up %v because local was modified in the past %s.", ft, t.BackupperArgs.SkipNotMofiedFor)
				} else {
					log.Printf("Skipping %v because local wasn't modified in the past %s.", ft, t.BackupperArgs.SkipNotMofiedFor)
					shouldSkip = true
					return nil
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if shouldSkip {
			return nil
		}
	}

	err = t.limiter.Retry(func() error {
		_, err = t.s3.PutObject(ctx, t.s3Args.Bucket, ft.Remote, f, stats.Size(), minio.PutObjectOptions{})
		return err
	})
	return err
}

type recursiveDir struct {
	parent string
	dir    fs.FileInfo
}

// WalkDir returns a channel of all file names in the root directory.
// If recursive is true, it will also return all files in subdirectories.
// WalkDir spawns a goroutine that reads n filenames at a time.
func (t *S3Transferrer) walkDir(ctx context.Context, root string, recursive bool, n int) (<-chan string, error) {
	if n < 1 {
		return nil, fmt.Errorf("n must be greater than 0")
	}

	stats, err := os.Stat(root)
	if err != nil {
		return nil, err
	}

	if !stats.IsDir() {
		return nil, fmt.Errorf("local path is not a directory")
	}

	dirStack := []recursiveDir{{path.Dir(root), stats}}
	if strings.HasSuffix(root, "/") {
		dirStack[0].parent = path.Dir(dirStack[0].parent)
	}
	files := make(chan string)
	var f *os.File = nil
	var recD recursiveDir
	close := func() {
		if f != nil {
			f.Close()
		}
		close(files)
	}
	go func() {
		for {
			// Stop if the context is cancelled
			if ctx.Err() != nil {
				close()
				return
			}

			// open the next file in the stack
			if f == nil {
				if len(dirStack) == 0 {
					close()
					return
				}
				recD = dirStack[len(dirStack)-1]
				dirStack = dirStack[:len(dirStack)-1]
				f, err = os.Open(path.Join(recD.parent, recD.dir.Name()))
				if err != nil {
					t.Cancel(err)
					close()
					return
				}
			}

			entries, err := f.Readdir(n)
			if err == io.EOF {
				f.Close()
				f = nil
				continue
			}
			if err != nil {
				t.Cancel(err)
				close()
				return
			}

			currentPath := path.Join(recD.parent, recD.dir.Name())

			for _, entry := range entries {
				if entry.IsDir() {
					if recursive {
						dirStack = append(dirStack, recursiveDir{
							path.Join(currentPath),
							entry,
						})
					}
					continue
				}
				files <- filepath.Join(currentPath, entry.Name())
			}
		}
	}()

	return files, nil
}
