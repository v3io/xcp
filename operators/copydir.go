package operators

import (
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/xcp/backends"
	"io"
	"path"
	"strings"
	"sync"
	"sync/atomic"
)

func endWithSlash(path string) bool {
	return strings.HasSuffix(path, "/") || strings.HasSuffix(path, "\\")
}

func CopyDir(task *backends.ListDirTask, target *backends.PathParams, logger logger.Logger, workers int) error {
	fileChan := make(chan *backends.FileDetails, 1000)
	summary := &backends.ListSummary{}

	if task.Source.Path != "" && !endWithSlash(task.Source.Path) {
		task.Source.Path += "/"
	}

	logger.InfoWith("copy task", "from", task.Source, "to", target)
	client, err := backends.GetNewClient(logger, task.Source)
	if err != nil {
		return errors.Wrap(err, "failed to get list source")
	}

	errChan := make(chan error, 60)

	go func(errChan chan error) {
		var err error
		err = client.ListDir(fileChan, task, summary)
		if err != nil {
			errChan <- errors.Wrap(err, "failed in list dir")
		}
	}(errChan)

	wg := sync.WaitGroup{}
	var transferred int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int, fileChan chan *backends.FileDetails, errChan chan error) {
			defer wg.Done()
			src, err := backends.GetNewClient(logger, task.Source)
			if err != nil {
				errChan <- errors.Wrap(err, "failed to get source")
				return
			}

			dst, err := backends.GetNewClient(logger, target)
			if err != nil {
				errChan <- errors.Wrap(err, "failed to get target")
				return
			}

			for f := range fileChan {
				relKeyPath := strings.TrimPrefix(f.Key, task.Source.Path)
				targetPath := path.Join(target.Path, relKeyPath)

				logger.DebugWith("copy file", "src", f.Key, "dst", targetPath,
					"bucket", target.Bucket, "size", f.Size, "mtime", f.Mtime)
				err = copyFile(dst, src, f, targetPath)
				if err != nil {
					errChan <- errors.Wrap(err, "failed in copy file")
					break
				}
				atomic.AddInt64(&transferred, 1)
			}
		}(i, fileChan, errChan)
	}

	wg.Wait()
	select {
	case err := <-errChan:
		logger.ErrorWith("copy loop failed", "err", err)
	default:

	}

	logger.Info("Total files: %d,  Total size: %d KB, Transferred %d files\n",
		summary.TotalFiles, summary.TotalBytes/1024, transferred)
	return nil
}

func copyFile(dst, src backends.FSClient, fileObj *backends.FileDetails, targetPath string) error {

	reader, err := src.Reader(fileObj.Key)
	if err != nil {
		return err
	}
	defer reader.Close()

	writer, err := dst.Writer(targetPath, nil)
	if err != nil {
		return err
	}
	_, err = io.CopyN(writer, reader, fileObj.Size)
	if err != nil {
		writer.Close()
		return err
	}
	return writer.Close()
}
