package copydir

import (
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/xcp/backends"
	"io"
	"path"
	"strings"
	"sync"
)

func RunTask(task *backends.CopyTask, logger logger.Logger, workers int) error {
	fileChan := make(chan *backends.FileDetails, 1000)
	summary := &backends.ListSummary{}

	if !(strings.HasSuffix(task.Source.Path, "/") || strings.HasSuffix(task.Source.Path, "\\")) {
		task.Source.Path += "/"
	}

	logger.InfoWith("copy task", "from", task.Source, "to", task.Target)
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
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int, fileChan chan *backends.FileDetails, errChan chan error) {
			defer wg.Done()
			src, err := backends.GetNewClient(logger, task.Source)
			if err != nil {
				errChan <- errors.Wrap(err, "failed to get source")
				return
			}

			dst, err := backends.GetNewClient(logger, task.Target)
			if err != nil {
				errChan <- errors.Wrap(err, "failed to get target")
				return
			}

			for f := range fileChan {
				relKeyPath := strings.TrimPrefix(f.Key, task.Source.Path)
				targetPath := path.Join(task.Target.Path, relKeyPath)

				logger.DebugWith("copy file", "src", f.Key, "dst", targetPath,
					"bucket", task.Target.Bucket, "size", f.Size, "mtime", f.Mtime)
				err = copyFile(dst, src, f, targetPath)
				if err != nil {
					errChan <- errors.Wrap(err, "failed in copy file")
					break
				}
			}
		}(i, fileChan, errChan)
	}

	wg.Wait()
	select {
	case err := <-errChan:
		logger.ErrorWith("copy loop failed", "err", err)
	default:

	}

	logger.Info("Transferred - Total files: %d,  Total size: %d KB\n", summary.TotalFiles, summary.TotalBytes/1024)
	return nil
}

func copyFile(dst, src backends.FSClient, fileObj *backends.FileDetails, targetPath string) error {

	reader, err := src.Reader(fileObj.Key)
	if err != nil {
		return err
	}
	defer reader.Close()

	writer, err := dst.Writer(targetPath)
	if err != nil {
		return err
	}
	defer writer.Close()
	io.CopyN(writer, reader, fileObj.Size)
	return nil
}
