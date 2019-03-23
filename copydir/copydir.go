package copydir

import (
	"fmt"
	"github.com/nuclio/logger"
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

	logger.DebugWith("copy task", "from", task.Source, "to", task.Target)
	client, err := backends.GetNewClient(logger, task.Source)
	if err != nil {
		return err
	}

	errChan := make(chan error, 60)

	go func(errChan chan error) {
		var err error
		err = client.ListDir(fileChan, task, summary)
		if err != nil {
			errChan <- err
		}
	}(errChan)

	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int, fileChan chan *backends.FileDetails, errChan chan error) {
			defer wg.Done()
			src, err := backends.GetNewClient(logger, task.Source)
			if err != nil {
				errChan <- err
				return
			}

			dst, err := backends.GetNewClient(logger, task.Target)
			if err != nil {
				errChan <- err
				return
			}

			for f := range fileChan {
				relKeyPath := strings.TrimPrefix(f.Key, task.Source.Path)
				targetPath := path.Join(task.Target.Path, relKeyPath)

				logger.DebugWith("copy file", "src", f.Key, "dst", targetPath,
					"bucket", task.Target.Bucket, "size", f.Size, "mtime", f.Mtime)
				err = copyFile(dst, src, f, targetPath)
				if err != nil {
					fmt.Println(err)
					errChan <- err
					break
				}
			}
		}(i, fileChan, errChan)
	}

	wg.Wait()
	select {
	case err := <-errChan:
		fmt.Println("Err:", err)
	default:

	}

	fmt.Printf("Transferred - Total files: %d,  Total size: %d KB\n", summary.TotalFiles, summary.TotalBytes/1024)
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
