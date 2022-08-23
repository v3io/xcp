/*
Copyright 2019 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package operators

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/v3io/xcp/backends"
	"io"
	"path"
	"strings"
	"sync"
	"sync/atomic"
)

func CopyDir(task *backends.ListDirTask, target *backends.PathParams, logger logger.Logger, workers int) error {
	fileChan := make(chan *backends.FileDetails, 1000)
	summary := &backends.ListSummary{}
	withMeta := task.WithMeta

	logger.InfoWith("copy task", "from", task.Source, "to", target)
	client, err := backends.GetNewClient(logger, task.Source)
	if err != nil {
		return fmt.Errorf("failed to get list source, %v", err)
	}

	errChan := make(chan error, 60)

	go func(errChan chan error) {
		var err error
		err = client.ListDir(fileChan, task, summary)
		if err != nil {
			errChan <- fmt.Errorf("failed in list dir, %v", err)
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
				errChan <- fmt.Errorf("failed to get source, %v", err)
				return
			}

			dst, err := backends.GetNewClient(logger, target)
			if err != nil {
				errChan <- fmt.Errorf("failed to get target, %v", err)
				return
			}

			for f := range fileChan {
				relKeyPath := strings.TrimPrefix(f.Key, task.Source.Path)
				targetPath := path.Join(target.Path, relKeyPath)

				logger.DebugWith("copy file", "src", f.Key, "dst", targetPath,
					"bucket", target.Bucket, "size", f.Size, "mtime", f.Mtime)
				err = copyFile(dst, src, f, targetPath, withMeta)
				if err != nil {
					errChan <- fmt.Errorf("failed in copy file, %v", err)
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

func copyFile(dst, src backends.FSClient, fileObj *backends.FileDetails, targetPath string, withMeta bool) error {

	reader, err := src.Reader(fileObj.Key)
	if err != nil {
		return err
	}
	defer reader.Close()

	opts := backends.FileMeta{}
	if withMeta {
		opts.Mode = fileObj.Mode
		opts.Mtime = fileObj.Mtime
	}

	writer, err := dst.Writer(targetPath, &opts)
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
