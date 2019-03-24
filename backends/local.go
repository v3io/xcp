package backends

import (
	"github.com/nuclio/logger"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type LocalClient struct {
	logger logger.Logger
	params *PathParams
}

func NewLocalClient(logger logger.Logger, params *PathParams) (FSClient, error) {
	var err error
	params.Path, err = filepath.Abs(params.Path)
	if err == nil {
		params.Path = filepath.ToSlash(params.Path)
	}
	return &LocalClient{logger: logger, params: params}, err
}

func (c *LocalClient) ListDir(fileChan chan *FileDetails, task *CopyTask, summary *ListSummary) error {
	defer close(fileChan)

	visit := func(localPath string, fi os.FileInfo, err error) error {
		localPath = filepath.ToSlash(localPath)

		if !task.Hidden && strings.HasPrefix(fi.Name(), ".") {
			if fi.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if fi.IsDir() {
			relPath := strings.TrimPrefix(localPath, filepath.ToSlash(c.params.Path))
			if relPath != "" && !task.Recursive {
				return filepath.SkipDir
			}
			return nil
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if !task.Since.IsZero() && fi.ModTime().Before(task.Since) {
			return nil
		}
		if (fi.Size() < task.MinSize) || (task.MaxSize > 0 && fi.Size() > task.MaxSize) {
			return nil
		}
		if task.Filter != "" {
			match, err := filepath.Match(task.Filter, fi.Name())
			if err != nil || !match {
				return err
			}
		}

		fileDetails := &FileDetails{
			Key: localPath, Size: fi.Size(), Mtime: fi.ModTime(),
		}
		c.logger.DebugWith("List file", "key", localPath, "modified", fi.ModTime(), "size", fi.Size())

		summary.TotalBytes += fi.Size()
		summary.TotalFiles += 1
		fileChan <- fileDetails

		return nil
	}

	return filepath.Walk(c.params.Path, visit)
}

func (c *LocalClient) Reader(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (c *LocalClient) Writer(path string) (io.WriteCloser, error) {
	if err := ValidFSTarget(path); err != nil {
		return nil, err
	}

	return os.OpenFile(
		path,
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0666,
	)
}