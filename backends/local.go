package backends

import (
	"github.com/nuclio/logger"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
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

func (c *LocalClient) ListDir(fileChan chan *FileDetails, task *ListDirTask, summary *ListSummary) error {
	defer close(fileChan)

	visit := func(localPath string, fi os.FileInfo, err error) error {
		if err != nil {
			c.logger.Error("List walk error with path %s, %v", localPath, err)
			return err
		}
		localPath = filepath.ToSlash(localPath)

		if fi.IsDir() {
			relPath := strings.TrimPrefix(localPath, filepath.ToSlash(c.params.Path))
			if (relPath != "" && !task.Recursive) || (!task.Hidden && strings.HasPrefix(fi.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}

		if fi.Mode()&os.ModeSymlink != 0 {
			return nil
		}

		if !IsMatch(task, fi.Name(), fi.ModTime(), fi.Size()) {
			return nil
		}

		fileDetails := &FileDetails{
			Key: localPath, Size: fi.Size(), Mtime: fi.ModTime(), Mode: uint32(fi.Mode()),
		}
		c.logger.DebugWith("List file", "key", localPath,
			"modified", fi.ModTime(), "size", fi.Size(), "mode", uint32(fi.Mode()))

		summary.TotalBytes += fi.Size()
		summary.TotalFiles += 1
		fileChan <- fileDetails

		return nil
	}

	return filepath.Walk(c.params.Path, visit)
}

func (c *LocalClient) Reader(path string) (FSReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &fileReader{f}, err
}

type fileReader struct {
	f *os.File
}

func (r *fileReader) Read(p []byte) (n int, err error) {
	return r.f.Read(p)
}

func (r *fileReader) Close() error {
	return r.f.Close()
}

func (r *fileReader) Stat() (*FileMeta, error) {
	stat, err := r.f.Stat()
	if err != nil {
		return nil, err
	}
	meta := FileMeta{Mtime: stat.ModTime(), Mode: uint32(stat.Mode())}
	return &meta, err
}

func (c *LocalClient) Writer(path string, opts *FileMeta) (io.WriteCloser, error) {
	if err := ValidFSTarget(path); err != nil {
		return nil, err
	}

	mode := uint32(0666)
	if opts != nil && opts.Mode > 0 {
		mode = opts.Mode
	}

	f, err := os.OpenFile(
		path,
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		os.FileMode(mode),
	)

	if err != nil || opts == nil || opts.Mtime.IsZero() {
		return f, err
	}

	return &fileWriter{f: f, mtime: opts.Mtime, path: path}, nil
}

type fileWriter struct {
	f     *os.File
	mtime time.Time
	path  string
}

func (w *fileWriter) Write(p []byte) (n int, err error) {
	return w.f.Write(p)
}

func (w *fileWriter) Close() error {
	err := w.f.Close()
	if err != nil {
		return err
	}
	if w.mtime.IsZero() {
		return nil
	}
	return os.Chtimes(w.path, w.mtime, w.mtime)
}
