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
package backends

import (
	"fmt"
	"github.com/nuclio/logger"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const OriginalMtimeKey = "original_mtime"
const OriginalModeKey = "original_mode"

const OriginalMtimeS3Key = "X-Amz-Meta-Original_mtime"
const OriginalModeS3Key = "X-Amz-Meta-Original_mode"

type ListDirTask struct {
	Source    *PathParams
	Since     time.Time
	MinSize   int64
	MaxSize   int64
	Recursive bool
	InclEmpty bool
	Hidden    bool
	WithMeta  bool

	dir    string
	filter string
}

type FileDetails struct {
	Key   string
	Mtime time.Time
	Mode  uint32
	Size  int64
}

type ListSummary struct {
	TotalFiles int
	TotalBytes int64
}

type PathParams struct {
	Kind     string `json:"kind"`
	Endpoint string `json:"endpoint,omitempty"`
	Bucket   string `json:"bucket,omitempty"`
	Path     string `json:"path"`
	Tag      string `json:"tag,omitempty"`
	Secure   bool   `json:"secure,omitempty"`
	UserKey  string `json:"userKey,omitempty"`
	Secret   string `json:"secret,omitempty"`
	Token    string `json:"token,omitempty"`

	filter string
	isFile bool
}

func (p *PathParams) String() string {
	return fmt.Sprintf("%s://%s/%s/%s", p.Kind, p.Endpoint, p.Bucket, p.Path)
}

type FileMeta struct {
	Mtime time.Time
	Mode  uint32
	Attrs map[string]interface{}
}

type FSClient interface {
	ListDir(fileChan chan *FileDetails, task *ListDirTask, summary *ListSummary) error
	Reader(path string) (FSReader, error)
	Writer(path string, opts *FileMeta) (io.WriteCloser, error)
}

type FSReader interface {
	Read(p []byte) (n int, err error)
	Close() error
	Stat() (*FileMeta, error)
}

func GetNewClient(logger logger.Logger, params *PathParams) (FSClient, error) {
	switch strings.ToLower(params.Kind) {
	case "v3io":
		return NewV3ioClient(logger, params)
	case "s3":
		return NewS3Client(logger, params)
	case "", "file":
		return NewLocalClient(logger, params)
	default:
		return nil, fmt.Errorf("Unknown backend %s use s3, v3io or local", params.Kind)
	}
}

func ValidFSTarget(filePath string) error {
	// Verify if destination already exists.
	st, err := os.Stat(filePath)
	if err == nil {
		// If the destination exists and is a directory.
		if st.IsDir() {
			return fmt.Errorf("fileName %s is a directory.", filePath)
		}
	}

	// Proceed if file does not exist. return for all other errors.
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	// Extract top level directory.
	objectDir, _ := filepath.Split(filePath)
	if objectDir != "" {
		// Create any missing top level directories.
		if err := os.MkdirAll(objectDir, 0700); err != nil {
			return err
		}
	}

	return nil
}

func defaultFromEnv(param string, envvar string) string {
	if param == "" {
		param = os.Getenv(envvar)
	}
	return param
}

func IsMatch(task *ListDirTask, name string, mtime time.Time, size int64) bool {
	if !task.InclEmpty && size == 0 {
		return false
	}

	if !task.Hidden && strings.HasPrefix(name, ".") {
		return false
	}

	if !task.Since.IsZero() && mtime.Before(task.Since) {
		return false
	}

	if (size < task.MinSize) || (task.MaxSize > 0 && size > task.MaxSize) {
		return false
	}

	if task.Source.filter != "" {
		match, err := filepath.Match(task.Source.filter, name)
		if err != nil || !match {
			return false
		}
	}

	return true
}

// return is file, err
func ParseFilename(fullpath string, params *PathParams, forceDir bool) error {
	fullpath, filter := filepath.Split(fullpath)
	if hasMagics(fullpath) {
		return fmt.Errorf("No support for wildcard directory names")
	}
	params.Path = fullpath
	if filter == "" || hasMagics(filter) {
		params.filter = filter
		return nil
	}

	params.Path = fullpath + filter
	if forceDir && !endWithSlash(params.Path) {
		params.Path += "/"
	}
	params.isFile = true
	return nil
}

func hasMagics(text string) bool {
	for _, c := range text {
		if c == '*' || c == '?' || c == '[' {
			return true
		}
	}
	return false
}

func endWithSlash(path string) bool {
	return strings.HasSuffix(path, "/") || strings.HasSuffix(path, "\\")
}
