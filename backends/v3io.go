package backends

import (
	"bytes"
	"fmt"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	v3iohttp "github.com/v3io/v3io-go/pkg/dataplane/http"
	"io"
	"path/filepath"
	"strings"
	"time"
)

const defaultWorkers = 0

const (
	V3ioPathEnvironmentVariable       = "V3IO_API"
	V3ioUserEnvironmentVariable       = "V3IO_USERNAME"
	V3ioPasswordEnvironmentVariable   = "V3IO_PASSWORD"
	V3ioSessionKeyEnvironmentVariable = "V3IO_ACCESS_KEY"
)

type V3ioClientOpts struct {
	WebApiEndpoint string `json:"webApiEndpoint"`
	Container      string `json:"container"`
	Username       string `json:"username,omitempty"`
	Password       string `json:"password,omitempty"`
	SessionKey     string `json:"sessionKey,omitempty"`
	// Logging level (for verbose output) - "debug" | "info" | "warn" | "error"
	LogLevel string `json:"logLevel,omitempty"`
}

type V3ioClient struct {
	params    *PathParams
	container v3io.Container
	logger    logger.Logger
	task      *ListDirTask
	path      string
}

func NewV3ioClient(logger logger.Logger, params *PathParams) (FSClient, error) {

	if params.Token == "" {
		params.UserKey = defaultFromEnv(params.UserKey, V3ioUserEnvironmentVariable)
		params.Secret = defaultFromEnv(params.Secret, V3ioPasswordEnvironmentVariable)
		params.Token = defaultFromEnv(params.Token, V3ioSessionKeyEnvironmentVariable)
	}
	params.Endpoint = defaultFromEnv(params.Endpoint, V3ioPathEnvironmentVariable)

	config := v3io.NewSessionInput{
		Username:  params.UserKey,
		Password:  params.Secret,
		AccessKey: params.Token}

	newContainer, err := CreateContainer(logger, params.Endpoint, params.Bucket, &config, 0)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize a data container.")
	}

	newClient := V3ioClient{params: params, container: newContainer, logger: logger}
	return &newClient, err
}

func (c *V3ioClient) ListDir(fileChan chan *FileDetails, task *ListDirTask, summary *ListSummary) error {
	//bucket, keyPrefix := splitPath(searcher.Path)
	defer close(fileChan)
	c.task = task
	c.path = c.params.Path

	return c.getDir(c.path, fileChan, summary)
}

func (c *V3ioClient) getDir(path string, fileChan chan *FileDetails, summary *ListSummary) error {

	resp, err := c.container.GetContainerContentsSync(&v3io.GetContainerContentsInput{Path: path})
	if err != nil {
		c.logger.ErrorWith("ListBucket failed", "endpoint", c.params.Endpoint, "container",
			c.params.Bucket, "path", path)
		return errors.Wrap(err, "failed v3io ListBucket")
	}
	result := resp.Output.(*v3io.GetContainerContentsOutput)

	for _, obj := range result.Contents {

		if strings.HasSuffix(obj.Key, "/") {
			continue
		}

		t, err := time.Parse(time.RFC3339, obj.LastModified)
		if err != nil {
			return errors.Wrap(err, "Invalid object time string - not an RFC 3339 time format.")
		}
		var size int64
		if obj.Size != nil {
			size = int64(*obj.Size)
		}
		_, name := filepath.Split(obj.Key)

		if !IsMatch(c.task, name, t, size) {
			continue
		}

		c.logger.DebugWith("List dir:", "key", obj.Key, "modified", obj.LastModified, "size", obj.Size)
		fileDetails := &FileDetails{
			Key: obj.Key, Size: size, //Mtime: obj.LastModified,
		}

		summary.TotalBytes += size
		summary.TotalFiles += 1
		fileChan <- fileDetails
	}

	if c.task.Recursive {
		for _, val := range result.CommonPrefixes {
			_, name := filepath.Split(val.Prefix[0 : len(val.Prefix)-1])
			if c.task.Hidden || !strings.HasPrefix(name, ".") {
				err = c.getDir(val.Prefix, fileChan, summary)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *V3ioClient) Reader(path string) (FSReader, error) {
	resp, err := c.container.GetObjectSync(&v3io.GetObjectInput{Path: path})
	if err != nil {
		return nil, fmt.Errorf("Error in GetObject operation (%v)", err)
	}

	return &v3ioReader{reader: bytes.NewReader(resp.Body())}, nil
}

type v3ioReader struct {
	reader io.Reader
}

func (r *v3ioReader) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *v3ioReader) Close() error {
	return nil
}

func (r *v3ioReader) Stat() (*FileMeta, error) {
	// TBD return Mtime, ..
	meta := FileMeta{}
	return &meta, nil
}

func (c *V3ioClient) Writer(path string, opts *FileMeta) (io.WriteCloser, error) {
	return &v3ioWriter{path: path, container: c.container, opts: opts}, nil
}

type v3ioWriter struct {
	path      string
	buf       []byte
	opts      *FileMeta
	container v3io.Container
}

func (w *v3ioWriter) Write(p []byte) (n int, err error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *v3ioWriter) Close() error {
	// TBD write time, mode, kv metadata
	return w.container.PutObjectSync(&v3io.PutObjectInput{Path: w.path, Body: w.buf})
}

func CreateContainer(logger logger.Logger, addr, cont string, config *v3io.NewSessionInput, workers int) (v3io.Container, error) {
	// Create context
	contextInput := v3io.NewContextInput{ClusterEndpoints: []string{addr}, NumWorkers: workers}
	context, err := v3iohttp.NewContext(logger, &contextInput)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a V3IO client.")
	}

	// Create session
	session, err := context.NewSession(config)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a session.")
	}

	// Create the container
	container, err := session.NewContainer(&v3io.NewContainerInput{ContainerName: cont})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a container.")
	}

	return container, nil
}
