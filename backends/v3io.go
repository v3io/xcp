package backends

import (
	"bytes"
	"fmt"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"io"
	"net/url"
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
	// Number of parallel V3IO worker routines
}

type V3ioClient struct {
	params    *PathParams
	container *v3io.Container
	logger    logger.Logger
	task      *CopyTask
	path      string
}

func NewV3ioClient(logger logger.Logger, params *PathParams) (FSClient, error) {

	if params.Token == "" {
		params.UserKey = defaultFromEnv(params.UserKey, V3ioUserEnvironmentVariable)
		params.Secret = defaultFromEnv(params.Secret, V3ioPasswordEnvironmentVariable)
		params.Token = defaultFromEnv(params.Token, V3ioSessionKeyEnvironmentVariable)
	}
	params.Endpoint = defaultFromEnv(params.Endpoint, V3ioPathEnvironmentVariable)

	config := v3io.SessionConfig{
		Username:   params.UserKey,
		Password:   params.Secret,
		Label:      "xcopy",
		SessionKey: params.Token}

	newContainer, err := CreateContainer(logger, params.Endpoint, params.Bucket, &config, 0)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize a data container.")
	}

	newClient := V3ioClient{params: params, container: newContainer, logger: logger}
	return &newClient, err
}

func (c *V3ioClient) ListDir(fileChan chan *FileDetails, task *CopyTask, summary *ListSummary) error {
	//bucket, keyPrefix := splitPath(searcher.Path)
	defer close(fileChan)
	c.task = task
	c.path = c.params.Path

	return c.getDir(c.path, fileChan, summary)
}

func (c *V3ioClient) getDir(path string, fileChan chan *FileDetails, summary *ListSummary) error {

	resp, err := c.container.Sync.ListBucket(&v3io.ListBucketInput{Path: path})
	if err != nil {
		return err
	}
	result := resp.Output.(*v3io.ListBucketOutput)

	for _, obj := range result.Contents {

		if strings.HasSuffix(obj.Key, "/") {
			continue
		}

		_, name := filepath.Split(obj.Key)
		if !c.task.Hidden && strings.HasPrefix(name, ".") {
			continue
		}

		t, err := time.Parse(time.RFC3339, obj.LastModified+"Z")
		if err != nil {
			return errors.Wrap(err, "Invalid object time string - not an RFC 3339 time format.")
		}
		if !c.task.Since.IsZero() && t.Before(c.task.Since) {
			continue
		}

		size := int64(obj.Size)
		if (size < c.task.MinSize) || (c.task.MaxSize > 0 && size > c.task.MaxSize) {
			continue
		}

		if c.task.Filter != "" {
			match, err := filepath.Match(c.task.Filter, name)
			if err != nil || !match {
				continue
			}
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

func (c *V3ioClient) Reader(path string) (io.ReadCloser, error) {
	//_, path = SplitPath(path)  // remove the container prefix
	resp, err := c.container.Sync.GetObject(&v3io.GetObjectInput{Path: url.PathEscape(path)})
	if err != nil {
		return nil, fmt.Errorf("Error in GetObject operation (%v)", err)
	}

	return v3ioReader{reader: bytes.NewReader(resp.Body())}, nil
}

type v3ioReader struct {
	reader io.Reader
}

func (r v3ioReader) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r v3ioReader) Close() error {
	return nil
}

func (c *V3ioClient) Writer(path string) (io.WriteCloser, error) {
	return &v3ioWriter{path: path, container: c.container}, nil
}

type v3ioWriter struct {
	path      string
	buf       []byte
	container *v3io.Container
}

func (w *v3ioWriter) Write(p []byte) (n int, err error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *v3ioWriter) Close() error {
	return w.container.Sync.PutObject(&v3io.PutObjectInput{Path: url.PathEscape(w.path), Body: w.buf})
}

func CreateContainer(logger logger.Logger, addr, cont string, config *v3io.SessionConfig, workers int) (*v3io.Container, error) {
	// Create context
	context, err := v3io.NewContext(logger, addr, workers)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a V3IO client.")
	}

	// Create session
	session, err := context.NewSessionFromConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a session.")
	}

	// Create the container
	container, err := session.NewContainer(cont)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a container.")
	}

	return container, nil
}
