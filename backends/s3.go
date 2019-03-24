package backends

import (
	"bytes"
	"context"
	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/s3utils"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"io"
	"path/filepath"
	"strings"
)

type s3client struct {
	params      *PathParams
	logger      logger.Logger
	minioClient *minio.Client
}

func NewS3Client(logger logger.Logger, params *PathParams) (FSClient, error) {

	newClient := s3client{params: params, logger: logger}

	var minioClient *minio.Client
	var err error
	if params.Endpoint == "" {
		params.Endpoint = "s3.amazonaws.com"
	}
	params.UserKey = defaultFromEnv(params.UserKey, "AWS_ACCESS_KEY_ID")
	params.Secret = defaultFromEnv(params.Secret, "AWS_SECRET_ACCESS_KEY")

	if params.Tag != "" {
		minioClient, err = minio.NewWithRegion(
			params.Endpoint, params.UserKey, params.Secret, params.Secure, params.Tag)
	} else {
		minioClient, err = minio.New(params.Endpoint, params.UserKey, params.Secret, params.Secure)
	}

	if err != nil {
		return nil, err
	}

	newClient.minioClient = minioClient
	return &newClient, nil
}

func SplitPath(path string) (string, string) {
	parts := strings.Split(path, "/")
	if len(parts) <= 1 {
		return path, ""
	}
	return parts[0], path[len(parts[0])+1:]
}

func (c *s3client) ListDir(fileChan chan *FileDetails, task *CopyTask, summary *ListSummary) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	defer close(fileChan)

	objCh := c.minioClient.ListObjectsV2(c.params.Bucket, c.params.Path, task.Recursive, doneCh)
	for obj := range objCh {
		if obj.Err != nil {
			return errors.WithStack(obj.Err)
		}

		if strings.HasSuffix(obj.Key, "/") {
			continue
		}

		_, name := filepath.Split(obj.Key)
		if !IsMatch(task, name, obj.LastModified, obj.Size) {
			continue
		}

		c.logger.DebugWith("List dir:", "key", obj.Key, "modified", obj.LastModified, "size", obj.Size)
		fileDetails := &FileDetails{
			Key: c.params.Bucket + "/" + obj.Key, Size: obj.Size, Mtime: obj.LastModified,
		}

		summary.TotalBytes += obj.Size
		summary.TotalFiles += 1
		fileChan <- fileDetails
	}

	return nil
}

func (c *s3client) PutObject(objectPath, filePath string) (n int64, err error) {
	bucket, objectName := SplitPath(objectPath)
	return c.minioClient.FPutObjectWithContext(
		context.Background(), bucket, objectName, filePath, minio.PutObjectOptions{})
}

func (c *s3client) Reader(path string) (io.ReadCloser, error) {
	bucket, objectName := SplitPath(path)
	if err := s3utils.CheckValidBucketName(bucket); err != nil {
		return nil, err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
		return nil, err
	}

	obj, err := c.minioClient.GetObject(bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return obj, nil
}

func (c *s3client) Writer(path string) (io.WriteCloser, error) {
	return &s3Writer{path: path, minioClient: c.minioClient}, nil
}

type s3Writer struct {
	path        string
	buf         []byte
	minioClient *minio.Client
}

func (w *s3Writer) Write(p []byte) (n int, err error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *s3Writer) Close() error {
	bucket, objectName := SplitPath(w.path)
	r := bytes.NewReader(w.buf)
	_, err := w.minioClient.PutObject(bucket, objectName, r, int64(len(w.buf)), minio.PutObjectOptions{})
	return err
}
