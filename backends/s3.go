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
	"strconv"
	"strings"
	"time"
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
	if strings.HasPrefix(path, "/") {
		path = path[1:]
	}
	parts := strings.Split(path, "/")
	if len(parts) <= 1 {
		return path, ""
	}
	return parts[0], path[len(parts[0])+1:]
}

func (c *s3client) ListDir(fileChan chan *FileDetails, task *ListDirTask, summary *ListSummary) error {
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

		var mode uint32
		if obj.Metadata.Get(OriginalModeKey) != "" {
			if i, err := strconv.Atoi(obj.Metadata.Get(OriginalModeKey)); err == nil {
				mode = uint32(i)
			}
		}

		var originalTime time.Time
		if obj.Metadata.Get(OriginalMtimeKey) != "" {
			if t, err := time.Parse(time.RFC3339, obj.Metadata.Get(OriginalMtimeKey)); err == nil {
				originalTime = t
			}
		}

		c.logger.DebugWith("List dir:", "key", obj.Key, "modified", obj.LastModified, "size", obj.Size)
		fileDetails := &FileDetails{
			Key: c.params.Bucket + "/" + obj.Key, Size: obj.Size,
			Mtime: obj.LastModified, Mode: mode, OriginalMtime: originalTime,
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

func (c *s3client) Writer(path string, opts *WriteOptions) (io.WriteCloser, error) {
	return &s3Writer{bucket: c.params.Bucket, path: path, client: c, opts: opts}, nil
}

type s3Writer struct {
	bucket string
	path   string
	buf    []byte
	opts   *WriteOptions
	client *s3client
}

func (w *s3Writer) Write(p []byte) (n int, err error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *s3Writer) Close() error {
	r := bytes.NewReader(w.buf)
	opts := minio.PutObjectOptions{}
	if w.opts != nil {
		// optionally set metadata keys with original mode and mtime
		opts.UserMetadata = map[string]string{OriginalMtimeKey: w.opts.Mtime.Format(time.RFC3339),
			OriginalModeKey: strconv.Itoa(int(w.opts.Mode))}
	}

	objectName := w.path
	if strings.HasPrefix(objectName, "/") {
		objectName = objectName[1:]
	}
	_, err := w.client.minioClient.PutObject(w.bucket, objectName, r, int64(len(w.buf)), opts)
	if err != nil {
		w.client.logger.Error("obj %s put error (%v)", w.path, err)
	}
	return err
}
