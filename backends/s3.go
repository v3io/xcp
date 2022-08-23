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

		c.logger.DebugWith("List dir:", "key", obj.Key,
			"modified", obj.LastModified, "size", obj.Size)
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

func (c *s3client) Reader(path string) (FSReader, error) {
	bucket, objectName := SplitPath(path)
	if err := s3utils.CheckValidBucketName(bucket); err != nil {
		return nil, err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
		return nil, err
	}

	obj, err := c.minioClient.GetObject(bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return &s3Reader{obj}, nil
}

type s3Reader struct {
	obj *minio.Object
}

func (r *s3Reader) Read(p []byte) (n int, err error) {
	return r.obj.Read(p)
}

func (r *s3Reader) Close() error {
	return r.obj.Close()
}

func (r *s3Reader) Stat() (*FileMeta, error) {
	stat, err := r.obj.Stat()
	if err != nil {
		return nil, err
	}
	var mode uint32
	if stat.Metadata.Get(OriginalModeS3Key) != "" {
		if i, err := strconv.Atoi(stat.Metadata.Get(OriginalModeS3Key)); err == nil {
			mode = uint32(i)
		}
	}

	modified := stat.LastModified
	if stat.Metadata.Get(OriginalMtimeS3Key) != "" {
		if t, err := time.Parse(time.RFC3339, stat.Metadata.Get(OriginalMtimeS3Key)); err == nil {
			modified = t
		}
	}

	meta := FileMeta{Mtime: modified, Mode: uint32(mode)}
	return &meta, err
}

func (c *s3client) Writer(path string, opts *FileMeta) (io.WriteCloser, error) {
	return &s3Writer{bucket: c.params.Bucket, path: path, client: c, opts: opts}, nil
}

type s3Writer struct {
	bucket string
	path   string
	buf    []byte
	opts   *FileMeta
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
