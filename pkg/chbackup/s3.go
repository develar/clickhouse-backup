package chbackup

import (
	"github.com/minio/minio-go/v6/pkg/encrypt"
	"io"
	"time"

	"github.com/minio/minio-go/v6"
	"github.com/pkg/errors"
)

// S3 - presents methods for manipulate data on s3
type S3 struct {
	minioClient *minio.Client
	Config      *S3Config
}

// Connect - connect to s3
func (s *S3) Connect() error {
	var err error

	if s.minioClient, err = minio.New(s.Config.Endpoint, s.Config.AccessKey, s.Config.SecretKey, !s.Config.DisableSSL); err != nil {
		println(s.Config.Endpoint)
		return err
	}
	return nil
}

func (s *S3) Kind() string {
	return "S3"
}

func (s *S3) GetFileReader(key string) (io.ReadCloser, error) {
	object, err := s.minioClient.GetObject(s.Config.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (s *S3) PutFile(key string, r io.ReadCloser, progressBarUpdater *ProgressBarUpdater) error {
	options := minio.PutObjectOptions{}
	if s.Config.PartSize > 0 {
		options.PartSize = uint64(s.Config.PartSize)
		options.Progress = progressBarUpdater
	}
	if s.Config.SSE != "" {
		var err error
		// todo is it correct?
		options.ServerSideEncryption, err = encrypt.NewSSEC([]byte(s.Config.SSE))
		if err != nil {
			return err
		}
	}
	_, err := s.minioClient.PutObject(s.Config.Bucket, key, r, -1, options)
	return err
}

func (s *S3) DeleteFile(key string) error {
	err := s.minioClient.RemoveObject(s.Config.Bucket, key)
	if err != nil {
		return errors.Wrapf(err, "cannot delete file (bucket=%s, key=%s)", s.Config.Bucket, key)
	}
	return nil
}

func (s *S3) GetFile(key string) (RemoteFile, error) {
	objectInfo, err := s.minioClient.StatObject(s.Config.Bucket, key, minio.StatObjectOptions{})
	if err != nil {
		errorResponse, ok := err.(minio.ErrorResponse)
		if ok && errorResponse.Code != "NoSuchKey" {
			return nil, ErrNotFound
		}
		return nil, errors.Wrapf(err, "cannot get file metadata (bucket=%s, key=%s)", s.Config.Bucket, key)
	}
	return &s3File{objectInfo.Size, objectInfo.LastModified, key}, nil
}

func (s *S3) Walk(s3Path string, process func(r RemoteFile)) error {
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := s.minioClient.ListObjectsV2(s.Config.Bucket, s.Config.Path, false, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return errors.Wrapf(object.Err, "cannot get file metadata (bucket=%s, key=%s)", s.Config.Bucket, object.Key)
		}

		process(&s3File{object.Size, object.LastModified, object.Key})
	}

	return nil
}

type s3File struct {
	size         int64
	lastModified time.Time
	name         string
}

func (f *s3File) Size() int64 {
	return f.size
}

func (f *s3File) Name() string {
	return f.name
}

func (f *s3File) LastModified() time.Time {
	return f.lastModified
}
