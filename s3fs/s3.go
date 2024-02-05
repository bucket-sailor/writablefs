/* SPDX-License-Identifier: MPL-2.0
 *
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package s3fs

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	gopath "path"
	"strings"
	"sync"

	"github.com/bucket-sailor/writablefs"
	"github.com/bucket-sailor/writablefs/dirfs"
	"github.com/hashicorp/go-multierror"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type s3FS struct {
	ctx        context.Context
	cancel     context.CancelFunc
	logger     *slog.Logger
	client     *minio.Client
	bucketName string
	// For storing staged writes.
	stagingDir string
	stagingFS  writablefs.FS
	filesMu    sync.Mutex
	files      map[string]*file
}

// Options for opening a new S3 filesystem.
type Options struct {
	EndpointURL     string
	Region          string
	TLSClientConfig *tls.Config
	Credentials     *credentials.Credentials
	BucketName      string
}

// New opens a new S3 filesystem.
func New(ctx context.Context, logger *slog.Logger, opts Options) (writablefs.FS, error) {
	logger.Debug("Opening S3 filesystem", "endpointURL", opts.EndpointURL, "bucketName", opts.BucketName)

	// Parse the endpoint URL.
	endpointURL, err := url.Parse(opts.EndpointURL)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint url: %w", err)
	}

	// If the port is empty, set it to the default port for the scheme.
	if endpointURL.Port() == "" {
		switch endpointURL.Scheme {
		case "http":
			endpointURL.Host += ":80"
		case "https":
			endpointURL.Host += ":443"
		}
	}

	// Set up the TLS transport (if required).
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if opts.TLSClientConfig != nil {
		transport.TLSClientConfig = opts.TLSClientConfig
	}

	client, err := minio.New(endpointURL.Host, &minio.Options{
		Region:    opts.Region,
		Transport: transport,
		Secure:    endpointURL.Scheme == "https",
		Creds:     opts.Credentials,
	})
	if err != nil {
		return nil, err
	}

	// S3 objects are immutable, so we need to stage writes to a temporary filesystem
	// and then upload the object to S3 when complete (eg. when closed).
	stagingDir, err := os.MkdirTemp("", "s3fs-*")
	if err != nil {
		return nil, err
	}

	stagingFS, err := dirfs.New(stagingDir)
	if err != nil {
		_ = os.RemoveAll(stagingDir)

		return nil, err
	}

	logger.Debug("Using staging directory", "path", stagingDir)

	ctx, cancel := context.WithCancel(ctx)

	return &s3FS{
		ctx:        ctx,
		cancel:     cancel,
		logger:     logger,
		client:     client,
		bucketName: opts.BucketName,
		stagingDir: stagingDir,
		stagingFS:  stagingFS,
		files:      make(map[string]*file),
	}, nil
}

func (fsys *s3FS) Close() error {
	fsys.logger.Debug("Closing S3 filesystem")

	// Cancel any pending operations.
	fsys.cancel()

	fsys.logger.Debug("Closing all open files")

	// Close all open files (eg. CLOEXEC).
	fsys.filesMu.Lock()
	for _, file := range fsys.files {
		// Abort any pending operations.
		file.cancel()

		// Close all open handles.
		for h := range file.handles {
			if err := h.Close(); err != nil {
				return err
			}

			delete(file.handles, h)
		}
	}

	fsys.logger.Debug("Removing staging directory", "path", fsys.stagingDir)

	return os.RemoveAll(fsys.stagingDir)
}

func (fsys *s3FS) Open(path string) (writablefs.FileReadOnly, error) {
	return fsys.OpenFile(path, writablefs.FlagReadOnly)
}

func (fsys *s3FS) OpenFile(path string, flag writablefs.FileOpenFlag) (writablefs.File, error) {
	var nonEmpty bool
	if _, err := fsys.Stat(path); err != nil {
		if errors.Is(err, writablefs.ErrNotExist) {
			if !flag.IsSet(writablefs.FlagCreate) {
				return nil, writablefs.ErrNotExist
			}
		} else {
			return nil, err
		}
	} else {
		nonEmpty = true
	}

	fsys.filesMu.Lock()
	f, ok := fsys.files[path]
	if !ok {
		ctx, cancel := context.WithCancel(fsys.ctx)

		f = &file{
			ctx:     ctx,
			cancel:  cancel,
			fsys:    fsys,
			key:     toKey(path, false),
			handles: make(map[*fileHandle]struct{}),
		}

		fsys.files[path] = f
	}
	fsys.filesMu.Unlock()

	return f.newHandle(flag.IsSet(writablefs.FlagReadOnly), nonEmpty)
}

func (fsys *s3FS) MkdirAll(path string) error {
	key := toKey(path, true)

	fsys.logger.Debug("Creating directory structure", "key", key)

	var partialKey string
	for _, part := range strings.Split(key, "/") {
		if part == "" {
			continue
		}

		partialKey += part + "/"

		fsys.logger.Debug("Creating directory", "key", partialKey)

		// Represent directories as a zero-length object with a slash suffix.
		_, err := fsys.client.PutObject(fsys.ctx, fsys.bucketName, partialKey, bytes.NewReader(nil), 0, minio.PutObjectOptions{})
		if err != nil {
			fsys.logger.Error("Failed to create directory", "key", partialKey, "error", err)
			return err
		}
	}

	return nil
}

func (fsys *s3FS) ReadDir(path string) ([]writablefs.DirEntry, error) {
	key := toKey(path, true)

	fsys.logger.Debug("Listing objects in directory", "key", key)

	ctx, cancel := context.WithCancel(fsys.ctx)
	defer cancel()

	objCh := fsys.client.ListObjects(ctx, fsys.bucketName, minio.ListObjectsOptions{
		Prefix:    key,
		Recursive: false,
	})

	var entries []writablefs.DirEntry
	for obj := range objCh {
		if obj.Err != nil {
			return nil, obj.Err
		}

		// Skip the directory itself (not all S3 implementations will return it).
		if obj.Key == key {
			continue
		}

		entries = append(entries, &dirEntry{
			name:    strings.TrimPrefix(obj.Key, key),
			size:    obj.Size,
			modTime: obj.LastModified,
			isDir:   strings.HasSuffix(obj.Key, "/"),
		})
	}

	// Check if the directory exists.
	// We only do this as a last resort as it can be an expensive operation.
	if len(entries) == 0 && key != "" {
		fsys.logger.Debug("Checking if directory actually exists", "key", key, "parentKey", parentKey)

		if _, err := fsys.Stat(key); err != nil {
			return nil, writablefs.ErrNotExist
		}
	}

	if len(entries) > 0 {
		fsys.logger.Debug("Found objects in directory", "key", key, "count", len(entries))
	}

	return entries, nil
}

func (fsys *s3FS) RemoveAll(path string) error {
	// Is it an object instead of a directory?
	fi, err := fsys.Stat(path)
	if err == nil && !fi.IsDir() {
		key := toKey(path, false)

		fsys.logger.Debug("Removing object", "key", key)

		err := fsys.client.RemoveObject(fsys.ctx, fsys.bucketName, key, minio.RemoveObjectOptions{})
		if err != nil {
			return err
		}

		return nil
	}

	key := toKey(path, true)

	fsys.logger.Debug("Removing directory", "key", key)

	objCh := fsys.client.ListObjects(fsys.ctx, fsys.bucketName, minio.ListObjectsOptions{
		Prefix:    key,
		Recursive: true,
	})

	var resultMu sync.Mutex
	var result *multierror.Error

	objToDeleteCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objToDeleteCh)

		for obj := range objCh {
			if obj.Err != nil {
				resultMu.Lock()
				result = multierror.Append(result, obj.Err)
				resultMu.Unlock()

				continue
			}

			objToDeleteCh <- obj
		}
	}()

	removeErrorCh := fsys.client.RemoveObjects(fsys.ctx, fsys.bucketName, objToDeleteCh, minio.RemoveObjectsOptions{})

	for err := range removeErrorCh {
		if err.Err != nil {
			resultMu.Lock()
			result = multierror.Append(result, err.Err)
			resultMu.Unlock()
		}
	}

	if err := fsys.client.RemoveObject(fsys.ctx, fsys.bucketName, key, minio.RemoveObjectOptions{}); err != nil {
		resultMu.Lock()
		result = multierror.Append(result, err)
		resultMu.Unlock()
	}

	return result.ErrorOrNil()
}

func (fsys *s3FS) Rename(oldPath string, newPath string) error {
	fsys.logger.Debug("Renaming object", "oldPath", oldPath, "newPath", newPath)

	// TODO: Implement directory renames.

	src := minio.CopySrcOptions{
		Bucket: fsys.bucketName,
		Object: toKey(oldPath, false),
	}
	dst := minio.CopyDestOptions{
		Bucket: fsys.bucketName,
		Object: toKey(newPath, false),
	}

	if _, err := fsys.client.CopyObject(fsys.ctx, dst, src); err != nil {
		return err
	}

	return fsys.client.RemoveObject(fsys.ctx, fsys.bucketName, src.Object, minio.RemoveObjectOptions{})
}

func (fsys *s3FS) Stat(path string) (writablefs.FileInfo, error) {
	key := toKey(path, false)

	fsys.logger.Debug("Getting status of object", "key", key)

	if key == "" {
		fsys.logger.Debug("Returning pseudo-entry for root directory")

		// Return a pseudo-entry for the root directory.
		return &fileInfo{
			info: minio.ObjectInfo{
				Key: key,
			},
		}, nil
	}

	info, err := fsys.client.StatObject(fsys.ctx, fsys.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			fsys.logger.Debug("Attempting to get status of directory by listing parent", "key", key)

			ctx, cancel := context.WithCancel(fsys.ctx)
			defer cancel()

			objCh := fsys.client.ListObjects(ctx, fsys.bucketName, minio.ListObjectsOptions{
				Prefix:     parentKey(key),
				Recursive:  false,
				StartAfter: key,
				MaxKeys:    1,
			})

			for obj := range objCh {
				if obj.Err != nil {
					return nil, obj.Err
				}

				if strings.TrimSuffix(obj.Key, "/") == gopath.Base(strings.TrimSuffix(key, "/")) {
					fsys.logger.Debug("Found directory in parent", "key", key)

					return &fileInfo{
						info: minio.ObjectInfo{
							Key:          obj.Key,
							LastModified: obj.LastModified,
							Size:         obj.Size,
						},
					}, nil
				}
			}

			return nil, writablefs.ErrNotExist
		}

		return nil, err
	}

	return &fileInfo{
		info: info,
	}, nil
}

func parentKey(key string) string {
	return toKey(gopath.Dir(strings.TrimSuffix(key, "/")), true)
}

func toKey(path string, isDir bool) string {
	key := gopath.Clean(path)

	// The dot prefix doesn't make sense in S3.
	if key == "." {
		key = ""
	}

	// Remove leading slash.
	key = strings.TrimPrefix(key, "/")

	// Add trailing slash if it's a directory (and it's not the root directory).
	if isDir && (key != "" && !strings.HasSuffix(key, "/")) {
		key += "/"
	}

	return key
}
