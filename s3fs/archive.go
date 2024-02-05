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
	"archive/tar"
	"bytes"
	"context"
	"io"
	gopath "path"
	"sort"
	"strings"
	"sync"

	"github.com/bucket-sailor/queue"
	"github.com/minio/minio-go/v7"
)

func (fsys *s3FS) Archive(name string) (io.ReadCloser, error) {
	const (
		numConnections            = 20
		largeObjectThresholdBytes = 100000000 // 100MB
	)

	key := toKey(name, true)

	fsys.logger.Debug("Archiving directory", "key", key)

	pr, pw := io.Pipe()
	go func() {
		tw := tar.NewWriter(pw)

		objCh := fsys.client.ListObjects(fsys.ctx, fsys.bucketName, minio.ListObjectsOptions{
			Prefix:    key,
			Recursive: true,
		})

		var objects []minio.ObjectInfo
		directories := make(map[string]bool)

		for obj := range objCh {
			if obj.Err != nil {
				pw.CloseWithError(obj.Err)
				return
			}

			// Skip the directory itself (not all S3 implementations will return it).
			if obj.Key == key {
				continue
			}

			// Collect directories.
			if strings.HasSuffix(obj.Key, "/") {
				directories[strings.TrimSuffix(strings.TrimPrefix(obj.Key, key), "/")] = true
				continue
			}

			dir := gopath.Dir(strings.TrimPrefix(obj.Key, key))
			for dir != "." && dir != "/" && !directories[dir] {
				directories[dir] = true
				dir = gopath.Dir(dir)
			}

			objects = append(objects, obj)
		}

		var dirPaths []string
		for dir := range directories {
			dirPaths = append(dirPaths, dir)
		}

		sort.Strings(dirPaths)

		// Add the directories up front so we can add files in arbitrary order.
		for _, path := range dirPaths {
			hdr := &tar.Header{
				Name:     path,
				Typeflag: tar.TypeDir,
				Mode:     0o755,
			}

			if err := tw.WriteHeader(hdr); err != nil {
				pw.CloseWithError(err)
				return
			}
		}

		var writerMu sync.Mutex

		q := queue.NewQueue(numConnections)

		ctx, cancel := context.WithCancel(fsys.ctx)

		// Process the objects in parallel (this is important for small objects)
		for i := range objects {
			q.Add(func() {
				obj := objects[i]

				rc, err := fsys.client.GetObject(ctx, fsys.bucketName, obj.Key, minio.GetObjectOptions{})
				if err != nil {
					writerMu.Lock()
					pw.CloseWithError(err)
					writerMu.Unlock()
					return
				}

				if obj.Size > largeObjectThresholdBytes {
					defer rc.Close()

					// Write the object to the tar.
					writerMu.Lock()
					defer writerMu.Unlock()

					hdr := &tar.Header{
						Name:    strings.TrimPrefix(obj.Key, key),
						Size:    obj.Size,
						ModTime: obj.LastModified,
						Mode:    0o644,
					}

					if err := tw.WriteHeader(hdr); err != nil {
						pw.CloseWithError(err)
						return
					}

					if _, err := io.Copy(tw, rc); err != nil {
						pw.CloseWithError(err)
						return
					}
				} else {
					// Read the object into a buffer (this let's us perform get object in parallel).
					var buf bytes.Buffer
					if _, err := io.Copy(&buf, rc); err != nil {
						_ = rc.Close()
						writerMu.Lock()
						pw.CloseWithError(err)
						writerMu.Unlock()
						return
					}

					_ = rc.Close()

					// Write the object to the tar.
					writerMu.Lock()
					defer writerMu.Unlock()

					hdr := &tar.Header{
						Name:    strings.TrimPrefix(obj.Key, key),
						Size:    obj.Size,
						ModTime: obj.LastModified,
						Mode:    0o644,
					}

					if err := tw.WriteHeader(hdr); err != nil {
						pw.CloseWithError(err)
						return
					}

					if _, err := io.Copy(tw, &buf); err != nil {
						pw.CloseWithError(err)
						return
					}
				}
			})
		}

		// Stop the queue when the pipe is closed or an error occurs.
		go func() {
			if _, err := pr.Read(make([]byte, 0)); err != nil {
				q.Clear()
			}
		}()

		// Close the pipe when the queue is done.
		go func() {
			<-q.Idle()

			tw.Close()

			pw.Close()

			cancel()
		}()
	}()

	return pr, nil
}
