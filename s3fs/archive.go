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
	"fmt"
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
		largeObjectThresholdBytes = 32000000 // 32MB
	)

	key := toKey(name, true)

	fsys.logger.Debug("Archiving directory", "key", key)

	pr, pw := io.Pipe()
	go func() {
		objCh := fsys.client.ListObjects(fsys.ctx, fsys.bucketName, minio.ListObjectsOptions{
			Prefix:    key,
			Recursive: true,
		})

		var objects []minio.ObjectInfo
		directories := make(map[string]bool)

		for objInfo := range objCh {
			if objInfo.Err != nil {
				pw.CloseWithError(objInfo.Err)
				return
			}

			// Skip the directory itself (not all S3 implementations will return it).
			if objInfo.Key == key {
				continue
			}

			// Collect directories.
			if strings.HasSuffix(objInfo.Key, "/") {
				directories[strings.TrimSuffix(strings.TrimPrefix(objInfo.Key, key), "/")] = true
				continue
			}

			dir := gopath.Dir(strings.TrimPrefix(objInfo.Key, key))
			for dir != "." && dir != "/" && !directories[dir] {
				directories[dir] = true
				dir = gopath.Dir(dir)
			}

			objects = append(objects, objInfo)
		}

		var dirPaths []string
		for dir := range directories {
			dirPaths = append(dirPaths, dir)
		}

		sort.Strings(dirPaths)

		var writerMu sync.Mutex
		tw := tar.NewWriter(pw)

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

		q := queue.NewQueue(numConnections)

		for _, objInfo := range objects {
			objInfo := objInfo

			q.Add(func() error {
				fsys.logger.Debug("Adding object to archive", "key", objInfo.Key)

				obj, err := fsys.client.GetObject(fsys.ctx, fsys.bucketName, objInfo.Key, minio.GetObjectOptions{})
				if err != nil {
					return err
				}
				defer obj.Close()

				if objInfo.Size > largeObjectThresholdBytes {
					writerMu.Lock()
					defer writerMu.Unlock()

					hdr := &tar.Header{
						Name:    strings.TrimPrefix(objInfo.Key, key),
						Size:    objInfo.Size,
						ModTime: objInfo.LastModified,
						Mode:    0o644,
					}

					if err := tw.WriteHeader(hdr); err != nil {
						return err
					}

					n, err := io.Copy(tw, obj)
					if err != nil {
						return err
					}

					if n != objInfo.Size {
						return fmt.Errorf("unexpected size %d != %d for object %q: %w", n, objInfo.Size, objInfo.Key, io.ErrShortWrite)
					}
				} else {
					buf := make([]byte, objInfo.Size)
					n, err := io.ReadFull(obj, buf)
					if err != nil {
						return err
					}

					if int64(n) != objInfo.Size {
						return fmt.Errorf("unexpected size %d != %d for object %q: %w", n, objInfo.Size, objInfo.Key, io.ErrShortWrite)
					}

					writerMu.Lock()
					defer writerMu.Unlock()

					hdr := &tar.Header{
						Name:    strings.TrimPrefix(objInfo.Key, key),
						Size:    objInfo.Size,
						ModTime: objInfo.LastModified,
						Mode:    0o644,
					}

					if err := tw.WriteHeader(hdr); err != nil {
						return err
					}

					n, err = tw.Write(buf)
					if err != nil {
						return err
					}

					if int64(n) != objInfo.Size {
						return io.ErrShortWrite
					}
				}

				return nil
			})
		}

		err := q.Wait()
		if err != nil {
			_ = tw.Close()
			pw.CloseWithError(err)
			return
		}

		if err := tw.Close(); err != nil {
			pw.CloseWithError(err)
			return
		}

		pw.Close()
	}()

	return pr, nil
}
