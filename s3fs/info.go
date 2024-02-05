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
	"path/filepath"
	"strings"
	"time"

	"github.com/bucket-sailor/writablefs"
	"github.com/minio/minio-go/v7"
)

type dirEntry struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (e *dirEntry) Name() string {
	return strings.TrimSuffix(e.name, "/")
}

func (e *dirEntry) IsDir() bool {
	return e.isDir

}
func (e *dirEntry) Type() writablefs.FileMode {
	return 0
}

func (e *dirEntry) Info() (writablefs.FileInfo, error) {
	return &fileInfo{
		info: minio.ObjectInfo{
			Key:          e.name,
			Size:         e.size,
			LastModified: e.modTime,
		},
	}, nil
}

type fileInfo struct {
	info minio.ObjectInfo
}

func (fi *fileInfo) Name() string {
	return filepath.Base(fi.info.Key)
}

func (fi *fileInfo) Size() int64 {
	return fi.info.Size
}

func (fi *fileInfo) Mode() writablefs.FileMode {
	return 0
}

func (fi *fileInfo) ModTime() time.Time {
	return fi.info.LastModified
}

func (fi *fileInfo) IsDir() bool {
	return fi.info.Key == "" || strings.HasSuffix(fi.info.Key, "/")
}

func (fi *fileInfo) Sys() any {
	return nil
}
