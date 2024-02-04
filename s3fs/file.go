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
	"context"
	"io"
	"path/filepath"
	"sync"

	"github.com/bucket-sailor/writablefs"
	"github.com/minio/minio-go/v7"
)

var (
	_ writablefs.File = (*fileHandle)(nil)
)

// file is an s3 object that is shared between multiple virtual file handles.
type file struct {
	mu sync.Mutex
	// So that we can abort pending operations.
	ctx    context.Context
	cancel context.CancelFunc
	// The filesystem this file is associated with.
	fsys *s3FS
	// The key associated with this file.
	key string
	// The staging file for writing to (if any).
	stagingFile writablefs.File
	// Are there any staged changes?
	dirty bool
	// The file handles that are currently open.
	handles map[*fileHandle]struct{}
}

// newHandle creates a new handle for this file.
func (f *file) newHandle(readOnly, nonEmpty bool) (*fileHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !readOnly && f.stagingFile == nil {
		f.fsys.logger.Debug("Creating staging file", "key", f.key)

		if err := f.fsys.stagingFS.MkdirAll(filepath.Dir(f.key)); err != nil {
			return nil, err
		}

		var err error
		f.stagingFile, err = f.fsys.stagingFS.OpenFile(f.key, writablefs.FlagReadWrite|writablefs.FlagCreate)
		if err != nil {
			return nil, err
		}

		if nonEmpty {
			f.fsys.logger.Debug("Downloading existing object into staging file", "key", f.key)

			obj, err := f.fsys.client.GetObject(f.fsys.ctx, f.fsys.bucketName, f.key, minio.GetObjectOptions{})
			if err != nil {
				return nil, err
			}
			defer obj.Close()

			if _, err = io.Copy(f.stagingFile, obj); err != nil {
				return nil, err
			}
		}
	}

	h := &fileHandle{
		fsys:     f.fsys,
		file:     f,
		readOnly: readOnly,
	}

	f.handles[h] = struct{}{}

	return h, nil
}

func (f *file) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	lastClose := len(f.handles) == 0
	if lastClose {
		if f.dirty {
			f.mu.Unlock()
			err := f.Sync()
			f.mu.Lock()
			if err != nil {
				return err
			}
		}

		// TODO: maybe we should keep this laying around so that we can potentially
		// avoid re-downloading the object if it's opened again soon.
		if f.stagingFile != nil {
			f.fsys.logger.Debug("Removing staging file", "key", f.key)

			if err := f.stagingFile.Close(); err != nil {
				return err
			}

			if err := f.fsys.stagingFS.RemoveAll(f.key); err != nil {
				return err
			}

			f.stagingFile = nil
		}
	}

	return nil
}

func (f *file) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err := f.stagingFile.WriteAt(p, off)
	if err != nil {
		return n, err
	}

	if n > 0 {
		f.dirty = true
	}

	return n, nil
}

func (f *file) Stat() (writablefs.FileInfo, error) {
	f.mu.Lock()

	if f.stagingFile != nil {
		fi, err := f.stagingFile.Stat()
		f.mu.Unlock()
		if err != nil {
			return nil, err
		}

		return &fileInfo{
			info: minio.ObjectInfo{
				Key:          f.key,
				Size:         fi.Size(),
				LastModified: fi.ModTime(),
			},
		}, nil
	}

	f.mu.Unlock()

	return f.fsys.Stat(f.key)
}

func (f *file) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.dirty {
		f.fsys.logger.Debug("Uploading modified object", "key", f.key)

		if _, err := f.stagingFile.Seek(0, io.SeekStart); err != nil {
			return err
		}

		if err := f.stagingFile.Sync(); err != nil {
			return err
		}

		fi, err := f.stagingFile.Stat()
		if err != nil {
			return err
		}

		_, err = f.fsys.client.PutObject(f.ctx, f.fsys.bucketName, f.key, f.stagingFile, fi.Size(), minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			return err
		}

		f.dirty = false
	}

	// TODO: we should also check if the remote object has been modified and
	// download it if it has.

	return nil
}

func (f *file) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.stagingFile != nil {
		f.fsys.logger.Debug("Truncating staging file", "key", f.key, "size", size)

		if err := f.stagingFile.Truncate(size); err != nil {
			return err
		}
	}

	f.dirty = true

	return nil
}

// fileHandle is a stateful virtual file handle. It keeps track of the
// current file cursor and enforces read-only permissions.
type fileHandle struct {
	mu sync.Mutex
	// The filesystem this file is associated with.
	fsys *s3FS
	// The file this handle is associated with.
	file     *file
	readOnly bool
	// The current offset in the file.
	offset int64
	// An open object handle (if any).
	// This is used in sequential read mode.
	obj *minio.Object
}

func (h *fileHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.fsys.logger.Debug("Closing object", "key", h.file.key)

	if h.obj != nil {
		h.obj.Close()
	}

	h.file.mu.Lock()
	delete(h.file.handles, h)
	h.file.mu.Unlock()

	return h.file.Close()
}

func (h *fileHandle) Read(p []byte) (n int, err error) {
	h.fsys.logger.Debug("Reading from object", "key", h.file.key)

	h.file.mu.Lock()

	if h.file.stagingFile != nil {
		defer h.file.mu.Unlock()

		h.fsys.logger.Debug("Reading from staging file", "key", h.file.key)

		n, err = h.file.stagingFile.ReadAt(p, h.offset)
	} else {
		h.file.mu.Unlock()

		h.fsys.logger.Debug("Reading from remote object", "key", h.file.key)

		h.mu.Lock()
		defer h.mu.Unlock()

		if h.obj == nil {
			var opts minio.GetObjectOptions
			if err := opts.SetRange(h.offset, -1); err != nil {
				return 0, err
			}

			var err error
			h.obj, err = h.fsys.client.GetObject(h.file.ctx, h.fsys.bucketName, h.file.key, opts)
			if err != nil {
				return 0, err
			}
		}

		n, err = h.obj.Read(p)
	}

	h.offset += int64(n)

	return n, err
}

func (h *fileHandle) ReadAt(p []byte, off int64) (int, error) {
	h.fsys.logger.Debug("Reading from object at offset", "key", h.file.key, "offset", off)

	h.file.mu.Lock()

	if h.file.stagingFile != nil {
		defer h.file.mu.Unlock()

		h.fsys.logger.Debug("Reading from staging file", "key", h.file.key, "offset", off)

		return h.file.stagingFile.ReadAt(p, off)
	} else {
		h.file.mu.Unlock()

		h.fsys.logger.Debug("Reading from remote object", "key", h.file.key, "offset", off)

		opts := minio.GetObjectOptions{}
		if err := opts.SetRange(off, -1); err != nil {
			return 0, err
		}

		var err error
		obj, err := h.fsys.client.GetObject(h.file.ctx, h.fsys.bucketName, h.file.key, opts)
		if err != nil {
			return 0, err
		}
		defer obj.Close()

		return obj.Read(p)
	}
}

func (h *fileHandle) Write(p []byte) (n int, err error) {
	h.fsys.logger.Debug("Writing to object", "key", h.file.key)

	if h.readOnly {
		return 0, writablefs.ErrPermission
	}

	n, err = h.file.WriteAt(p, h.offset)
	h.offset += int64(n)
	return n, err
}

func (h *fileHandle) WriteAt(p []byte, off int64) (int, error) {
	h.fsys.logger.Debug("Writing to object at offset", "key", h.file.key, "offset", off)

	if h.readOnly {
		return 0, writablefs.ErrPermission
	}

	return h.file.WriteAt(p, off)
}

func (h *fileHandle) Seek(offset int64, whence int) (int64, error) {
	h.fsys.logger.Debug("Seeking object", "key", h.file.key, "offset", offset, "whence", whence)

	h.mu.Lock()
	defer h.mu.Unlock()

	switch whence {
	case io.SeekStart:
		h.offset = offset
	case io.SeekCurrent:
		h.offset += offset
	case io.SeekEnd:
		fi, err := h.file.Stat()
		if err != nil {
			return 0, err
		}

		h.offset = fi.Size() + offset
	}

	if h.obj != nil {
		if err := h.obj.Close(); err != nil {
			return 0, err
		}

		h.obj = nil
	}

	return h.offset, nil
}

func (h *fileHandle) Stat() (writablefs.FileInfo, error) {
	h.fsys.logger.Debug("Getting object status", "key", h.file.key)

	return h.file.Stat()
}

func (h *fileHandle) Sync() error {
	h.fsys.logger.Debug("Syncing object", "key", h.file.key)

	return h.file.Sync()
}

func (h *fileHandle) Truncate(size int64) error {
	h.fsys.logger.Debug("Truncating object", "key", h.file.key, "size", size)

	if h.readOnly {
		return writablefs.ErrPermission
	}

	return h.file.Truncate(size)
}

func (h *fileHandle) XAttrs() (writablefs.ExtendedAttributes, error) {
	return newS3Attrs(h.fsys, h)
}
