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
	"strings"
	"sync"

	"github.com/bucket-sailor/writablefs"
	"github.com/minio/minio-go/v7"
)

type attrChange struct {
	name   string
	value  string
	remove bool
}

type s3Attrs struct {
	fsys   *s3FS
	handle *fileHandle
	// A cache of the extended attributes.
	cache map[string]string
	// Pending changes to the extended attributes.
	changesMu sync.Mutex
	changes   map[string]attrChange
}

func newS3Attrs(fsys *s3FS, handle *fileHandle) (*s3Attrs, error) {
	a := &s3Attrs{
		fsys:    fsys,
		handle:  handle,
		cache:   make(map[string]string),
		changes: make(map[string]attrChange),
	}

	if err := a.Sync(); err != nil {
		return nil, err
	}

	return a, nil
}

func (a *s3Attrs) Get(name string) ([]byte, error) {
	name = strings.ToLower(name)

	a.fsys.logger.Debug("Getting extended attribute", "key", a.handle.file.key, "name", name)

	// check the pending changes first.
	if change, ok := a.changes[name]; ok {
		if change.remove {
			return nil, writablefs.ErrNoSuchAttr
		}
		return []byte(change.value), nil
	}

	// check the cache.
	if value, ok := a.cache[name]; ok {
		return []byte(value), nil
	}

	return nil, writablefs.ErrNoSuchAttr
}

func (a *s3Attrs) Set(name string, data []byte) error {
	name = strings.ToLower(name)

	a.fsys.logger.Debug("Setting extended attribute", "key", a.handle.file.key, "name", name)

	if a.handle.readOnly {
		return writablefs.ErrPermission
	}

	a.changesMu.Lock()
	a.changes[name] = attrChange{
		name:  name,
		value: string(data),
	}
	a.changesMu.Unlock()

	return nil
}

func (a *s3Attrs) Remove(name string) error {
	name = strings.ToLower(name)

	a.fsys.logger.Debug("Removing extended attribute", "key", a.handle.file.key, "name", name)

	if a.handle.readOnly {
		return writablefs.ErrPermission
	}

	a.changesMu.Lock()
	a.changes[name] = attrChange{
		name:   name,
		remove: true,
	}
	a.changesMu.Unlock()

	return nil
}

func (a *s3Attrs) List() ([]string, error) {
	a.fsys.logger.Debug("Listing extended attributes", "key", a.handle.file.key)

	keys := make(map[string]struct{})
	for key := range a.cache {
		keys[key] = struct{}{}
	}

	for _, change := range a.changes {
		if change.remove {
			delete(keys, change.name)
		} else {
			keys[change.name] = struct{}{}
		}
	}

	names := make([]string, 0, len(keys))
	for key := range keys {
		names = append(names, key)
	}

	return names, nil
}

func (a *s3Attrs) Sync() error {
	a.fsys.logger.Debug("Syncing extended attributes", "key", a.handle.file.key)

	// Populate the cache with the current metadata.
	info, err := a.fsys.client.StatObject(a.fsys.ctx, a.fsys.bucketName, a.handle.file.key, minio.StatObjectOptions{})
	if err != nil {
		return err
	}

	a.cache = make(map[string]string)
	for key, value := range info.UserMetadata {
		a.cache[strings.ToLower(key)] = value
	}

	a.changesMu.Lock()
	defer a.changesMu.Unlock()

	// No changes to commit.
	if len(a.changes) == 0 {
		a.fsys.logger.Debug("No changes to commit", "key", a.handle.file.key)

		return nil
	}

	for name, change := range a.changes {
		if change.remove {
			delete(a.cache, name)
		} else {
			a.cache[name] = change.value
		}
	}

	copySrc := minio.CopySrcOptions{
		Bucket: a.fsys.bucketName,
		Object: a.handle.file.key,
	}

	copyDst := minio.CopyDestOptions{
		Bucket:          a.fsys.bucketName,
		Object:          a.handle.file.key,
		UserMetadata:    a.cache,
		ReplaceMetadata: true,
	}

	_, err = a.fsys.client.CopyObject(a.fsys.ctx, copyDst, copySrc)
	if err != nil {
		return err
	}

	// Clear the pending changes.
	a.changes = make(map[string]attrChange)

	return nil
}
