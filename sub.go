/* SPDX-License-Identifier: MPL-2.0
 *
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package writablefs

import (
	"io/fs"
	"path/filepath"
)

type subFS struct {
	parentFsys FS
	prefix     string
}

// TODO: fix up path errors that have references to the parent path and such.

// Sub returns a writable FS that is a subdirectory of the given FS.
func Sub(fsys FS, prefix string) FS {
	return &subFS{parentFsys: fsys, prefix: prefix}
}

func (fsys *subFS) Close() error {
	return nil
}

func (fsys *subFS) Open(path string) (FileReadOnly, error) {
	return fsys.parentFsys.OpenFile(path, FlagReadOnly)
}

func (fsys *subFS) OpenFile(path string, flag FileOpenFlag) (File, error) {
	return fsys.parentFsys.OpenFile(filepath.Join(fsys.prefix, filepath.Clean(path)), flag)
}

func (fsys *subFS) MkdirAll(path string) error {
	return fsys.parentFsys.MkdirAll(filepath.Join(fsys.prefix, filepath.Clean(path)))
}

func (fsys *subFS) ReadDir(path string) ([]fs.DirEntry, error) {
	return fsys.parentFsys.ReadDir(filepath.Join(fsys.prefix, filepath.Clean(path)))
}

func (fsys *subFS) RemoveAll(path string) error {
	return fsys.parentFsys.RemoveAll(filepath.Join(fsys.prefix, filepath.Clean(path)))
}

func (fsys *subFS) Rename(oldPath string, newPath string) error {
	return fsys.parentFsys.Rename(filepath.Join(fsys.prefix, filepath.Clean(oldPath)), filepath.Join(fsys.prefix, filepath.Clean(newPath)))
}

func (fsys *subFS) Stat(path string) (fs.FileInfo, error) {
	return fsys.parentFsys.Stat(filepath.Join(fsys.prefix, filepath.Clean(path)))
}
