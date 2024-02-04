/* SPDX-License-Identifier: MPL-2.0
 *
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dirfs

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/bucket-sailor/writablefs"
)

type dirFS string

// New returns a writeable file system rooted at the given directory.
func New(dir string) (writablefs.FS, error) {
	var err error
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	return dirFS(dir), nil
}

func (fsys dirFS) Close() error {
	return nil
}

func (fsys dirFS) Open(name string) (writablefs.FileReadOnly, error) {
	return fsys.OpenFile(name, writablefs.FlagReadOnly)
}

func (fsys dirFS) OpenFile(name string, flag writablefs.FileOpenFlag) (writablefs.File, error) {
	path, err := fsys.safePath(name)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, int(flag), 0o644)
	if err != nil {
		return nil, err
	}

	return &fileWithXAttrs{f}, nil
}

func (fsys dirFS) MkdirAll(name string) error {
	path, err := fsys.safePath(name)
	if err != nil {
		return err
	}

	return os.MkdirAll(path, 0o755)
}

func (fsys dirFS) ReadDir(name string) ([]writablefs.DirEntry, error) {
	path, err := fsys.safePath(name)
	if err != nil {
		return nil, err
	}

	return os.ReadDir(path)
}

func (fsys dirFS) RemoveAll(name string) error {
	path, err := fsys.safePath(name)
	if err != nil {
		return err
	}

	return os.RemoveAll(path)
}

func (fsys dirFS) Rename(oldName, newName string) error {
	oldPath, err := fsys.safePath(oldName)
	if err != nil {
		return err
	}

	newPath, err := fsys.safePath(newName)
	if err != nil {
		return err
	}

	return os.Rename(oldPath, newPath)
}

func (fsys dirFS) Stat(name string) (writablefs.FileInfo, error) {
	path, err := fsys.safePath(name)
	if err != nil {
		return nil, err
	}

	return os.Stat(path)
}

func (fsys dirFS) safePath(path string) (string, error) {
	absPath, err := filepath.Abs(filepath.Join(string(fsys), path))
	if err != nil {
		return "", err
	}

	if !strings.HasPrefix(absPath, string(fsys)) {
		return "", writablefs.ErrPermission
	}

	return absPath, nil
}

type fileWithXAttrs struct {
	*os.File
}

func (f *fileWithXAttrs) XAttrs() (writablefs.ExtendedAttributes, error) {
	return &fileAttrs{f.File}, nil
}
