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
	"fmt"
	"io"
	gofs "io/fs"
	"os"
)

var (
	ErrInvalid    = gofs.ErrInvalid                 // "invalid argument"
	ErrPermission = gofs.ErrPermission              // "permission denied"
	ErrExist      = gofs.ErrExist                   // "file already exists"
	ErrNotExist   = gofs.ErrNotExist                // "file does not exist"
	ErrClosed     = gofs.ErrClosed                  // "file already closed"
	ErrNoSuchAttr = fmt.Errorf("no such attribute") // "no such attribute"
)

type FileMode = gofs.FileMode

const (
	ModeDir  = gofs.ModeDir
	ModePerm = gofs.ModePerm
)

type DirEntry = gofs.DirEntry
type FileInfo = gofs.FileInfo

// FileOpenFlag allows configuring how a file is opened.
type FileOpenFlag int

const (
	FlagReadOnly  = FileOpenFlag(os.O_RDONLY)
	FlagWriteOnly = FileOpenFlag(os.O_WRONLY)
	FlagReadWrite = FileOpenFlag(os.O_RDWR)
	FlagCreate    = FileOpenFlag(os.O_CREATE)
)

func (f FileOpenFlag) IsSet(flag FileOpenFlag) bool { return f&flag != 0 }

// FileReadOnly is the interface implemented by a read-only file.
// This is kept for compatibility with io/fs.
type FileReadOnly = gofs.File

// ExtendedAttributes is an interface for working with file extended attributes.
type ExtendedAttributes interface {
	// Get returns the value of the extended attribute identified by name.
	Get(name string) ([]byte, error)
	// Set sets the value of the extended attribute identified by name.
	Set(name string, data []byte) error
	// Remove removes the extended attribute identified by name.
	Remove(name string) error
	// List returns the names of all extended attributes associated with the file.
	List() ([]string, error)
	// Sync flushes any metadata changes to the file system.
	Sync() error
}

// File is the interface implemented by a writeable file.
type File interface {
	FileReadOnly
	io.Writer
	io.Seeker
	io.ReaderAt
	io.WriterAt

	// Sync flushes any changes to the file system.
	Sync() error

	// Truncate changes the size of the file.
	Truncate(size int64) error

	// XAttrs returns the extended attributes of the file.
	// If the file system does not support extended attributes,
	// an error is returned. You should call Sync() after modifying
	// the extended attributes to ensure they are persisted.
	XAttrs() (ExtendedAttributes, error)
}

// FS is the interface implemented by a writeable file system.
type FS interface {
	io.Closer
	gofs.FS
	gofs.ReadDirFS
	gofs.StatFS

	// OpenFile opens a file using the given flags.
	// By passing O_RDWR, the file can be opened for writing.
	OpenFile(path string, flag FileOpenFlag) (File, error)

	// MkdirAll creates a directory named path, along with any necessary parents.
	MkdirAll(path string) error

	// RemoveAll removes path and any children it contains.
	RemoveAll(path string) error

	// Rename renames (moves) oldpath to newpath.
	Rename(oldPath, newPath string) error
}

// ArchiveFS is the interface implemented by a file system that can create tarballs.
// This useful for dowloading whole directories etc.
type ArchiveFS interface {
	FS

	// Archive creates a tar archive of the directory at the given path.
	Archive(path string) (io.ReadCloser, error)
}
