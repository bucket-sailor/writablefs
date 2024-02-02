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
	"io"
	gofs "io/fs"
	"os"
)

var (
	ErrInvalid    = gofs.ErrInvalid    // "invalid argument"
	ErrPermission = gofs.ErrPermission // "permission denied"
	ErrExist      = gofs.ErrExist      // "file already exists"
	ErrNotExist   = gofs.ErrNotExist   // "file does not exist"
	ErrClosed     = gofs.ErrClosed     // "file already closed"
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

// File is the interface implemented by a writeable file.
type File interface {
	FileReadOnly
	io.Writer
	io.Seeker
	io.ReaderAt
	io.WriterAt
	Sync() error
	Truncate(size int64) error
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
