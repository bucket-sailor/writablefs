/* SPDX-License-Identifier: MPL-2.0
 *
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package test

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bucket-sailor/writablefs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDirectoryArchive(t *testing.T, fsys writablefs.FS) {
	t.Run("Directory Archive", func(t *testing.T) {
		archiveFS, ok := fsys.(writablefs.ArchiveFS)
		if !ok {
			t.Skip("archive not supported by filesystem")
		}

		err := fsys.RemoveAll(t.Name())
		require.NoError(t, err)

		dirPath := filepath.Join(t.Name(), randomString(10))

		// Create a directory
		err = fsys.MkdirAll(dirPath)
		require.NoError(t, err)

		// Create a file in the directory
		filePath := filepath.Join(dirPath, randomString(10)+".txt")

		f, err := fsys.OpenFile(filePath, writablefs.FlagCreate|writablefs.FlagReadWrite)
		require.NoError(t, err)

		_, err = f.Write([]byte("just a test"))
		require.NoError(t, err)

		err = f.Sync()
		require.NoError(t, err)

		err = f.Close()
		require.NoError(t, err)

		// Archive the directory
		r, err := archiveFS.Archive(t.Name())
		require.NoError(t, err)

		// Extract the directory
		outDir := t.TempDir()

		err = extract(outDir, r)
		require.NoError(t, err)

		assert.DirExists(t, filepath.Join(outDir, strings.TrimPrefix(dirPath, t.Name()+"/")))
		assert.FileExists(t, filepath.Join(outDir, strings.TrimPrefix(filePath, t.Name()+"/")))
	})
}

func extract(outDir string, r io.Reader) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		path := filepath.Join(outDir, hdr.Name)

		// is this a directory?
		if hdr.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(path, os.FileMode(hdr.Mode)); err != nil {
				return err
			}

			continue
		}

		f, err := os.Create(path)
		if err != nil {
			return err
		}

		if _, err := io.Copy(f, tr); err != nil {
			return err
		}

		if err := f.Close(); err != nil {
			return err
		}

		if err := os.Chmod(path, os.FileMode(hdr.Mode)); err != nil {
			return err
		}

		if err := os.Chtimes(path, hdr.ModTime, hdr.ModTime); err != nil {
			return err
		}
	}

	return nil
}
