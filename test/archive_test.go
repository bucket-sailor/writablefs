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
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"

	"io/fs"
	gofs "io/fs"

	"github.com/bucket-sailor/writablefs"
	"github.com/bucket-sailor/writablefs/dirfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/mod/sumdb/dirhash"
)

func testArchive(t *testing.T, fsys writablefs.FS) {
	t.Run("Create Archive", func(t *testing.T) {
		t.Run("End to End", func(t *testing.T) {
			archiveFS, ok := fsys.(writablefs.ArchiveFS)
			if !ok {
				t.Skip("archive not supported by filesystem")
			}

			err := fsys.RemoveAll(t.Name())
			require.NoError(t, err)

			f, err := os.Open("testdata/archive.tar.gz")
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, f.Close())
			})

			err = extractTarGZ(f, fsys, t.Name())
			require.NoError(t, err)

			// Archive the directory
			r, err := archiveFS.Archive(t.Name())
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, r.Close())
			})

			// Create a new directory based filesystem to extract the archive into.
			extractFS, err := dirfs.New(t.TempDir())
			require.NoError(t, err)

			err = extractTar(r, extractFS, "")
			require.NoError(t, err)

			var files []string
			err = gofs.WalkDir(extractFS, ".", func(path string, d fs.DirEntry, err error) error {
				if !d.IsDir() {
					files = append(files, path)
				}

				return err
			})
			require.NoError(t, err)

			computedHash, err := dirhash.DefaultHash(files, func(name string) (io.ReadCloser, error) {
				return extractFS.OpenFile(name, writablefs.FlagReadOnly)
			})
			require.NoError(t, err)

			// Calculated by running dirhash against the tarball independently.
			expectedHash := "h1:UoKe33WMeu/fqKt4bDnwDr0KBRDO6rmmSG22XdcV0J8="
			assert.Equal(t, expectedHash, computedHash)
		})
	})
}

func extractTarGZ(r io.Reader, fsys writablefs.FS, path string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()

	return extractTar(gz, fsys, path)
}

func extractTar(r io.Reader, fsys writablefs.FS, path string) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		path := filepath.Join(path, hdr.Name)

		// is this a directory?
		if hdr.Typeflag == tar.TypeDir {
			if err := fsys.MkdirAll(path); err != nil {
				return err
			}

			continue
		}

		f, err := fsys.OpenFile(path, writablefs.FlagCreate|writablefs.FlagReadWrite)
		if err != nil {
			return err
		}

		n, err := io.Copy(f, tr)
		if err != nil {
			return err
		}

		if n != hdr.Size {
			return io.ErrShortWrite
		}

		if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}
