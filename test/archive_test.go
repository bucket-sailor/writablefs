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

	"github.com/bucket-sailor/writablefs"
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

		f, err := os.Open("testdata/archive.tar.gz")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, f.Close())
		})

		err = extract(f, fsys, t.Name())
		require.NoError(t, err)

		// Archive the directory
		r, err := archiveFS.Archive(t.Name())
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		// TODO: compute a dirhash and compare it to the expected value

		_, err = io.Copy(io.Discard, r)
		require.NoError(t, err)
	})
}

func extract(r io.Reader, fsys writablefs.FS, path string) error {
	gzr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
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
