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
	"errors"
	"io"
	"os"
	"testing"

	"github.com/bucket-sailor/writablefs"
	"github.com/stretchr/testify/require"
)

// TODO: this needs to be made A LOT more comprehensive.

func testBasicOperations(t *testing.T, fsys writablefs.FS) {
	t.Run("Basic Operations", func(t *testing.T) {
		testDir := t.Name()
		require.NoError(t, fsys.RemoveAll(testDir))
		require.NoError(t, fsys.MkdirAll(testDir))

		fsys = writablefs.Sub(fsys, testDir)

		f, err := fsys.OpenFile("hello.txt", writablefs.FlagCreate|writablefs.FlagWriteOnly)
		require.NoError(t, err)

		_, err = f.Write([]byte("hello world"))
		require.NoError(t, err)

		require.NoError(t, f.Truncate(5))

		require.NoError(t, f.Sync())

		require.NoError(t, f.Close())

		fi, err := fsys.Stat("hello.txt")
		require.NoError(t, err)

		require.Equal(t, "hello.txt", fi.Name())
		require.Equal(t, int64(5), fi.Size())

		f, err = fsys.OpenFile("hello.txt", writablefs.FlagReadOnly)
		require.NoError(t, err)

		data := make([]byte, 5)

		_, err = f.Read(data)
		if err != nil && !errors.Is(err, io.EOF) {
			require.NoError(t, err)
		}

		require.NoError(t, f.Close())

		require.Equal(t, "hello", string(data))

		entries, err := fsys.ReadDir("./")
		require.NoError(t, err)

		require.Contains(t, fileNames(entries), "hello.txt")

		require.NoError(t, fsys.RemoveAll("."))

		_, err = fsys.OpenFile("hello.txt", writablefs.FlagReadOnly)
		require.Error(t, err)

		_, err = fsys.Stat("hello.txt")
		require.Error(t, err)
	})
}

func fileNames(files []os.DirEntry) []string {
	names := make([]string, len(files))
	for i, file := range files {
		names[i] = file.Name()
	}
	return names
}
