/* SPDX-License-Identifier: MPL-2.0
 *
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package writablefs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/bucket-sailor/writablefs"
	"github.com/bucket-sailor/writablefs/dirfs"
	"github.com/bucket-sailor/writablefs/s3fs"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestFS(t *testing.T) {
	logger := slogt.New(t)

	type testCase struct {
		name  string
		new   func() (writablefs.FS, any, error)
		after func(t *testing.T, fsys writablefs.FS, testContext any)
	}

	testCases := []testCase{
		{
			name: "Dir",
			new: func() (writablefs.FS, any, error) {
				storageDir := t.TempDir()
				fsys, err := dirfs.New(storageDir)
				return fsys, storageDir, err
			},
			after: func(t *testing.T, fsys writablefs.FS, testContext any) {
				storageDir := testContext.(string)
				require.NoError(t, os.RemoveAll(storageDir))
			},
		},
		{
			name: "S3",
			new: func() (writablefs.FS, any, error) {
				s3ContainerReq := tc.ContainerRequest{
					Image:        "chrislusf/seaweedfs",
					ExposedPorts: []string{"8333/tcp"},
					Cmd:          []string{"server", "-s3", "-dir=/data"},
					Mounts: tc.ContainerMounts{
						{
							Source: tc.GenericVolumeMountSource{
								Name: "seaweedfs-data",
							},
							Target: "/data",
						},
					},
					WaitingFor: wait.ForListeningPort("8333/tcp"),
				}

				ctx := context.Background()
				s3Container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
					ContainerRequest: s3ContainerReq,
					Started:          true,
				})
				require.NoError(t, err)

				t.Log("Configuring S3")

				weedCommands := `
	s3.bucket.create -name test
	s3.configure -access_key=admin -secret_key=admin -buckets=test -user=admin -actions=Read,Write,List,Tagging,Admin -apply
	`

				ret, _, err := s3Container.Exec(ctx, []string{"/bin/sh", "-c", "echo '" + weedCommands + "' | weed shell"})
				require.NoError(t, err)
				require.Zero(t, ret)

				dockerHost, err := s3Container.Host(ctx)
				require.NoError(t, err)

				s3ContainerPort, err := s3Container.MappedPort(ctx, "8333")
				require.NoError(t, err)

				opts := s3fs.Options{
					EndpointURL: fmt.Sprintf("http://%s:%s", dockerHost, s3ContainerPort.Port()),
					Credentials: credentials.NewStaticV4("admin", "admin", ""),
					BucketName:  "test",
				}

				fsys, err := s3fs.New(context.Background(), logger, opts)
				return fsys, s3Container, err
			},
			after: func(t *testing.T, fsys writablefs.FS, testContext any) {
				require.NoError(t, fsys.Close())

				s3Container := testContext.(tc.Container)
				require.NoError(t, s3Container.Terminate(context.Background()))
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Run("Basic", func(t *testing.T) {
				fsys, testContext, err := testCase.new()
				require.NoError(t, err)
				t.Cleanup(func() {
					testCase.after(t, fsys, testContext)
				})

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
		})
	}
}

func fileNames(files []os.DirEntry) []string {
	names := make([]string, len(files))
	for i, file := range files {
		names[i] = file.Name()
	}
	return names
}
