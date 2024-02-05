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
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"testing"

	"github.com/bucket-sailor/writablefs/dirfs"
	"github.com/bucket-sailor/writablefs/s3fs"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestFilesystems(t *testing.T) {
	logger := slogt.New(t)

	ctx := context.Background()

	s3Container, s3EndpointURL, err := startS3Server(ctx, logger)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, s3Container.Terminate(ctx))
	})

	t.Run("Directory", func(t *testing.T) {
		storageDir := t.TempDir()

		fsys, err := dirfs.New(storageDir)
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, fsys.Close())
		})

		// Test the filesystem
		testBasicOperations(t, fsys)
		testXAttrs(t, fsys)
		testDirectoryArchive(t, fsys)
	})

	t.Run("S3 - SeaweedFS", func(t *testing.T) {
		opts := s3fs.Options{
			EndpointURL: s3EndpointURL,
			Credentials: credentials.NewStaticV4("admin", "admin", ""),
			BucketName:  "test",
		}

		fsys, err := s3fs.New(ctx, logger, opts)
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, fsys.Close())
		})

		// Test the filesystem
		testBasicOperations(t, fsys)
		testXAttrs(t, fsys)
		testDirectoryArchive(t, fsys)
	})
}

func startS3Server(ctx context.Context, logger *slog.Logger) (tc.Container, string, error) {
	req := tc.ContainerRequest{
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

	ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to start S3 server: %w", err)
	}

	logger.Info("Configuring S3 server")

	weedCommands := `
s3.bucket.create -name test
s3.configure -access_key=admin -secret_key=admin -buckets=test -user=admin -actions=Read,Write,List,Tagging,Admin -apply
`

	ret, _, err := ctr.Exec(ctx, []string{"/bin/sh", "-c", "echo '" + weedCommands + "' | weed shell"})
	if err != nil || ret != 0 {
		return ctr, "", fmt.Errorf("failed to configure S3 server: %w", err)
	}

	dockerHost, err := ctr.Host(ctx)
	if err != nil {
		return ctr, "", fmt.Errorf("failed to get S3 server host: %w", err)
	}

	s3Port, err := ctr.MappedPort(ctx, "8333")
	if err != nil {
		return ctr, "", fmt.Errorf("failed to get S3 server port: %w", err)
	}

	endpointURL := fmt.Sprintf("http://%s:%s", dockerHost, s3Port.Port())

	return ctr, endpointURL, nil
}

func randomString(n int) string {
	var letters = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_.")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}

	return string(s)
}
