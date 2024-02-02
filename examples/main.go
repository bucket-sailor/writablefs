/* SPDX-License-Identifier: MPL-2.0
 *
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/bucket-sailor/writablefs"
	"github.com/bucket-sailor/writablefs/s3fs"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	opts := s3fs.Options{
		EndpointURL: "http://localhost:8080",
		Credentials: credentials.NewStaticV4("admin", "admin", ""),
		BucketName:  "test",
	}

	logger := slog.Default()
	fsys, err := s3fs.New(context.Background(), logger, opts)
	if err != nil {
		logger.Error("Failed to create S3 FS", "error", err)
		os.Exit(1)
	}
	defer fsys.Close()

	// Use fsys as a fs.FS or as a writablefs.FS.

	f, err := fsys.OpenFile("test.txt", writablefs.FlagReadWrite|writablefs.FlagCreate)
	if err != nil {
		logger.Error("Failed to open file", "error", err)
		os.Exit(1)
	}

	if _, err := f.Write([]byte("Hello, world!")); err != nil {
		logger.Error("Failed to write to file", "error", err)
		os.Exit(1)
	}

	if err := f.Close(); err != nil {
		logger.Error("Failed to close file", "error", err)
		os.Exit(1)
	}
}
