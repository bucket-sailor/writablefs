# writablefs

A superset of [fs.FS](https://pkg.go.dev/io/fs#FS) supporting write operations.

Inspired by [hackpadfs](https://github.com/hack-pad/hackpadfs), [rclone](https://github.com/rclone/rclone), and [s3fs](https://github.com/jszwec/s3fs).

## Backends

* Local directory filesystem.
* S3 compatible object storage.

## Usage

To use an S3 compatible object storage as a read-write filesystem:

```go
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
```

Caveats:

* S3 objects are immutable (but versionable). This means changing a single byte in a file will result in a new object being created. To avoid this becoming a huge problem the S3 backend will only flush/upload writes when the file is closed or when Sync() is explicitly called. This means that if the program crashes or is killed pending writes will be lost. So flush as often as makes sense, also perhaps consider spreading writes across multiple smaller files.
* Not all S3 implementations are strongly consistent (but [Amazon](https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/) and a lot of [others](https://developers.cloudflare.com/r2/reference/consistency/) are). This means writes may not be immediately visible to other clients.

## TODOs

* [ ] Port the [Filesystem Test Suite](https://github.com/zfsonlinux/fstest) to Go (of course almost no backends will be fully compliant).
* [ ] Add POSIX attributes to S3 objects (e.g. owner, group, permissions) via [S3 object metadata](https://docs.aws.amazon.com/fsx/latest/LustreGuide/posix-metadata-support.html).
* [ ] Most providers now offer strong read-after-write and metadata consistency. This means we can implement distributed flock()!