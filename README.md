# metarclone

**metarclone** is a command-line utility to sync files with file attributes to rclone remotes. It aggregates files smaller than a certain threshold into larger tarballs, thus useful for remotes where creating lots of small files are not feasible, e.g. Google Drive, S3 Glacier.

***Note: Still under development; partial download sync is not implemented currently**

## Features

- Small-file aggregation while preserving directory structure whenever possible
- File attributes preservation (ctime, mtime, sparse files; permission, owner, ACLs on Linux) using tar
- Partial sync based on whole-file content and/or timestamps
- Configurable compression methods