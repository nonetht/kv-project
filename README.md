# bitcask-go

A lightweight Bitcask-style key-value store written in Go. Data is appended to log-structured segment files with an in-memory index for fast lookups, plus simple transactional batches.

## Features
- Append-only data files with automatic file rotation.
- In-memory B-Tree index for O(log n) lookups.
- Batched writes via `WriteBatch` with optional fsync on commit.
- Basic iteration support over sorted keys.

## Getting Started
Prerequisite: Go 1.25+

```bash
git clone <repo-url>
cd bitcask-go
go test ./...          # run full test suite
```

## Usage
```go
package main

import "bitcask-go"

func main() {
    setup := bitcask_go.DefaultSetup
    db, err := bitcask_go.Open(setup)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    _ = db.Put([]byte("name"), []byte("alice"))
    val, _ := db.Get([]byte("name"))

    batch := db.NewWriteBatch(bitcask_go.DefaultWriteBatchSetup)
    _ = batch.Put([]byte("city"), []byte("shanghai"))
    _ = batch.Delete([]byte("name"))
    _ = batch.Commit()

    _, _ = val, batch
}
```

## Testing
- Run all tests: `go test ./...`
- If sandboxed environments block the default Go build cache, set an explicit cache dir: `GOCACHE=$(pwd)/.gocache go test ./...`

## Project Layout
- `db.go` – core DB operations, file management, and index rebuild.
- `batch.go` – transactional `WriteBatch` implementation.
- `data/` – data file and log record encoding/decoding.
- `index/` – in-memory B-Tree index abstraction.
- `utils/` – helpers for test keys/values.
- `*_test.go` – unit tests covering DB operations, batches, and data layer.
