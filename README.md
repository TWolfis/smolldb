# smolldb

[![Go Version](https://img.shields.io/badge/go-1.27.1-00ADD8?logo=go&logoColor=white)](go.mod)
[![Test](https://gitea.thematrix.arpa/neo/smolldb/actions/workflows/test.yaml/badge.svg)](https://gitea.thematrix.arpa/neo/smolldb/actions?workflow=test.yaml)
<!--
Enable once the module has a public import path that pkg.go.dev can reach
(change `module smolldb` in go.mod and replace <public/path> below):
[![Go Reference](https://pkg.go.dev/badge/<public/path>/smolldb.svg)](https://pkg.go.dev/<public/path>/smolldb)
-->

A very small in-memory key/value database for Go. It is embedded in your own
program and talks to clients over a unix socket with a tiny text protocol.

- Keys are `int`, values are `string` (values may contain spaces).
- Fixed capacity, set once at creation.
- One global mutex; correct and simple, not built for write throughput.
- No persistence, replication, expiry, or eviction. Everything lives in memory.

It is a library only; there is no standalone server binary.

## Quick start

```go
package main

import (
	"fmt"
	"log"

	"smolldb"
)

func main() {
	db := smolldb.NewSmolldb(1000) // capacity: 1000 entries

	// Listen binds the socket synchronously, so it is ready when it returns.
	if err := db.Listen("unix", smolldb.DEFAULT_SOCKET_PATH); err != nil {
		log.Fatal(err)
	}
	go db.Serve()
	defer db.Close() // closes the listener and removes the socket file

	client, err := db.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Set(1, "hello world"); err != nil {
		log.Fatal(err)
	}

	value, err := client.Get(1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value) // hello world

	if err := client.Delete(1); err != nil {
		log.Fatal(err)
	}
}
```

The module path is currently `smolldb`. To import it from another module, use a
`replace` directive pointing at a local checkout, or rename the module path in
`go.mod`.

## API

| Call | Description |
|---|---|
| `NewSmolldb(size int) *Smolldb` | Create a database that holds at most `size` entries. |
| `(*Smolldb).Listen(network, address string) error` | Bind the listener. Synchronous. |
| `(*Smolldb).Serve() error` | Accept and handle connections until `Close`. Returns `nil` on a clean shutdown. |
| `(*Smolldb).ListenAndServe(network, address string) error` | `Listen` followed by `Serve`. |
| `(*Smolldb).Connect() (*SmolldbClient, error)` | Dial the address the server is listening on. |
| `(*Smolldb).Close()` | Close the listener and remove the socket file. |
| `(*SmolldbClient).Set(key int, value string) error` | Store a value. Fails if the key exists or the database is full. |
| `(*SmolldbClient).Get(key int) (string, error)` | Fetch a value. Errors if the key is missing. |
| `(*SmolldbClient).Delete(key int) error` | Remove a key. |
| `(*SmolldbClient).Close()` | Close the connection. |

`(*Smolldb).Set/Get/Delete` also exist for in-process use, but they do **not**
take the lock. Only requests arriving over the socket are serialized. Use the
client if more than one goroutine touches the database.

## Wire protocol

Plain text over a stream socket. Send one request, then read one response.

| Request | Success response | Error response |
|---|---|---|
| `SET <key> <value>` | `OK` | `Error: key <key> already exists` / `Error: database is full` |
| `GET <key>` | the value | `Error: key <key> not found` |
| `DELETE <key>` | `OK` | |

Malformed requests get `Error: invalid request format`, and unknown commands get
`Error: invalid command: ...`. The connection stays open after an error.

Limits to be aware of:

- Requests and responses are read into a 1024-byte buffer. There is no framing,
  so keep each request and value well under that.
- The server closes a connection that sends nothing for 1 second.
- The default socket path is `/tmp/smolldb.sock` (`DEFAULT_SOCKET_PATH`). Unix
  socket paths are limited to about 104 bytes on macOS.
- `Serve` installs an `os.Interrupt` handler that calls `Close` and then
  `os.Exit(0)`.

## Development

```sh
go test -race ./...                                   # tests
go test . -bench=. -run='^$' -benchmem                # benchmarks
go test . -bench=. -run='^$' -cpuprofile=cpu.prof -o smolldb.test
go tool pprof -top smolldb.test cpu.prof              # inspect the profile
```

Sweep client concurrency with Go's `-cpu` flag:

```sh
go test . -bench=BenchmarkClientSetGetParallel -run='^$' -benchtime=100000x -cpu=1,2,4,8,16,32
```

CI runs `go test ./...` on every push and pull request to `main`
(see [.gitea/workflows/test.yaml](.gitea/workflows/test.yaml)).

## Performance

Measured on an Apple M3 Pro (11 cores, 36 GB). Treat these as ballpark figures.

| Scenario | Result |
|---|---|
| In-process `Set` / `Get` / `Delete` | ~30 / ~13 / ~32 ns/op, 0 allocations |
| Client round trip over the socket (`Set` + `Get`) | ~5.7 µs |
| 50M entries, sequential insert | ~14 s, ~52 bytes/entry, ~3 GB RSS |
| 50M entries, random `Get` | ~61 ns/op |
| Concurrent clients | latency roughly doubles by 16 clients, then plateaus |

The plateau comes from the single mutex in the request handler: more clients do
not add throughput. Above about 64 concurrent clients on this machine, some
connections hit the 1-second read deadline and were closed by the server.
