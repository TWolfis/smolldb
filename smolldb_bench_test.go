package smolldb

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// In-process benchmarks: exercise Set/Get/Delete directly, without going
// through the socket protocol, to isolate the cost of the data structure
// and locking from network/parsing overhead.

func BenchmarkSet(b *testing.B) {
	s := NewSmolldb(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Set(i, "value"); err != nil {
			b.Fatalf("set failed: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	s := NewSmolldb(b.N)
	for i := 0; i < b.N; i++ {
		s.Set(i, "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, exists := s.Get(i); !exists {
			b.Fatalf("key %d unexpectedly missing", i)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	s := NewSmolldb(b.N)
	for i := 0; i < b.N; i++ {
		s.Set(i, "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Delete(i)
	}
}

// BenchmarkClientSetGet exercises the full client/server round trip
// (socket write, parseRequest, locked dispatch, socket read) for a single
// client doing sequential SET+GET pairs.
func BenchmarkClientSetGet(b *testing.B) {
	s, client := newBenchServer(b, b.N+1)
	defer client.Close()
	defer s.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Set(i, "value"); err != nil {
			b.Fatalf("set failed: %v", err)
		}
		if _, err := client.Get(i); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

// BenchmarkClientSetGetParallel runs many concurrent clients against a
// single server to show how the single mutex in handleCommand behaves
// under contention as the scale (GOMAXPROCS, b.N) grows.
func BenchmarkClientSetGetParallel(b *testing.B) {
	s, _ := newBenchServer(b, b.N+1)
	defer s.Close()

	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client, err := s.Connect()
		if err != nil {
			b.Errorf("failed to connect: %v", err)
			return
		}
		defer client.Close()

		for pb.Next() {
			key := int(counter.Add(1))
			if err := client.Set(key, "value"); err != nil {
				b.Errorf("set failed: %v", err)
				return
			}
			if _, err := client.Get(key); err != nil {
				b.Errorf("get failed: %v", err)
				return
			}
		}
	})
}

func newBenchServer(b *testing.B, size int) (*Smolldb, *SmolldbClient) {
	b.Helper()

	// b.TempDir() nests under a long path (.../T/<benchmark name><random>/001/)
	// that overflows the ~104 byte sockaddr_un limit on macOS/BSD, so bind
	// under /tmp directly with a short, unique name instead.
	dir, err := os.MkdirTemp("/tmp", "smolldb-bench")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })

	socketPath := filepath.Join(dir, "s.sock")
	s := NewSmolldb(size)
	if err := s.Listen("unix", socketPath); err != nil {
		b.Fatalf("failed to start server: %v", err)
	}
	go s.Serve()

	client, err := s.Connect()
	if err != nil {
		s.Close()
		b.Fatalf("failed to connect: %v", err)
	}

	return s, client
}
