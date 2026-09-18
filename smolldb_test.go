package smolldb

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func SetupTestServer(t *testing.T) *Smolldb {
	// Each test gets its own socket path so tests don't collide with each
	// other or with a leftover socket file from a previous run.
	socketPath := filepath.Join(t.TempDir(), "smolldb.sock")

	s := NewSmolldb(200)
	// Listen runs synchronously, so s.address is set and the socket is
	// bound before this function returns - no sleep-based guessing needed.
	if err := s.Listen("unix", socketPath); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	go s.Serve()

	return s
}

// Test write operation on the database
func WriteSingleOp(sc *SmolldbClient) error {
	content := "Hello, World!"
	err := sc.Set(1, content)
	if err != nil {
		return err
	}
	return nil
}

func ReadSingleOp(sc *SmolldbClient) error {
	value, err := sc.Get(1)
	if err != nil {
		return err
	}
	if value != "Hello, World!" {
		return fmt.Errorf("expected 'Hello, World!', got '%s'", value)
	}
	return nil
}

func TestWriteAndRead(t *testing.T) {
	// Start the server in a separate goroutine
	s := SetupTestServer(t)

	defer s.Close()

	// Connect to the server
	client, err := s.Connect()
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer client.Close()

	// Test write operation
	err = WriteSingleOp(client)
	if err != nil {
		t.Fatalf("Write operation failed: %v", err)
	}

	// Test read operation
	err = ReadSingleOp(client)
	if err != nil {
		t.Fatalf("Read operation failed: %v", err)
	}
}

// multi-client test to check for deadlocks
func TestMultiClient(t *testing.T) {
	// Start the server in a separate goroutine
	s := SetupTestServer(t)
	defer s.Close() // Ensure the server is closed after the test
	var wg sync.WaitGroup

	// Create multiple clients
	numClients := 50
	var clients []*SmolldbClient
	for i := 0; i < numClients; i++ {
		client, err := s.Connect()
		if err != nil {
			t.Fatalf("Failed to connect client %d: %v", i, err)
		}
		defer client.Close()
		clients = append(clients, client)
	}

	// Each client performs write and read operations
	for i, client := range clients {
		wg.Add(1)
		go func(c *SmolldbClient, idx int) {
			defer wg.Done()
			key := idx + 1
			value := fmt.Sprintf("value%d", key)

			err := c.Set(key, value)
			if err != nil {
				t.Errorf("Client %d failed to set value: %v", idx, err)
				return
			}

			readValue, err := c.Get(key)
			if err != nil {
				t.Errorf("Client %d failed to get value: %v", idx, err)
				return
			}

			if readValue != value {
				t.Errorf("Client %d expected value '%s', got '%s'", idx, value, readValue)
			}
		}(client, i)
	}

	wg.Wait()
}
