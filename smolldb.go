// smolldb is a very small in-memory database implementation
package smolldb

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	allowedCommands     = "SET, GET, DELETE"
	DEFAULT_SOCKET_PATH = "/tmp/smolldb.sock"
)

type Smolldb struct {
	Data  map[int]string
	items int
	size  int

	// Listener for the unix socket server
	listener net.Listener

	// address the listener is bound to, set by ListenAndServe and used by
	// Connect/Close so a server doesn't have to run on DEFAULT_SOCKET_PATH
	address string

	// add a mutex to protect concurrent access to the database
	mutex sync.Mutex
}

// NewSmolldb creates a new instance of Smolldb with the specified size
func NewSmolldb(size int) *Smolldb {
	return &Smolldb{
		Data:  make(map[int]string, size),
		items: 0,
		size:  size,
	}
}

// Close stops the server, if running, and cleans up its socket file
func (s *Smolldb) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
	if s.address != "" {
		os.Remove(s.address)
	}
}

// Set stores a value in the database
func (s *Smolldb) Set(key int, value string) error {
	if s.items >= s.size {
		return fmt.Errorf("database is full")
	}

	if _, exists := s.Data[key]; exists {
		return fmt.Errorf("key %d already exists", key)
	}
	s.Data[key] = value
	s.items++
	return nil
}

// Get retrieves a value from the database
func (s *Smolldb) Get(key int) (string, bool) {
	value, exists := s.Data[key]
	return value, exists
}

// Delete removes a value from the database
func (s *Smolldb) Delete(key int) {
	delete(s.Data, key)
	s.items--
}

// Listen binds the server to the given network address. It runs
// synchronously so that, unlike ListenAndServe, callers know the listener
// (and s.address, used by Connect/Close) is ready as soon as it returns —
// no sleep-and-hope needed to avoid racing a background goroutine. Call
// Serve afterwards, typically in a goroutine, to start accepting connections.
func (s *Smolldb) Listen(network, address string) error {
	listener, err := net.Listen(network, address)
	if err != nil {
		return err
	}
	s.listener = listener
	s.address = address
	return nil
}

// Serve accepts and handles connections until the listener is closed.
func (s *Smolldb) Serve() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			fmt.Printf("Received signal: %s. Shutting down...\n", sig)
			s.Close()
			os.Exit(0)
		}
	}()

	for {
		// Placeholder for accepting connections and handling requests
		conn, err := s.listener.Accept()
		if err != nil {
			// Close() closing the listener is a normal shutdown, not a failure.
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		go handleConn(conn, s)
	}
}

// ListenAndServe binds to the given network address and serves until the
// listener is closed or an unrecoverable error occurs.
func (s *Smolldb) ListenAndServe(network, address string) error {
	if err := s.Listen(network, address); err != nil {
		return err
	}
	return s.Serve()
}

type SmolldbClient struct {
	conn *net.UnixConn
}

func (c *SmolldbClient) Close() {
	c.conn.Close()
}

func (c *SmolldbClient) Get(key int) (string, error) {
	request := fmt.Sprintf("GET %d", key)
	_, err := c.conn.Write([]byte(request))
	if err != nil {
		return "", err
	}

	buf := make([]byte, 1024)
	n, err := c.conn.Read(buf)
	if err != nil {
		return "", err
	}

	response := string(buf[:n])
	if response == fmt.Sprintf("Error: key %d not found", key) {
		return "", fmt.Errorf("key %d not found", key)
	}

	return response, nil
}

// Set stores a value in the database
func (c *SmolldbClient) Set(key int, value string) error {
	request := fmt.Sprintf("SET %d %s", key, value)
	_, err := c.conn.Write([]byte(request))
	if err != nil {
		return err
	}

	buf := make([]byte, 1024)
	n, err := c.conn.Read(buf)
	if err != nil {
		return err
	}

	response := string(buf[:n])
	if response != "OK" {
		return fmt.Errorf("failed to set value: %s", response)
	}

	return nil
}

// Delete removes a value from the database
func (c *SmolldbClient) Delete(key int) error {
	request := fmt.Sprintf("DELETE %d", key)
	_, err := c.conn.Write([]byte(request))
	if err != nil {
		return err
	}

	buf := make([]byte, 1024)
	n, err := c.conn.Read(buf)
	if err != nil {
		return err
	}

	response := string(buf[:n])
	if response != "OK" {
		return fmt.Errorf("failed to delete value: %s", response)
	}

	return nil
}

// connect to the local unix socket server and send a request
func (s *Smolldb) Connect() (*SmolldbClient, error) {
	addr, err := net.ResolveUnixAddr("unix", s.address)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		return nil, err
	}

	return &SmolldbClient{conn: conn}, nil
}

func handleConn(conn net.Conn, s *Smolldb) {
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		readDeadline := time.Now().Add(1 * time.Second)
		err := conn.SetDeadline(readDeadline)
		if err != nil {
			fmt.Println("Error setting read deadline:", err)
			return
		}

		// read data from the connection
		n, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			} else {
				fmt.Println("Error reading from connection:", err)
				return
			}
		}

		// Processing the request and sending a response
		command, key, value, err := parseRequest(string(buf[:n]))
		if err != nil {
			fmt.Println("Error parsing request:", err)
			conn.Write([]byte(fmt.Sprintf("Error: %s", err.Error())))
			continue
		}

		s.handleCommand(conn, command, key, value)
	}
}

// handleCommand runs a single parsed command against the database under lock
// and writes the response. The lock is released via defer so every branch
// below (including early returns) releases it exactly once.
func (s *Smolldb) handleCommand(conn net.Conn, command string, key int, value string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	switch command {
	case "SET":
		err := s.Set(key, value)
		if err != nil {
			fmt.Println("Error setting value:", err)
			conn.Write([]byte(fmt.Sprintf("Error: %s", err.Error())))
			return
		}
		conn.Write([]byte("OK"))
	case "GET":
		value, exists := s.Get(key)
		if !exists {
			conn.Write([]byte(fmt.Sprintf("Error: key %d not found", key)))
			return
		}
		conn.Write([]byte(value))
	case "DELETE":
		s.Delete(key)
		conn.Write([]byte("OK"))
	default:
		conn.Write([]byte(fmt.Sprintf("Error: invalid command %s. Allowed commands are: %s", command, allowedCommands)))
	}
}

func parseRequest(request string) (string, int, string, error) {
	// SplitN with a limit of 3 keeps spaces inside a SET value intact
	// instead of truncating it at the first space.
	parts := strings.SplitN(strings.TrimSpace(request), " ", 3)
	if len(parts) < 2 {
		return "", 0, "", fmt.Errorf("invalid request format")
	}

	command := parts[0]
	if command != "SET" && command != "GET" && command != "DELETE" {
		return "", 0, "", fmt.Errorf("invalid command: %s. Allowed commands are: %s", command, allowedCommands)
	}

	key, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, "", fmt.Errorf("invalid request format")
	}

	var value string
	if command == "SET" {
		if len(parts) < 3 {
			return "", 0, "", fmt.Errorf("invalid request format")
		}
		value = parts[2]
	}

	return command, key, value, nil
}
