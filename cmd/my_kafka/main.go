// Package main is the entry point for the my_kafka server, which listens for
// TCP connections on port 9092 and handles them using the repl package.
package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/internal/repl"
)

func main() {
	lc := net.ListenConfig{}

	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	defer func() {
		if closeErr := l.Close(); closeErr != nil {
			fmt.Fprintln(os.Stderr, closeErr)
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}

		go func(conn net.Conn) {
			err := repl.HandleConnection(conn)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}(conn)
	}
}
