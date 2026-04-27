package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/internal/repl"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
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
