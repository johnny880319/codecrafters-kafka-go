package main

import (
	"errors"
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	defer func() {
		if l.Close() != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}

		go func(conn net.Conn) {
			err := handleConnection(conn)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}(conn)
	}
}

func handleConnection(conn net.Conn) (err error) {
	defer func() {
		err = errors.Join(err, conn.Close())
	}()

	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			return fmt.Errorf("error reading from connection: %w", err)
		}

		_, err = conn.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07})
		if err != nil {
			return fmt.Errorf("error writing to connection: %w", err)
		}
	}
}
