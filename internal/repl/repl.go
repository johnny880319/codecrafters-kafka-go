// Package repl provides functionality for handling Kafka protocol messages
// over a TCP connection.
package repl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// HandleConnection handles a single TCP connection, reading Kafka protocol messages,
// processing them, and writing responses until the connection is closed.
func HandleConnection(conn net.Conn) (err error) {
	defer func() {
		err = errors.Join(err, conn.Close())
	}()

	for {
		header, body, err := readMessage(conn)
		if err != nil {
			return fmt.Errorf("error reading message: %w", err)
		}
		err = writeMessage(conn, header, body)
		if err != nil {
			return fmt.Errorf("error writing message: %w", err)
		}
	}
}

func readMessage(conn net.Conn) (requestHeader, []byte, error) {
	// read bytes
	buf := make([]byte, 4)
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return requestHeader{}, nil, fmt.Errorf("error reading from connection: %w", err)
	}
	if n != 4 {
		return requestHeader{}, nil, fmt.Errorf("expected to read 4 bytes, but read %d", n)
	}

	messageSize := int(binary.BigEndian.Uint32(buf[0:4]))
	if messageSize < 8 || messageSize > 1<<20 { // 8 bytes for header, 1MB max message size
		return requestHeader{}, nil, fmt.Errorf("invalid message size: %d", messageSize)
	}

	buf = make([]byte, messageSize)
	n, err = io.ReadFull(conn, buf)
	if err != nil {
		return requestHeader{}, nil, fmt.Errorf("error reading from connection: %w", err)
	}
	if n != messageSize {
		return requestHeader{}, nil, fmt.Errorf("expected to read %d bytes, but read %d", messageSize, n)
	}

	// handle message
	header, headerLength, err := handleHeader(buf)
	if err != nil {
		return requestHeader{}, nil, fmt.Errorf("error handling header: %w", err)
	}
	return header, buf[headerLength:], nil
}

func writeMessage(conn net.Conn, header requestHeader, body []byte) error {
	response, err := encodeHeader(header)
	if err != nil {
		return fmt.Errorf("error encoding header: %w", err)
	}
	responseBody, err := generateResponse(header, body)
	if err != nil {
		return fmt.Errorf("error generating response: %w", err)
	}
	response = append(response, responseBody...)
	response = append(encodeInt(len(response), 4), response...)

	_, err = conn.Write(response)
	if err != nil {
		return fmt.Errorf("error writing to connection: %w", err)
	}
	return nil
}

func generateResponse(header requestHeader, body []byte) ([]byte, error) {
	switch header.requestAPIKey {
	case 18:
		return encodeAPIVersionsV4(header, body)
	case 75:
		return encodeDescribeTopicPartitionsV0(header, body)
	default:
		return nil, fmt.Errorf("unsupported API key: %d", header.requestAPIKey)
	}
}
