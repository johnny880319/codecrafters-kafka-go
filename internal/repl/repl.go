package repl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

func HandleConnection(conn net.Conn) (err error) {
	defer func() {
		err = errors.Join(err, conn.Close())
	}()

	for {
		buf := make([]byte, 4)
		n, err := io.ReadFull(conn, buf)
		if err != nil {
			return fmt.Errorf("error reading from connection: %w", err)
		}
		if n != 4 {
			return fmt.Errorf("expected to read 4 bytes, but read %d", n)
		}

		messageSize := int(binary.BigEndian.Uint32(buf[0:4]))
		if messageSize < 8 || messageSize > 1<<20 { // 8 bytes for header, 1MB max message size
			return fmt.Errorf("invalid message size: %d", messageSize)
		}

		buf = make([]byte, messageSize)
		n, err = io.ReadFull(conn, buf)
		if err != nil {
			return fmt.Errorf("error reading from connection: %w", err)
		}
		if n != messageSize {
			return fmt.Errorf("expected to read %d bytes, but read %d", messageSize, n)
		}
		message, err := parseMessage(buf)
		if err != nil {
			return fmt.Errorf("error parsing message: %w", err)
		}
		response := generateResponse(message)

		_, err = conn.Write(response)
		if err != nil {
			return fmt.Errorf("error writing to connection: %w", err)
		}
	}
}

type messageField struct {
	requestAPIKey     int
	requestAPIVersion int
	correlationID     int
}

func parseMessage(b []byte) (messageField, error) {
	requestAPIKey := int(binary.BigEndian.Uint16(b[0:2]))
	if requestAPIKey > 18 {
		return messageField{}, fmt.Errorf("unsupported API key: %d", requestAPIKey)
	}

	return messageField{
		requestAPIKey:     requestAPIKey,
		requestAPIVersion: int(binary.BigEndian.Uint16(b[2:4])),
		correlationID:     int(binary.BigEndian.Uint32(b[4:8])),
	}, nil
}

func generateResponse(field messageField) []byte {
	var response []byte
	response = append(response, encodeInt(field.correlationID, 4)...)

	errorCode := 0
	if field.requestAPIVersion >= 5 {
		errorCode = 35 // 35 = UNSUPPORTED_VERSION
	}
	response = append(response, encodeInt(errorCode, 2)...)

	// api keys
	response = append(response, encodeInt(3, 1)...) // 2 api keys + 1

	response = append(response, encodeInt(18, 2)...) // API key 18 (APIVersions)
	response = append(response, encodeInt(0, 2)...)  // min version
	response = append(response, encodeInt(4, 2)...)  // max version
	response = append(response, encodeInt(0, 1)...)  // tag buffer

	response = append(response, encodeInt(75, 2)...) // API key 75 (DescribeTopicPartitions)
	response = append(response, encodeInt(0, 2)...)  // min version
	response = append(response, encodeInt(0, 2)...)  // max version
	response = append(response, encodeInt(0, 1)...)  // tag buffer

	// throttle_time_ms
	response = append(response, encodeInt(0, 4)...) // 0 ms
	response = append(response, encodeInt(0, 1)...) // tag buffer

	responseSize := len(response)
	response = append(encodeInt(responseSize, 4), response...)
	return response
}

func encodeInt(n int, length int) []byte {
	b := make([]byte, length)
	for i := length - 1; i >= 0; i-- {
		b[i] = byte(n & 0xFF)
		n >>= 8
	}
	return b
}
