package repl

import (
	"errors"
	"fmt"
	"net"
)

func REPL(conn net.Conn) (err error) {
	defer func() {
		err = errors.Join(err, conn.Close())
	}()

	for {
		buf := make([]byte, 4)
		n, err := conn.Read(buf)
		if err != nil {
			return fmt.Errorf("error reading from connection: %w", err)
		}
		if n != 4 {
			return fmt.Errorf("expected to read 4 bytes, but read %d", n)
		}
		messageSize := bytesToInt(buf, 0, 4)
		buf = make([]byte, messageSize)
		n, err = conn.Read(buf)
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
	requestAPIKey := bytesToInt(b, 0, 2)
	if requestAPIKey > 18 {
		return messageField{}, fmt.Errorf("unsupported API key: %d", requestAPIKey)
	}

	return messageField{
		requestAPIKey:     requestAPIKey,
		requestAPIVersion: bytesToInt(b, 2, 2),
		correlationID:     bytesToInt(b, 4, 4),
	}, nil
}

func generateResponse(field messageField) []byte {
	var response []byte
	response = append(response, intToBytes(field.correlationID, 4)...)

	errorCode := 0
	if field.requestAPIVersion >= 5 {
		errorCode = 35 // 35 = UNSUPPORTED_VERSION
	}
	response = append(response, intToBytes(errorCode, 2)...)

	// api keys
	response = append(response, intToBytes(2, 1)...)  // 2 api keys
	response = append(response, intToBytes(18, 2)...) // API key 18 (API_VERSIONS)
	response = append(response, intToBytes(0, 2)...)  // min version
	response = append(response, intToBytes(4, 2)...)  // max version
	response = append(response, intToBytes(0, 1)...)  // tag buffer

	// throttle_time_ms
	response = append(response, intToBytes(0, 4)...)
	response = append(response, intToBytes(0, 1)...) // tag buffer

	responseSize := len(response)
	response = append(intToBytes(responseSize, 4), response...)
	return response
}

func bytesToInt(b []byte, start, length int) int {
	result := 0
	for i := 0; i < length; i++ {
		result = (result << 8) | int(b[start+i])
	}
	return result
}

func intToBytes(n int, length int) []byte {
	b := make([]byte, length)
	for i := length - 1; i >= 0; i-- {
		b[i] = byte(n & 0xFF)
		n >>= 8
	}
	return b
}
