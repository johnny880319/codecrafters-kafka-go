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
		message := parseMessage(buf)
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

func parseMessage(b []byte) messageField {
	return messageField{
		requestAPIKey:     bytesToInt(b, 0, 2),
		requestAPIVersion: bytesToInt(b, 2, 2),
		correlationID:     bytesToInt(b, 4, 4),
	}
}

func generateResponse(field messageField) []byte {
	var responseHeader []byte
	responseHeader = append(responseHeader, intToBytes(field.correlationID, 4)...)

	errorCode := 0
	if field.requestAPIVersion >= 5 {
		errorCode = 35 // 35 = UNSUPPORTED_VERSION
	}
	responseHeader = append(responseHeader, intToBytes(errorCode, 2)...)

	responseSize := len(responseHeader)
	response := append(intToBytes(responseSize, 4), responseHeader...)
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
