package repl

import (
	"encoding/binary"
	"fmt"
)

type requestHeader struct {
	requestAPIKey     int
	requestAPIVersion int
	headerContent     headerContent
}

type headerContent struct {
	correlationID int
}

func handleHeader(b []byte) (requestHeader, int, error) {
	requestAPIKey := int(binary.BigEndian.Uint16(b[0:2]))
	requestAPIVersion := int(binary.BigEndian.Uint16(b[2:4]))

	headerContent, contentLength, err := decodeHeader(requestAPIKey, requestAPIVersion, b[4:])
	if err != nil {
		return requestHeader{}, 0, err
	}

	return requestHeader{
		requestAPIKey:     requestAPIKey,
		requestAPIVersion: requestAPIVersion,
		headerContent:     headerContent,
	}, contentLength + 4, nil
}

func decodeHeader(requestAPIKey int, requestAPIVersion int, b []byte) (headerContent, int, error) {
	switch requestAPIKey {
	case 18:
		return decodeHeaderV2(b)
	case 75:
		return decodeHeaderV2(b)
	default:
		return headerContent{}, 0, fmt.Errorf("unsupported API key and version: %d, %d", requestAPIKey, requestAPIVersion)
	}
}

func encodeHeader(header requestHeader) ([]byte, error) {
	switch header.requestAPIKey {
	case 18:
		return encodeHeaderV0(header.headerContent), nil
	case 75:
		return encodeHeaderV1(header.headerContent), nil
	default:
		return nil, fmt.Errorf("unsupported API key and version: %d, %d", header.requestAPIKey, header.requestAPIVersion)
	}
}

func decodeHeaderV2(b []byte) (headerContent, int, error) {
	offset := 0

	// correlation ID, client ID length
	if len(b) < 6 {
		return headerContent{}, 0, fmt.Errorf("expected to read at least 6 bytes for header, but got %d", len(b))
	}

	correlationID := int(binary.BigEndian.Uint32(b[offset : offset+4]))
	offset += 4
	//nolint:gosec // client ID length is actually a int16.
	clientIDLength := max(int(int16(binary.BigEndian.Uint16(b[offset:offset+2]))), 0)
	offset += 2

	// client ID, tag buffer
	if len(b) < offset+clientIDLength+1 {
		return headerContent{}, 0, fmt.Errorf(
			"expected to read %d bytes for client ID and tag buffer, but got %d", clientIDLength+1, len(b)-offset,
		)
	}

	offset += clientIDLength // We don't actually need the client ID currently.
	tagBuffer := int(b[offset])
	offset++

	if tagBuffer != 0 {
		return headerContent{}, 0, fmt.Errorf("expected tag buffer to be 0, but got %d", tagBuffer)
	}

	return headerContent{
		correlationID: correlationID,
	}, offset, nil
}

func encodeHeaderV0(headerContent headerContent) []byte {
	var headerBytes []byte
	headerBytes = append(headerBytes, encodeInt(headerContent.correlationID, 4)...)
	return headerBytes
}

func encodeHeaderV1(headerContent headerContent) []byte {
	var headerBytes []byte
	headerBytes = append(headerBytes, encodeInt(headerContent.correlationID, 4)...)
	headerBytes = append(headerBytes, encodeInt(0, 1)...) // tag buffer
	return headerBytes
}
