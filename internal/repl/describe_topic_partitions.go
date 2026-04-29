package repl

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
)

func encodeDescribeTopicPartitionsV0(_ requestHeader, body []byte) ([]byte, error) {
	// read metadata
	if err := parseMetadata(); err != nil {
		return nil, err
	}

	// read request
	offset := 0

	topicArrayLengthRaw, n := binary.Uvarint(body[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("error reading topic array length: %d", n)
	}
	offset += n
	//nolint:gosec // we assume the client is well-behaved and won't send a huge topic array length.
	topicArrayLength := max(int(topicArrayLengthRaw)-1, 0)

	topicNames := make([]string, topicArrayLength)
	topicNameLengthRaws := make([]uint64, topicArrayLength)
	for i := 0; i < topicArrayLength; i++ {
		topicNameLengthRaw, n := binary.Uvarint(body[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("error reading topic name length: %d", n)
		}
		offset += n

		topicNameLengthRaws[i] = topicNameLengthRaw
		//nolint:gosec // we assume the client is well-behaved and won't send a huge topic name length.
		topicNameLength := max(int(topicNameLengthRaw)-1, 0)
		topicNames[i] = string(body[offset : offset+topicNameLength])
		offset += topicNameLength
	}

	// write response
	response := make([]byte, 0)
	response = append(response, encodeInt(0, 4)...)                // throttle_time_ms
	response = binary.AppendUvarint(response, topicArrayLengthRaw) // topic array length

	for i, topicName := range topicNames {
		response = append(response, encodeInt(3, 2)...)                   // error code UNKNOWN_TOPIC
		response = binary.AppendUvarint(response, topicNameLengthRaws[i]) // topic name length
		response = append(response, []byte(topicName)...)                 // topic name
		response = append(response, encodeInt(0, 16)...)                  // topic ID (null UUID)
		response = append(response, encodeInt(0, 1)...)                   // is internal = false
		response = binary.AppendUvarint(response, 1)                      // partition array length
		response = append(response, encodeInt(0, 4)...)                   // topic authorized operations
		response = binary.AppendUvarint(response, 0)                      // tag buffer
	}

	response = append(response, encodeInt(255, 1)...) // next cursor = -1 (no more topics)
	response = binary.AppendUvarint(response, 0)      // tag buffer

	return response, nil
}

func parseMetadata() error {
	filePath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	fmt.Print(hex.Dump(fileBytes))
	return nil
}
