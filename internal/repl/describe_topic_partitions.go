package repl

import (
	"encoding/binary"
	"fmt"
	"sort"
)

func encodeDescribeTopicPartitionsV0(_ requestHeader, body []byte) ([]byte, error) {
	// read metadata
	topicRecords, partitionRecords, err := parseMetadata()
	if err != nil {
		return nil, err
	}

	// read request
	offset := 0

	topicArrayLength, n, err := readCompactLength(body[offset:])
	if err != nil {
		return nil, fmt.Errorf("error reading topic array length: %w", err)
	}
	offset += n

	topicNames := make([]string, topicArrayLength)
	for i := 0; i < topicArrayLength; i++ {
		topicName, n, err := readCompactString(body[offset:])
		if err != nil {
			return nil, err
		}
		offset += n
		topicNames[i] = topicName

		_, n = binary.Uvarint(body[offset:]) // skip tag buffer
		if n <= 0 {
			return nil, fmt.Errorf("error reading tag buffer length: %d", n)
		}
		offset += n
	}
	sort.Strings(topicNames)

	// write response
	response := make([]byte, 0)
	response = append(response, encodeInt(0, 4)...) // throttle_time_ms
	//nolint:gosec // we assume the client is well-behaved and won't send a huge topic array length.
	response = binary.AppendUvarint(response, uint64(topicArrayLength)+1) // topic array length

	for _, topicName := range topicNames {
		topicRecord, ok := topicRecords[topicName]

		if ok {
			response = append(response, encodeInt(errorCodeNone, 2)...) // error code
		} else {
			response = append(response, encodeInt(errorCodeUnknownTopicOrPartition, 2)...) // error code
		}
		response = binary.AppendUvarint(response, uint64(len(topicName)+1)) // topic name length
		response = append(response, []byte(topicName)...)                   // topic name
		if ok {
			response = append(response, topicRecord.topicUUID[:]...) // topic ID
		} else {
			response = append(response, encodeInt(0, 16)...) // topic ID (null UUID)
		}
		response = append(response, encodeInt(0, 1)...) // is internal = false
		if ok {
			responsePartitionRecords := encodePartitionRecord(partitionRecords, topicRecord.topicUUID)
			response = append(response, responsePartitionRecords...)
		} else {
			response = binary.AppendUvarint(response, 1) // empty partition array length
		}
		response = append(response, encodeInt(0, 4)...) // topic authorized operations
		response = binary.AppendUvarint(response, 0)    // tag buffer
	}

	response = append(response, encodeInt(255, 1)...) // next cursor = -1 (no more topics)
	response = binary.AppendUvarint(response, 0)      // tag buffer

	return response, nil
}

func encodePartitionRecord(partitionRecords map[[16]byte][]partitionRecord, topicUUID [16]byte) []byte {
	records, ok := partitionRecords[topicUUID]
	if !ok {
		var response []byte
		return binary.AppendUvarint(response, 1)
	}

	var response []byte
	response = binary.AppendUvarint(response, uint64(len(records)+1))
	for _, record := range records {
		response = append(response, encodeInt(errorCodeNone, 2)...) // error code NONE
		response = append(response, encodeInt(record.partitionID, 4)...)
		response = append(response, encodeInt(record.leader, 4)...)
		response = append(response, encodeInt(record.leaderEpoch, 4)...)

		response = binary.AppendUvarint(response, uint64(len(record.replicas)+1))
		for _, replica := range record.replicas {
			response = append(response, encodeInt(replica, 4)...)
		}

		response = binary.AppendUvarint(response, uint64(len(record.ISRs)+1))
		for _, isr := range record.ISRs {
			response = append(response, encodeInt(isr, 4)...)
		}

		// Assume no eligible leaders replicas, last, known ELR, offline replicas.
		for i := 0; i < 3; i++ {
			response = binary.AppendUvarint(response, 1) // empty array length
		}
		response = binary.AppendUvarint(response, 0) // tag buffer
	}
	return response
}
