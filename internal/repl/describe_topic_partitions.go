package repl

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
)

func encodeDescribeTopicPartitionsV0(_ requestHeader, body []byte) ([]byte, error) {
	// read metadata
	if _, _, err := parseMetadata(); err != nil {
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

type topicRecord struct {
	topicName string
	topicUUID string
}

type partitionRecord struct {
	partitionID int
	topicUUID   string
	replicas    []int
	ISRs        []int
	leader      int
	leaderEpoch int
}

func parseMetadata() (map[string]topicRecord, map[string][]partitionRecord, error) {
	filePath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}
	fmt.Fprint(os.Stderr, hex.Dump(fileBytes)) // for debugging

	topicRecords := make(map[string]topicRecord)
	partitionRecords := make(map[string][]partitionRecord)

	offset := 0
	for offset < len(fileBytes) {
		offset += 8 // Base offset
		batchLength := int(binary.BigEndian.Uint32(fileBytes[offset : offset+4]))
		offset += 4
		topicRecords, partitionRecords, err = parseBatch(fileBytes[offset:], topicRecords, partitionRecords)
		if err != nil {
			return nil, nil, err
		}
		offset += batchLength
	}

	return topicRecords, partitionRecords, nil
}

func parseBatch(
	fileBytes []byte,
	topicRecords map[string]topicRecord,
	partitionRecords map[string][]partitionRecord,
) (map[string]topicRecord, map[string][]partitionRecord, error) {
	// skip:
	// partition leader epoch (4 bytes), magic byte (1 byte), CRC (4 bytes),
	// attributes (2 bytes), last offset delta (4 bytes), base timestamp (8 bytes),
	// max timestamp (8 bytes), producer ID (8 bytes), producer epoch (2 bytes), base sequence (4 bytes)
	offset := 45
	recordLength := int(binary.BigEndian.Uint32(fileBytes[offset : offset+4]))
	offset += 4

	for i := 0; i < recordLength; i++ {
		length, n := binary.Varint(fileBytes[offset:])
		if n <= 0 {
			return nil, nil, fmt.Errorf("error reading record length: %d", n)
		}
		offset += n
		var err error
		topicRecords, partitionRecords, err = parseRecord(fileBytes[offset:], topicRecords, partitionRecords)
		if err != nil {
			return nil, nil, err
		}
		offset += int(length)
	}

	return topicRecords, partitionRecords, nil
}

func parseRecord(
	fileBytes []byte,
	topicRecords map[string]topicRecord,
	partitionRecords map[string][]partitionRecord,
) (map[string]topicRecord, map[string][]partitionRecord, error) {
	// skip:
	// attributes (1 byte), timestamp delta (varint), offset delta (varint),
	// key length (varint, assume 0), key (assume 0 bytes), value length (varint), frame version (1 byte)
	offset := 1
	for i := 0; i < 4; i++ {
		_, n := binary.Varint(fileBytes[offset:])
		if n <= 0 {
			return nil, nil, fmt.Errorf("error reading record field: %d", n)
		}
		offset += n
	}
	offset++

	if fileBytes[offset] == 0x02 {
		tr, err := parseTopicRecord(fileBytes[offset+1:])
		if err != nil {
			return nil, nil, err
		}
		topicRecords[tr.topicUUID] = tr
	}
	if fileBytes[offset] == 0x03 {
		pr, err := parsePartitionRecord(fileBytes[offset+1:])
		if err != nil {
			return nil, nil, err
		}
		partitionRecords[pr.topicUUID] = append(partitionRecords[pr.topicUUID], pr)
	}

	return topicRecords, partitionRecords, nil
}

func parseTopicRecord(fileBytes []byte) (topicRecord, error) {
	// skip: version (1 byte)
	offset := 1

	nameLengthRaw, n := binary.Uvarint(fileBytes[offset:])
	if n <= 0 {
		return topicRecord{}, fmt.Errorf("error reading topic name length: %d", n)
	}
	offset += n

	//nolint:gosec // we assume the client is well-behaved and won't send a huge topic name length.
	nameLength := max(int(nameLengthRaw)-1, 0)
	topicName := string(fileBytes[offset : offset+nameLength])
	offset += nameLength

	topicUUID := hex.EncodeToString(fileBytes[offset : offset+16])
	return topicRecord{
		topicName: topicName,
		topicUUID: topicUUID,
	}, nil
}

func parsePartitionRecord(fileBytes []byte) (partitionRecord, error) {
	// skip: version (1 byte)
	offset := 1

	partitionID := int(binary.BigEndian.Uint32(fileBytes[offset : offset+4]))
	offset += 4

	topicUUID := hex.EncodeToString(fileBytes[offset : offset+16])
	offset += 16

	lengthOfReplicasArrayRaw, n := binary.Uvarint(fileBytes[offset:])
	if n <= 0 {
		return partitionRecord{}, fmt.Errorf("error reading replicas array length: %d", n)
	}
	offset += n

	//nolint:gosec // we assume the client is well-behaved and won't send a huge replicas array length.
	lengthOfReplicasArray := max(int(lengthOfReplicasArrayRaw)-1, 0)
	replicas := make([]int, lengthOfReplicasArray)
	for i := 0; i < lengthOfReplicasArray; i++ {
		replicas[i] = int(binary.BigEndian.Uint32(fileBytes[offset : offset+4]))
		offset += 4
	}

	lengthOfISRsArrayRaw, n := binary.Uvarint(fileBytes[offset:])
	if n <= 0 {
		return partitionRecord{}, fmt.Errorf("error reading ISRs array length: %d", n)
	}
	offset += n

	//nolint:gosec // we assume the client is well-behaved and won't send a huge ISRs array length.
	lengthOfISRsArray := max(int(lengthOfISRsArrayRaw)-1, 0)
	isrs := make([]int, lengthOfISRsArray)
	for i := 0; i < lengthOfISRsArray; i++ {
		isrs[i] = int(binary.BigEndian.Uint32(fileBytes[offset : offset+4]))
		offset += 4
	}

	// skip:
	// length of removing replicas array (varint, assume 0), removing replicas array (assume 0 replicas),
	// length of adding replicas array (varint, assume 0), adding replicas array (assume 0 replicas)
	offset += 2

	leader := int(binary.BigEndian.Uint32(fileBytes[offset : offset+4]))
	offset += 4

	leaderEpoch := int(binary.BigEndian.Uint32(fileBytes[offset : offset+4]))

	return partitionRecord{
		partitionID: partitionID,
		topicUUID:   topicUUID,
		replicas:    replicas,
		ISRs:        isrs,
		leader:      leader,
		leaderEpoch: leaderEpoch,
	}, nil
}
