package repl

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
)

func encodeDescribeTopicPartitionsV0(_ requestHeader, body []byte) ([]byte, error) {
	// read metadata
	topicRecords, partitionRecords, err := parseMetadata()
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "topicRecords: %+v\n", topicRecords)
	fmt.Fprintf(os.Stderr, "partitionRecords: %+v\n", partitionRecords)

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
	for i := 0; i < topicArrayLength; i++ {
		topicNameLengthRaw, n := binary.Uvarint(body[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("error reading topic name length: %d", n)
		}
		offset += n

		//nolint:gosec // we assume the client is well-behaved and won't send a huge topic name length.
		topicNameLength := max(int(topicNameLengthRaw)-1, 0)
		topicNames[i] = string(body[offset : offset+topicNameLength])
		offset += topicNameLength
		_, n = binary.Uvarint(body[offset:]) // skip tag buffer
		if n <= 0 {
			return nil, fmt.Errorf("error reading tag buffer length: %d", n)
		}
		offset += n
	}
	sort.Strings(topicNames)

	fmt.Fprintf(os.Stderr, "topicNames: %+v\n", topicNames)

	// write response
	response := make([]byte, 0)
	response = append(response, encodeInt(0, 4)...)                // throttle_time_ms
	response = binary.AppendUvarint(response, topicArrayLengthRaw) // topic array length

	for _, topicName := range topicNames {
		topicRecord, ok := topicRecords[topicName]

		if ok {
			response = append(response, encodeInt(0, 2)...) // error code NONE
		} else {
			response = append(response, encodeInt(3, 2)...) // error code UNKNOWN_TOPIC
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

type topicRecord struct {
	topicName string
	topicUUID [16]byte
}

type partitionRecord struct {
	partitionID int
	topicUUID   [16]byte
	replicas    []int
	ISRs        []int
	leader      int
	leaderEpoch int
}

func parseMetadata() (map[string]topicRecord, map[[16]byte][]partitionRecord, error) {
	filePath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}
	fmt.Fprint(os.Stderr, hex.Dump(fileBytes)) // for debugging

	topicRecords := make(map[string]topicRecord)
	partitionRecords := make(map[[16]byte][]partitionRecord)

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
	partitionRecords map[[16]byte][]partitionRecord,
) (map[string]topicRecord, map[[16]byte][]partitionRecord, error) {
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
	partitionRecords map[[16]byte][]partitionRecord,
) (map[string]topicRecord, map[[16]byte][]partitionRecord, error) {
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
		topicRecords[tr.topicName] = tr
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

	var topicUUID [16]byte
	copy(topicUUID[:], fileBytes[offset:offset+16])
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

	var topicUUID [16]byte
	copy(topicUUID[:], fileBytes[offset:offset+16])
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

func encodePartitionRecord(partitionRecords map[[16]byte][]partitionRecord, topicUUID [16]byte) []byte {
	records, ok := partitionRecords[topicUUID]
	if !ok {
		var response []byte
		return binary.AppendUvarint(response, 1)
	}

	var response []byte
	response = binary.AppendUvarint(response, uint64(len(records)+1))
	for _, record := range records {
		response = append(response, encodeInt(0, 2)...) // error code NONE
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
