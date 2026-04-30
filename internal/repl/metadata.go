package repl

import (
	"encoding/binary"
	"fmt"
	"os"
)

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
	recordCount := int(binary.BigEndian.Uint32(fileBytes[offset : offset+4]))
	offset += 4

	for i := 0; i < recordCount; i++ {
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

	topicName, n, err := readCompactString(fileBytes[offset:])
	if err != nil {
		return topicRecord{}, err
	}
	offset += n

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

	lengthOfReplicasArray, n, err := readCompactLength(fileBytes[offset:])
	if err != nil {
		return partitionRecord{}, err
	}
	offset += n

	replicas := make([]int, lengthOfReplicasArray)
	for i := 0; i < lengthOfReplicasArray; i++ {
		replicas[i] = int(binary.BigEndian.Uint32(fileBytes[offset : offset+4]))
		offset += 4
	}

	lengthOfISRsArray, n, err := readCompactLength(fileBytes[offset:])
	if err != nil {
		return partitionRecord{}, err
	}
	offset += n

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
