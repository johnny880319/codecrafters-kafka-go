package repl

func encodeDescribeTopicPartitionsV0(_ requestHeader, body []byte) ([]byte, error) {
	// read request
	offset := 0

	topicArrayLength := max(int(body[offset])-1, 0)
	offset++

	topicNames := make([]string, topicArrayLength)
	for i := 0; i < topicArrayLength; i++ {
		topicNameLength := max(int(body[offset])-1, 0)
		offset++
		topicNames[i] = string(body[offset : offset+topicNameLength])
		offset += topicNameLength
	}

	// write response
	response := make([]byte, 0)
	response = append(response, encodeInt(0, 4)...)                 // throttle_time_ms
	response = append(response, encodeInt(len(topicNames)+1, 1)...) // topic array length

	for _, topicName := range topicNames {
		response = append(response, encodeInt(3, 2)...)                // error code UNKNOWN_TOPIC
		response = append(response, encodeInt(len(topicName)+1, 1)...) // topic name length
		response = append(response, []byte(topicName)...)              // topic name
		response = append(response, encodeInt(0, 16)...)               // topic ID (null UUID)
		response = append(response, encodeInt(0, 1)...)                // is internal = false
		response = append(response, encodeInt(1, 1)...)                // partition array length
		response = append(response, encodeInt(0, 4)...)                // topic authorized operations
		response = append(response, encodeInt(0, 1)...)                // tag buffer
	}

	response = append(response, encodeInt(255, 1)...) // next cursor = -1 (no more topics)
	response = append(response, encodeInt(0, 1)...)   // tag buffer

	return response, nil
}
