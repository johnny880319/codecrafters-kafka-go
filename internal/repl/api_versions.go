package repl

func encodeAPIVersionsV4(header requestHeader, _ []byte) ([]byte, error) {
	var response []byte

	errorCode := 0
	if header.requestAPIVersion >= 5 {
		errorCode = 35 // 35 = UNSUPPORTED_VERSION
	}
	response = append(response, encodeInt(errorCode, 2)...)

	response = append(response, encodeInt(3, 1)...) // 2 api keys + 1

	response = append(response, encodeInt(18, 2)...) // API key 18 (APIVersions)
	response = append(response, encodeInt(0, 2)...)  // min version
	response = append(response, encodeInt(4, 2)...)  // max version
	response = append(response, encodeInt(0, 1)...)  // tag buffer

	response = append(response, encodeInt(75, 2)...) // API key 75 (DescribeTopicPartitions)
	response = append(response, encodeInt(0, 2)...)  // min version
	response = append(response, encodeInt(0, 2)...)  // max version
	response = append(response, encodeInt(0, 1)...)  // tag buffer

	response = append(response, encodeInt(0, 4)...) // throttle_time_ms
	response = append(response, encodeInt(0, 1)...) // tag buffer

	return response, nil
}
