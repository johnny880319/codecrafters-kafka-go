package repl

import (
	"encoding/binary"
	"fmt"
)

func encodeInt(n int, length int) []byte {
	b := make([]byte, length)
	for i := length - 1; i >= 0; i-- {
		b[i] = byte(n & 0xFF)
		n >>= 8
	}
	return b
}

func readCompactString(b []byte) (string, int, error) {
	stringLength, n, err := readCompactLength(b)
	if err != nil {
		return "", 0, err
	}

	compactString := string(b[n : n+stringLength])
	return compactString, n + stringLength, nil
}

func readCompactLength(b []byte) (int, int, error) {
	compactLengthRaw, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, 0, fmt.Errorf("error reading compact length: %d", n)
	}

	//nolint:gosec // we assume the client is well-behaved and won't send a huge replicas array length.
	compactLength := max(int(compactLengthRaw)-1, 0)
	return compactLength, n, nil
}
