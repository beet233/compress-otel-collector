package compressotelreceiver

import (
	"encoding/binary"
	"errors"
	"math"
)

type DataReader struct {
	data []byte
}

func NewDataReader(data []byte) *DataReader {
	return &DataReader{data: data}
}

func (r *DataReader) readString(length int) (string, error) {
	if len(r.data) < length {
		return "", errors.New("no data available")
	}

	str := string(r.data[:length])
	r.data = r.data[length:]
	return str, nil
}

func (r *DataReader) readInt() (int, error) {
	if len(r.data) < 8 {
		return 0, errors.New("not enough data for int")
	}

	val := int(binary.LittleEndian.Uint64(r.data[:8]))

	r.data = r.data[8:]
	return val, nil
}

func (r *DataReader) readLeb128Int() (int, error) {
	var result int = 0
	shift := 0
	for i := 0; i < 8; i++ {
		b, err := r.readByte()
		if err != nil {
			return 0, errors.New("not enough data for leb128 int")
		}
		result |= (int(b&0x7F) << shift)
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	if shift == 56 {
		b, err := r.readByte()
		if err != nil {
			return 0, errors.New("not enough data for leb128 int")
		}
		// 最后一 byte 没有标记位
		result |= (int(b&0xFF) << shift)
	}
	return result, nil
}

func (r *DataReader) readFloat() (float64, error) {
	if len(r.data) < 8 {
		return 0, errors.New("not enough data for float")
	}

	val := math.Float64frombits(binary.LittleEndian.Uint64(r.data[:8]))
	r.data = r.data[8:]
	return val, nil
}

func (r *DataReader) readByte() (byte, error) {
	if len(r.data) == 0 {
		return 0, errors.New("no data available")
	}

	val := r.data[0]
	r.data = r.data[1:]
	return val, nil
}
