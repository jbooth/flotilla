package flotilla

import (
	"encoding/binary"
	"io"
)

type ByteArgs struct {
	B [][]byte
}

func (b *ByteArgs) Marshal() ([]byte, error) {
	numArrs := len(b.B)
	offsets := make([]int, numArrs, numArrs)
	lengths := make([]int, numArrs, numArrs)
	headerLen := 4 + (4 * len(offsets)) + (4 * len(lengths))
	currOff := headerLen
	for i := 0; i < numArrs; i++ {
		offsets[i] = currOff
		lengths[i] = len(b.B[i])
		currOff += len(b.B[i])
	}
	buff := make([]byte, currOff, currOff)
	currOff = 0
	binary.LittleEndian.PutUint32(buff, uint32(numArrs))
	currOff += 4
	for i := 0; i < numArrs; i++ {
		binary.LittleEndian.PutUint32(buff[currOff:], uint32(offsets[i]))
		currOff += 4
		binary.LittleEndian.PutUint32(buff[currOff:], uint32(lengths[i]))
		currOff += 4
	}
	for i := 0; i < numArrs; i++ {
		copy(buff[currOff:], b.B[i])
		currOff += lengths[i]
	}
	return buff, nil
}

// returns references to original data, copy them elsewhere if you want data to be GC'd
func (b *ByteArgs) Unmarshal(data []byte) error {
	currOff := 0
	numArgs := int(binary.LittleEndian.Uint32(data))
	currOff += 4
	b.B = make([][]byte, numArgs, numArgs)
	for i := 0; i < numArgs; i++ {
		argOffset := binary.LittleEndian.Uint32(data[currOff:])
		currOff += 4
		argLength := binary.LittleEndian.Uint32(data[currOff:])
		currOff += 4
		b.B[i] = data[argOffset:(argOffset + argLength)]
	}
	return nil
}

func (b *ByteArgs) Write(w io.Writer) (n int, err error) {
	buff, err := b.Marshal()
	if err != nil {
		return -1, err
	}
	length := make([]byte, 4, 4)
	binary.LittleEndian.PutUint32(length, uint32(len(buff)))
	nWrit := 0
	for nWrit < 4 {
		w, err := w.Write(length[nWrit:])
		if w > 0 {
			nWrit += w
		}
		if err != nil {
			return nWrit, err
		}
	}
	nWrit = 0
	for nWrit < len(buff) {
		w, err := w.Write(buff[nWrit:])
		if w > 0 {
			nWrit += w
		}
		if err != nil {
			return nWrit + 4, err
		}
	}
	return nWrit + 4, nil
}

func (b *ByteArgs) Read(r io.Reader) (n int, err error) {
	length := make([]byte, 4, 4)
	nRead := 0
	for nRead < 4 {
		r, err := r.Read(length[nRead:])
		if r > 0 {
			nRead += r
		}
		if err != nil {
			return nRead, err
		}
	}
	buffLen := int(binary.LittleEndian.Uint32(length))
	buff := make([]byte, buffLen, buffLen)
	nRead = 0
	for nRead < buffLen {
		r, err := r.Read(buff[nRead:])
		if r > 0 {
			nRead += r
		}
		if err != nil {
			return nRead + 4, err
		}
	}
	err = b.Unmarshal(buff)
	return nRead + 4, err
}
