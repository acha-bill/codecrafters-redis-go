package resp

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

type TYPE rune

const (
	BulkString TYPE = '$'
	Array      TYPE = '*'
)
const MaxBulkLen = 536870912

var (
	crlf                 = []byte("\\r\\n")
	ErrUnsupportedType   = errors.New("unsupported type")
	ErrInvalidTerminator = errors.New("invalid terminator")
	ErrMaxBulkLen        = errors.New("max bulk length")
)

type Value struct {
	Type TYPE
	Val  any
}

func Encode(v any) []byte {
	switch v.(type) {
	case string:
		s := v.(string)
		r := fmt.Sprintf("%s%d%s%s%s", string(BulkString), len(s), crlf, s, crlf)
		return []byte(r)
	}
	return nil
}

func Decode(d []byte, v *Value) (n int, err error) {
	if v == nil {
		return 0, nil
	}

	switch TYPE(d[0]) {
	case BulkString:
		n, err = decodeBulk(d, v)
	case Array:
		n, err = decodeArray(d, v)
	default:
		err = ErrUnsupportedType
	}
	return
}

// decodeArray *<number-of-elements>\r\n<element-1>...<element-n>
func decodeArray(d []byte, v *Value) (int, error) {
	v.Type = Array
	d, n0 := d[1:], len(d)
	l, n, err := readLength(d)
	if err != nil {
		return 0, err
	}
	d = d[n:]
	n, err = readNewLine(d)
	if err != nil {
		return 0, err
	}
	d = d[n:]

	arr := make([]Value, l)
	for i := 0; i < l; i++ {
		var vi Value
		n, err = Decode(d, &vi)
		if err != nil {
			return 0, err
		}
		arr[i] = vi
		d = d[n:]
	}
	v.Val = arr
	return n0 - len(d), nil
}

// decodeBulk $<length>\r\n<data>\r\n
func decodeBulk(d []byte, v *Value) (int, error) {
	v.Type = BulkString
	d, n0 := d[1:], len(d)
	l, n, err := readLength(d)
	if err != nil {
		return 0, err
	}
	d = d[n:]
	n, err = readNewLine(d)
	if err != nil {
		return 0, err
	}
	d = d[n:]
	n, s, err := readString(d, l)
	if err != nil {
		return 0, err
	}
	d = d[n:]
	n, err = readNewLine(d)
	if err != nil {
		return 0, err
	}
	d = d[n:]
	v.Val = s
	return n0 - len(d), nil
}

func readString(d []byte, l int) (int, string, error) {
	if len(d) < l {
		return 0, "", errors.New("not enough data to read")
	}
	if l > MaxBulkLen {
		return 0, "", ErrMaxBulkLen
	}
	buf := make([]byte, l)
	n := copy(buf, d)
	if n != l {
		return 0, "", fmt.Errorf("len mismatch")
	}
	return n, string(buf), nil
}

func readLength(d []byte) (int, int, error) {
	var buf []byte
	for i := 0; i < len(d); i++ {
		if d[i] >= '0' && d[i] <= '9' {
			buf = append(buf, d[i])
		} else {
			break
		}
	}
	l, err := strconv.Atoi(string(buf))
	if err != nil {
		return 0, 0, err
	}
	return l, len(buf), nil
}

func readNewLine(d []byte) (int, error) {
	if bytes.Compare(d[0:4], crlf) != 0 {
		return 0, ErrInvalidTerminator
	}
	return 4, nil
}
