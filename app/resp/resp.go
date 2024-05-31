package resp

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type TYPE rune

const (
	SimpleString TYPE = '+'
	BulkString   TYPE = '$'
	Array        TYPE = '*'
	Int          TYPE = ':'
)
const MaxBulkLen = 536870912

var (
	crlf                 = []byte("\r\n")
	crlLen               = 2
	ErrUnsupportedType   = errors.New("unsupported type")
	ErrInvalidTerminator = errors.New("invalid terminator")
	ErrMaxBulkLen        = errors.New("max bulk length")

	Nil  = []byte("$-1\r\n")
	Ok   = []byte("+OK\r\n")
	Pong = []byte("+PONG\r\n")
)

type Value struct {
	Type TYPE
	Val  any
}

func DecodeCmd(in Value) (string, []Value, error) {
	if in.Type != Array {
		return "", nil, fmt.Errorf("only array allowed")
	}
	args := in.Val.([]Value)
	if len(args) == 0 {
		return "", nil, fmt.Errorf("command is missing")
	}
	if args[0].Type != BulkString {
		return "", nil, fmt.Errorf("bulk string expected")
	}
	return strings.ToUpper(args[0].Val.(string)), args, nil
}

func EncodeSimple(s string) []byte {
	return []byte(fmt.Sprintf("+%s%s", s, crlf))
}

func EncodeRDB() []byte {
	f, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	b := []byte(fmt.Sprintf("$%d", len(f)))
	b = append(b, crlf...)
	b = append(b, f...)
	//b = append(b, crlf...)
	return b
}

func EncodeError(err error) []byte {
	return []byte(fmt.Sprintf("-ERR %s%s", utils.Catpialize(err.Error()), crlf))
}

func Encode(v any) []byte {
	var res []byte

	switch v.(type) {
	case store.StreamEntry:
		res = encodeStreamEntry(v.(store.StreamEntry))
	}
	if res != nil {
		return res
	}

	t := reflect.TypeOf(v)
	switch t.Kind() {
	case reflect.Int:
		res = encodeInt(v)
	case reflect.String:
		res = encodeBulkString(v)
	case reflect.Slice, reflect.Array:
		res = encodeArray(v)
	case reflect.Map:
		res = encodeMap(v)
	default:
	}
	return res
}

func encodeInt(v any) []byte {
	return []byte(fmt.Sprintf(":%d%s", v, crlf))
}

func encodeBulkString(v any) []byte {
	s := v.(string)
	r := fmt.Sprintf("%s%d%s%s%s", string(BulkString), len(s), crlf, s, crlf)
	return []byte(r)
}

func encodeStreamEntry(e store.StreamEntry) []byte {
	id := encodeBulkString(e.ID)
	var values []string
	for k, v := range e.Values {
		values = append(values, k, v)
	}
	valuesEnc := encodeArray(values)
	res := encodeArray([]any{id, valuesEnc})
	return res
}

func encodeMap(v any) []byte {
	s := reflect.ValueOf(v)
	l := s.Len()

	arr := make([][]any, l)
	for i, k := range s.MapKeys() {
		v := s.MapIndex(k)
		arr[i] = []any{k.Interface(), v.Interface()}
	}
	return encodeArray(arr)
}

func encodeArray(v any) []byte {
	// don't encode raw bytes
	if _, ok := v.([]byte); ok {
		return v.([]byte)
	}

	s := reflect.ValueOf(v)
	l := s.Len()
	ret := make([][]byte, l)
	for i := 0; i < l; i++ {
		ret[i] = Encode(s.Index(i).Interface())
	}
	r := []byte(fmt.Sprintf("%s%d%s", string(Array), l, crlf))
	for i := range ret {
		r = append(r, ret[i]...)
	}
	return r
}

func Decode(d []byte, v *Value) (n int, err error) {
	if v == nil {
		return 0, nil
	}

	switch TYPE(d[0]) {
	case SimpleString:
		n, err = decodeSimple(d, v)
	case BulkString:
		n, err = decodeBulk(d, v)
	case Array:
		n, err = decodeArray(d, v)
	default:
		return 0, fmt.Errorf("invalid type. input=%q", string(d))
	}
	return
}

func decodeSimple(d []byte, v *Value) (int, error) {
	if len(d) < 3 {
		return 0, fmt.Errorf("invalid simple string")
	}

	var i int
	for i = 0; i < len(d); i++ {
		_, err := readNewLine(d[i+1 : i+3])
		if err == nil {
			break
		}
	}
	d = d[1 : i+1]
	v.Val = string(d)
	v.Type = SimpleString
	return i + 3, nil
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
	n, _ = readNewLine(d) // optional last terminator
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
	if !bytes.Equal(d[0:2], crlf) {
		return 0, fmt.Errorf("%v, %v, %w", d[0:2], crlf, ErrInvalidTerminator)
	}
	return 2, nil
}
