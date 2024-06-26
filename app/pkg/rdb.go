package pkg

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

var ErrInvalidHeader = errors.New("invalid header")
var ErrInvalidVersion = errors.New("invalid version")

type rdbValue struct {
	Kind  StringKind
	Value any
}
type RDBStoreValue struct {
	Val    any
	Expiry time.Time
}

type StringKind int

const (
	RegularString StringKind = iota
	Int8String
	Int16String
	Int32String
)

var data map[string]RDBStoreValue

func ReadRDB(path string) (map[string]RDBStoreValue, error) {
	data = make(map[string]RDBStoreValue)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	buf, _ := io.ReadAll(f)
	fmt.Println(hex.EncodeToString(buf))
	f.Seek(0, 0)

	r := bufio.NewReader(f)

	// REDIS
	buf = make([]byte, 5)
	n, err := r.Read(buf[:5])
	if n != 5 {
		return nil, ErrInvalidHeader
	}
	if err != nil {
		return nil, err
	}
	if string(buf[:n]) != "REDIS" {
		return nil, ErrInvalidHeader
	}

	// version
	n, err = r.Read(buf[:4])
	if n != 4 {
		return nil, ErrInvalidVersion
	}
	if err != nil {
		return nil, err
	}
	v, err := strconv.Atoi(string(buf[:n]))
	if err != nil {
		return nil, err
	}
	fmt.Println("version: ", v)

L:
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		switch b {
		case 0xfa:
			_, err = readFa(r)
			if err != nil {
				return nil, fmt.Errorf("read fa: %w", err)
			}
		case 0xfe:
			_, err = readFe(r)
			if err != nil {
				return nil, fmt.Errorf("read fe: %w", err)
			}
		case 0xfd:
			err = readFd(r)
			if err != nil {
				return nil, fmt.Errorf("read fd: %w", err)
			}
		case 0xfb:
			err = readFb(r)
			if err != nil {
				return nil, fmt.Errorf("read fb: %w", err)
			}
		case 0xfc:
			err = readFc(r)
			if err != nil {
				return nil, fmt.Errorf("read fc: %w", err)
			}
		case 0xff:
			fmt.Println("end reached")
			break L
		default:
			err = r.UnreadByte()
			if err != nil {
				return nil, fmt.Errorf("unread")
			}
			err = readData(r, time.Time{})
			if err != nil {
				return nil, fmt.Errorf("read data: %w", err)
			}
		}
	}

	return data, nil
}

func readData(r *bufio.Reader, expiry time.Time) error {
	_, err := r.ReadByte()
	if err != nil {
		return err
	}
	key, _, err := decode(r)
	var v rdbValue
	err = decodeValue(r, &v)
	if err != nil {
		return err
	}
	data[string(key)] = RDBStoreValue{
		Val:    v.Value,
		Expiry: expiry,
	}
	return nil
}

func readFe(r *bufio.Reader) ([]byte, error) {
	// db number
	_, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func readFb(r *bufio.Reader) error {
	//d, t, err := decode(r)
	//if err != nil {
	//	return err
	//}
	//fmt.Println("hash length", string(d), t)
	//
	//d, t, err = decode(r)
	//if err != nil {
	//	return err
	//}
	//fmt.Println("expiry length: ", string(d), t)
	//return nil
	r.ReadByte()
	r.ReadByte()
	return nil
}

func readFd(r *bufio.Reader) error {
	buf := make([]byte, 4)
	_, err := r.Read(buf)
	if err != nil {
		return err
	}
	ex := binary.LittleEndian.Uint32(buf)
	expiry := time.Unix(int64(ex), 0)
	fmt.Println("seconds expiry: ", expiry)
	err = readData(r, expiry)
	if err != nil {
		return err
	}
	return nil
}

func readFc(r *bufio.Reader) error {
	buf := make([]byte, 8)
	_, err := r.Read(buf)
	if err != nil {
		return err
	}
	ex := binary.LittleEndian.Uint64(buf)
	expiry := time.UnixMilli(int64(ex))
	fmt.Println("milliseconds expiry: ", expiry)
	err = readData(r, expiry)
	if err != nil {
		return err
	}
	return nil
}

func readFa(r *bufio.Reader) ([]byte, error) {
	faKey, t, err := decode(r)
	if err != nil {
		return nil, err
	}
	if t != RegularString {
		return nil, fmt.Errorf("expected regular string")
	}

	faVal, _, err := decode(r)
	if err != nil {
		return nil, err
	}
	fmt.Println("faKey= ", string(faKey), ", faVal=", string(faVal))
	return nil, nil
}

//func decodeInt32(r *bufio.Reader) (int32, error) {
//	b, _, err := decode(r)
//	if err != nil {
//		return 0, err
//	}
//	return int32(binary.LittleEndian.Uint32(b)), nil
//}
//
//func decodeString(r *bufio.Reader) (string, error) {
//	b, _, err := decode(r)
//	if err != nil {
//		return "", err
//	}
//	return string(b), nil
//}

func decodeValue(r *bufio.Reader, v *rdbValue) error {
	buf, t, err := decode(r)
	if err != nil {
		return err
	}

	v.Kind = t
	switch t {
	case RegularString:
		v.Value = string(buf)
	case Int8String:
		v.Value = int8(binary.LittleEndian.Uint16(buf))
	case Int16String:
		v.Value = int16(binary.LittleEndian.Uint16(buf))
	case Int32String:
		v.Value = int32(binary.LittleEndian.Uint32(buf))
	}

	return nil
}

func decode(r *bufio.Reader) ([]byte, StringKind, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	msb := (b & 0xc0) >> 6
	if msb == 0x00 {
		l := int(b & 0x3F)
		buf := make([]byte, l)
		_, err = r.Read(buf)
		if err != nil {
			return nil, 0, err
		}
		return buf, RegularString, nil
	}

	if msb == 0x01 {
		rem := b & 0x3F
		b, err = r.ReadByte()
		if err != nil {
			return nil, RegularString, err
		}

		l0 := int(rem) << 8
		l := l0 | int(b)
		buf := make([]byte, l)
		_, err = r.Read(buf)
		if err != nil {
			return nil, 0, err
		}
		return buf, RegularString, nil
	}

	if msb == 0x10 {
		b, err = r.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		l := int(b)
		buf := make([]byte, l)
		_, err = r.Read(buf)
		if err != nil {
			return nil, 0, err
		}
		return buf, RegularString, nil
	}

	t := int(b & 0x3F)
	switch t {
	case 0:
		// 8 bit int
		b, err := r.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		return []byte{b}, Int8String, nil
	case 1:
		// 16 bit int
		buf := make([]byte, 2)
		_, err := r.Read(buf)
		if err != nil {
			return nil, 0, err
		}
		return buf, Int16String, nil
	case 2:
		// 32 bit int
		buf := make([]byte, 4)
		_, err := r.Read(buf)
		if err != nil {
			return nil, 0, err
		}
		return buf, Int32String, nil
	case 3:
		// read compressed string
	}

	return nil, 0, fmt.Errorf("unkown format")
}
