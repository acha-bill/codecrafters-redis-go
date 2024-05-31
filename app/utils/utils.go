package utils

import (
	"slices"
	"unicode"
)

func Catpialize(s string) string {
	if len(s) == 0 {
		return s
	}
	runes := []rune(s)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func TrimBytes(d []byte) []byte {
	i := slices.Index(d, 0)
	return d[:i]
}

func MapCopy[M1 ~map[K]V, K comparable, V any](src M1) M1 {
	dst := make(M1)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
