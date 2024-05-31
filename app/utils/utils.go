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
