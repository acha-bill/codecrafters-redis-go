package pkg

import "slices"

func TrimBytes(d []byte) []byte {
	i := slices.Index(d, 0)
	return d[:i]
}
