package pkg

import "slices"

func TrimBytes(d []byte) {
	i := slices.Index(d, 0)
	d = d[:i]
}
