package resp

import "testing"

func TestDecodeSimple(t *testing.T) {
	ts := []struct {
		in  string
		out string
		n   int
	}{
		{
			in:  "+PING\r\n",
			out: "PING",
			n:   7,
		},
		{
			in:  "+TEST abc 123\r\nrubbish_dont_care",
			out: "TEST abc 123",
			n:   15,
		},
	}

	for _, tt := range ts {
		var v Value
		n, err := decodeSimple([]byte(tt.in), &v)
		if err != nil {
			t.Fatal(err)
		}
		if n != tt.n {
			t.Fatalf("want %d, got %d", tt.n, n)
		}
		if v.Val.(string) != tt.out {
			t.Fatalf("want %s, got %s", tt.out, v.Val.(string))
		}
	}
}
