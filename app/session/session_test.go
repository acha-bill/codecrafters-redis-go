package session

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"testing"
)

func TestParseInputs(t *testing.T) {
	ts := []struct {
		in   string
		outs []resp.Value
	}{
		{
			in: "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
			outs: []resp.Value{
				{
					Type: resp.Array,
					Val: []resp.Value{
						{Type: resp.BulkString, Val: "PSYNC"},
						{Type: resp.BulkString, Val: "?"},
						{Type: resp.BulkString, Val: "-"},
					},
				},
			},
		},
		{
			in: "+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n",
			outs: []resp.Value{
				{
					Type: resp.SimpleString,
					Val:  "FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0",
				},
				{
					Type: resp.BulkString,
					Val:  "REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2",
				},
				{
					Type: resp.Array,
					Val: []resp.Value{
						{Type: resp.BulkString, Val: "REPLCONF"},
						{Type: resp.BulkString, Val: "GETACK"},
						{Type: resp.BulkString, Val: "*"},
					},
				},
			},
		},
	}

	for _, tt := range ts {
		_, vals := parseInputs([]byte(tt.in))
		fmt.Println(vals)
	}
}
