/*
Copyright 2019 Stefan Miller

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client_test

import (
	"bufio"
	"bytes"
	"math"
	"math/big"
	"reflect"
	"testing"

	"go-resp3/client"
)

func TestProtocol(t *testing.T) {
	var writeTest = []struct {
		cmd []interface{}
		enc []byte
	}{
		{[]interface{}{1, 2, 3}, []byte("*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n")},
	}

	b := new(bytes.Buffer)
	enc := client.NewEncoder(b)

	for i, test := range writeTest {
		if err := enc.Encode(test.cmd...); err != nil {
			t.Fatalf("line: %d err: %v", i, err)
		}
		if err := enc.Flush(); err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(b.Bytes(), test.enc) != 0 {
			t.Fatalf("line: %d got: %v expected: %v", i, b.Bytes(), test.enc)
		}
		b.Reset()
	}
}

func TestDecode(t *testing.T) {
	const bigStr = "3492890328409238509324850943850943825024385"

	bigInt, ok := new(big.Int).SetString(bigStr, 10)
	if !ok {
		t.Fatal("cannot set bigInt")
	}

	var decodeTest = []struct {
		enc []byte
		dec client.RedisValue
	}{
		{[]byte("$0\r\n\r\n"), client.RedisValue{client.RkString, "", nil}},                                                               // zero length bulk string
		{[]byte("+Hello World\r\n"), client.RedisValue{client.RkString, "Hello World", nil}},                                              // simple string
		{[]byte("$11\r\nHello World\r\n"), client.RedisValue{client.RkString, "Hello World", nil}},                                        // bulk string
		{[]byte(":1234\r\n"), client.RedisValue{client.RkNumber, int64(1234), nil}},                                                       // number
		{[]byte("_\r\n"), client.RedisValue{client.RkNull, nil, nil}},                                                                     // null
		{[]byte(",1.23\r\n"), client.RedisValue{client.RkDouble, float64(1.23), nil}},                                                     // double
		{[]byte(",10\r\n"), client.RedisValue{client.RkDouble, float64(10), nil}},                                                         // double as integer
		{[]byte(",inf\r\n"), client.RedisValue{client.RkDouble, math.Inf(0), nil}},                                                        // double infinite
		{[]byte(",-inf\r\n"), client.RedisValue{client.RkDouble, math.Inf(-1), nil}},                                                      // double neg infinite
		{[]byte("#t\r\n"), client.RedisValue{client.RkBoolean, true, nil}},                                                                // boolean true
		{[]byte("#f\r\n"), client.RedisValue{client.RkBoolean, false, nil}},                                                               // boolean false
		{[]byte("=15\r\ntxt:Some string\r\n"), client.RedisValue{client.RkVerbatimString, client.VerbatimString("txt:Some string"), nil}}, // verbatim string
		{[]byte("(" + bigStr + "\r\n"), client.RedisValue{client.RkBigNumber, bigInt, nil}},                                               // big number

		{[]byte("-ERR this is the error description\r\n"), client.RedisValue{client.RkError, &client.RedisError{"ERR", "this is the error description"}, nil}}, // simple error
		{[]byte("!21\r\nSYNTAX invalid syntax\r\n"), client.RedisValue{client.RkError, &client.RedisError{"SYNTAX", "invalid syntax"}, nil}},                   // blob error

		{[]byte("$?\r\n;4\r\nHell\r\n;6\r\no worl\r\n;1\r\nd\r\n;0\r\n"), client.RedisValue{client.RkString, "Hello world", nil}}, // streamed string
		{ // slice
			[]byte("*4\r\n+first\r\n:1\r\n+second\r\n:2\r\n"),
			client.RedisValue{
				client.RkSlice,
				client.Slice{{client.RkString, "first", nil}, {client.RkNumber, int64(1), nil}, {client.RkString, "second", nil}, {client.RkNumber, int64(2), nil}},
				nil,
			},
		},
		{ // streamed slice
			[]byte("*?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n"),
			client.RedisValue{
				client.RkSlice,
				client.Slice{{client.RkString, "a", nil}, {client.RkNumber, int64(1), nil}, {client.RkString, "b", nil}, {client.RkNumber, int64(2), nil}},
				nil,
			},
		},
		{ // map
			[]byte("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n"),
			client.RedisValue{
				client.RkMap,
				client.Map{
					{client.RedisValue{client.RkString, "first", nil}, client.RedisValue{client.RkNumber, int64(1), nil}},
					{client.RedisValue{client.RkString, "second", nil}, client.RedisValue{client.RkNumber, int64(2), nil}},
				},
				nil,
			},
		},
		{ // streamed map
			[]byte("%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n"),
			client.RedisValue{
				client.RkMap,
				client.Map{
					{client.RedisValue{client.RkString, "a", nil}, client.RedisValue{client.RkNumber, int64(1), nil}},
					{client.RedisValue{client.RkString, "b", nil}, client.RedisValue{client.RkNumber, int64(2), nil}},
				},
				nil,
			},
		},
		{ // set
			[]byte("~2\r\n+first\r\n+second\r\n"),
			client.RedisValue{
				client.RkSet,
				client.Set{{client.RkString, "first", nil}, {client.RkString, "second", nil}},
				nil,
			},
		},
		{ // streamed set
			[]byte("~?\r\n+a\r\n+b\r\n.\r\n"),
			client.RedisValue{
				client.RkSet,
				client.Set{{client.RkString, "a", nil}, {client.RkString, "b", nil}},
				nil,
			},
		},
		{ // attributed slice
			[]byte("|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n*2\r\n:2039123\r\n:9543892\r\n"),
			client.RedisValue{
				client.RkSlice,
				client.Slice{{client.RkNumber, int64(2039123), nil}, {client.RkNumber, int64(9543892), nil}},
				client.Map{
					{
						client.RedisValue{client.RkString, "key-popularity", nil},
						client.RedisValue{
							client.RkMap,
							client.Map{
								{client.RedisValue{client.RkString, "a", nil}, client.RedisValue{client.RkDouble, float64(0.1923), nil}},
								{client.RedisValue{client.RkString, "b", nil}, client.RedisValue{client.RkDouble, float64(0.0012), nil}},
							},
							nil,
						},
					},
				},
			},
		},
		{ // attributed slice element
			[]byte("*3\r\n:1\r\n:2\r\n|1\r\n+ttl\r\n:3600\r\n:3\r\n"),
			client.RedisValue{
				client.RkSlice,
				client.Slice{
					{client.RkNumber, int64(1), nil},
					{client.RkNumber, int64(2), nil},
					{
						client.RkNumber,
						int64(3),
						client.Map{{client.RedisValue{client.RkString, "ttl", nil}, client.RedisValue{client.RkNumber, int64(3600), nil}}},
					},
				},
				nil,
			},
		},
	}

	b := new(bytes.Buffer)
	dec := client.NewDecoder(bufio.NewReader(b))

	for i, test := range decodeTest {
		b.Reset()
		if _, err := b.Write(test.enc); err != nil {
			t.Fatalf("line: %d err: %v", i, err)
		}

		dec, err := dec.Decode()
		if err != nil {
			t.Fatalf("line: %d err: %v", i, err)
		}

		if !reflect.DeepEqual(dec, test.dec) {
			t.Fatalf("line: %d value got %v - expected %v", i, dec, test.dec)
		}
	}
}
