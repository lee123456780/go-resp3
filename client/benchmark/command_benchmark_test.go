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
	"log"
	"os"
	"strconv"
	"strings"
	"testing"

	"go-resp3/client"
)

func lpadName(name string, cnt, n int) string {
	num := strconv.Itoa(cnt)
	return name + strings.Repeat("_", n-len(num)) + num
}

func benchmarkSet(conn client.Conn, cnt int, b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < cnt; j++ {
			conn.Set("mykey", "myvalue")
		}
	}
}

func benchmarkPipelineSet(conn client.Conn, cnt int, b *testing.B) {
	for i := 0; i < b.N; i++ {
		p := conn.Pipeline()
		for j := 0; j < cnt; j++ {
			p.Set("mykey", "myvalue")
		}
		if err := p.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCommand(b *testing.B) {

	dialer := client.Dialer{Logger: log.New(os.Stderr, "", log.LstdFlags)}
	conn, err := dialer.Dial("")
	if err != nil {
		b.Fatal(err)
	}

	b.Run("SingleCall", func(b *testing.B) {
		cnt := 1
		for i := 0; i < 6; i++ {
			b.Run(lpadName("Set", cnt, 6), func(b *testing.B) { benchmarkSet(conn, cnt, b) })
			cnt *= 10
		}
	})

	b.Run("Pipeline", func(b *testing.B) {
		cnt := 1
		for i := 0; i < 6; i++ {
			b.Run(lpadName("Set", cnt, 6), func(b *testing.B) { benchmarkPipelineSet(conn, cnt, b) })
			cnt *= 10
		}
	})

	if err := conn.Close(); err != nil {
		b.Fatal(err)
	}
}
