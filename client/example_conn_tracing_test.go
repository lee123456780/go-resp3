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
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/stfnmllr/go-resp3/client"
)

func trace(dir bool, b []byte) {
	if dir { // Example: print sent commands only.
		fmt.Println(strconv.Quote(string(b)))
	}
}

func ExampleConn_tracingFct() {
	// Create connetion providing trace callback.
	dialer := client.Dialer{TraceCallback: trace}
	conn, err := dialer.Dial("")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Set("mykey", "Hello Redis").Err(); err != nil {
		log.Fatal(err)
	}
	if err := conn.Get("mykey").Err(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"
	// "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$11\r\nHello Redis\r\n"
	// "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
	// "*1\r\n$4\r\nQUIT\r\n"
}

type tracer struct {
	mu     sync.RWMutex
	active bool
}

func (t *tracer) setActive(b bool) {
	t.mu.Lock()
	t.active = b
	t.mu.Unlock()
}

func (t *tracer) trace(dir bool, b []byte) {
	t.mu.RLock()
	if t.active && dir { // Example: print sent commands only if tracer is active.
		fmt.Println(strconv.Quote(string(b)))
	}
	t.mu.RUnlock()
}

func ExampleConn_tracingContext() {
	t := &tracer{active: true}

	// Create connetion providing trace callback.
	dialer := client.Dialer{TraceCallback: t.trace}
	conn, err := dialer.Dial("")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Set("mykey", "Hello Redis").Err(); err != nil {
		log.Fatal(err)
	}
	if err := conn.Get("mykey").Err(); err != nil {
		log.Fatal(err)
	}
	t.setActive(false)
	// Output:
	// "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"
	// "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$11\r\nHello Redis\r\n"
	// "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
}
