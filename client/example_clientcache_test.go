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

	"github.com/d024441/go-resp3/client"
)

func Example_clientcache() {
	mykey := client.RandomKey("mykey")

	// Create cache.
	cache := client.NewCache()

	// Create connetion providing cache.
	dialer := client.Dialer{Cache: cache}
	conn, err := dialer.Dial("")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Set client tracking on.
	if err := conn.ClientTracking(true, nil).Err(); err != nil {
		log.Fatal(err)
	}

	// Set key.
	conn.Set(mykey, "Hello Redis")

	// Get key.
	val, err := conn.Get(mykey).Value()
	if err != nil {
		log.Fatal(err)
	}

	// Save value in cache.
	cache.Put(mykey, val)

	// Read value from cache.
	val, ok := cache.Get(mykey)
	if !ok {
		log.Fatal("cache miss")
	}
	s, err := val.ToString()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(s)

	done := make(chan (struct{}), 0)

	// Change Key in different connection.
	go func() {
		conn, err := client.Dial("")
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		// Update key.
		if err = conn.Set(mykey, "Update mykey").Err(); err != nil {
			log.Fatal(err)
		}
		close(done)
	}()

	// Wait for go-routine.
	<-done

	// Cache miss: read until cache slot is invalidated.
	for {
		if _, ok := cache.Get(mykey); !ok {
			fmt.Println("Key invalidated")
			break
		}
	}

	// Output:
	// Hello Redis
	// Key invalidated
}
