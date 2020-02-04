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

import (
	"time"
)

func Monitor(time time.Time, db int64, addr string, cmd []string) {
	fmt.Printf("time: %s database: %d address: %s command %v\n", time, db, addr, cmd)
}

func Example_monitor() {
	// Register monitor callback.
	dialer := client.Dialer{MonitorCallback: Monitor}
	conn, err := dialer.Dial("")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Start monitor.
	ok, err := conn.Monitor().ToBool()
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		log.Fatal("Start monitor failed")
	}

	// Process some commands in same connection.
	mykey := client.RandomKey("mykey")
	for i := 0; i < 5; i++ {
		if err = conn.Set(mykey, "myValue").Err(); err != nil {
			log.Fatal(err)
		}
	}
}
