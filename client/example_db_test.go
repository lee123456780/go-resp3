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
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/stfnmllr/go-resp3/client"
)

type server struct {
	db       client.DB
	shutdown chan struct{}
	svr      http.Server
}

func startServer(address string) (*server, error) {
	s := &server{
		db:       client.OpenDB("", nil),
		shutdown: make(chan struct{}),
	}
	s.svr.Addr = address

	pingHandler := func(w http.ResponseWriter, _ *http.Request) {
		pong, err := s.db.Ping(nil).ToString()
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(pong))
	}

	// Register request handlers.
	http.HandleFunc("/Ping", pingHandler)

	go func() {
		if err := s.svr.ListenAndServe(); err != http.ErrServerClosed {
			// Error starting or closing listener:
			log.Fatalf("HTTP server ListenAndServe: %s", err)
		}
		// signal shutdown finalized.
		close(s.shutdown)
	}()
	// wait for server start.
	return s, s.isUpAndRunning(address)
}

func (s *server) stop() {
	if err := s.db.Close(); err != nil {
		log.Printf("DB Close: %v", err)
	}
	if err := s.svr.Shutdown(context.Background()); err != nil {
		log.Printf("HTTP server Shutdown: %v", err)
	}
	// wait for shutdown.
	<-s.shutdown
}

func (s *server) isUpAndRunning(address string) error {
	httpClient := &http.Client{}
	var lastErr error
	for i := 0; i < 3; i++ {
		if _, lastErr = httpClient.Get(fmt.Sprintf("http://%s/Ping", address)); lastErr == nil {
			return nil
		}
		// wait some time
		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

const httpServerAddress = "localhost:11111"

func Example_db() {
	// start HTTP server.
	httpServer, err := startServer(httpServerAddress)
	if err != nil {
		log.Fatal(err)
	}
	defer httpServer.stop()

	httpClient := &http.Client{}

	var wg sync.WaitGroup

	const numRequests = 5

	// start numRequests requests concurrently.
	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {

		go func() {
			defer wg.Done()

			resp, err := httpClient.Get(fmt.Sprintf("http://%s/Ping", httpServerAddress))
			if err != nil {
				log.Fatal(err)
			}
			if code := resp.StatusCode; code != 200 {
				log.Fatalf("http status code %d", code)
			}
			reply, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Print(string(reply))
		}()
	}
	wg.Wait()

	// Output:
	// PONGPONGPONGPONGPONG
}
