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

package client

import (
	"net"
	"os"
)

//go:generate converter

// Client related attributes like name and version.
const (
	ClientName    = "go-resp3"
	ClientVersion = "0.11.3"
)

// Environment variables.
const (
	EnvHost = "REDIS_HOST"
	EnvPort = "REDIS_PORT"
)

// Default redis host and port.
const (
	LocalHost        = "127.0.0.1"
	DefaultRedisPort = "6379"
)

const (
	tcpNetwork = "tcp"
)

func hostPort(address string) string {
	if address != "" { // use provided address
		return address
	}
	host, ok := os.LookupEnv(EnvHost)
	if !ok {
		host = LocalHost
	}
	port, ok := os.LookupEnv(EnvPort)
	if !ok {
		port = DefaultRedisPort
	}
	return net.JoinHostPort(host, port)
}
