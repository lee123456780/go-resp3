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

//go:generate commander

type command struct {
	send sendFct
}

func newCommand(send sendFct, sendInterceptor SendInterceptor) *command {
	if sendInterceptor == nil {
		return &command{send: send}
	}
	return &command{send: func(name string, r *result) {
		sendInterceptor(name, r.cmd())
		send(name, r)
	}}
}

// check interface implementations.
var _ Commands = (*command)(nil)
