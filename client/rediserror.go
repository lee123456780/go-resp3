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
	"strings"
)

// A RedisError represents the redis error message if a redis command was executed unsuccessfully.
type RedisError struct {
	Code string
	Msg  string
}

func newRedisError(s string) *RedisError {
	p := strings.Split(s, " ")
	return &RedisError{
		Code: p[0],
		Msg:  strings.Join(p[1:], " "),
	}
}

func (e *RedisError) Error() string { return e.Code + " " + e.Msg }
