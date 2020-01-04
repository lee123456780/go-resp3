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
	"sync"
)

const maxInt = int64(^uint(0) >> 1)

var bufferPool = sync.Pool{}

func getBuffer(size int64) []byte {
	if size > maxInt {
		panic("maximum integer size exceeded")
	}
	switch b := bufferPool.Get().(type) {
	default:
		return make([]byte, size)
	case []byte:
		if cap(b) < int(size) {
			return make([]byte, size)
		}
		return b[:size]
	}
}

func freeBuffer(b []byte) {
	bufferPool.Put(b)
}

func resizeBuffer(b []byte, size int64) []byte {
	if size > maxInt {
		panic("maximum integer size exceeded")
	}
	if cap(b) < int(size) {
		return make([]byte, size)
	}
	return b[:size]
}
