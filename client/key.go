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
	"crypto/rand"
	"encoding/base64"
	"io"

	"go-resp3/client/internal/crc64"
)

// RandomKey returns key + base64 encoded random bytes. Used for tests to avoid key overwrites on Redis server.
func RandomKey(key string) string {
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err.Error()) // rand should never fail
	}
	return key + base64.URLEncoding.EncodeToString(b)
}

const slotMask uint64 = 0xffffff // less significant 24 bits

// Key provides methods to calculate Redis crc64 hash and client caching slot.
type Key string

// CRC64 returns the Redis crc64 hash value of key.
func (k Key) CRC64() uint64 {
	return crc64.Checksum([]byte(k))
}

// Slot returns the Redis client caching slot for key.
func (k Key) Slot() uint32 {
	return uint32(k.CRC64() & slotMask)
}
