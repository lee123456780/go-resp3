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

package crc64

import (
	"testing"
)

// see Redis http://download.redis.io/redis-stable/src/crc64.c

// Specification of this CRC64 variant follows:
//  * Name: crc-64-jones
//  * Width: 64 bites
//  * Poly: 0xad93d23594c935a9
//  * Reflected In: True
//  * Xor_In: 0xffffffffffffffff
//  * Reflected_Out: True
//  * Xor_Out: 0x0
//  * Check("123456789"): 0xe9c6d914c4b8d9ca

const (
	poly = 0xad93d23594c935a9
)

func TestCRC64(t *testing.T) {
	if crc := Checksum([]byte(check)); crc != checkSum {
		t.Fatalf("got %x - expected %x", ^crc, checkSum)
	}
}
