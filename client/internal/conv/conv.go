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

package conv

import (
	"errors"
)

var ErrInvalidCharacter = errors.New("invalid character")

func ParseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, nil
	}
	switch b[0] {
	case '+':
		n, err := ParseUint(b[1:])
		return int64(n), err
	case '-':
		n, err := ParseUint(b[1:])
		return int64(n) * -1, err
	default:
		n, err := ParseUint(b)
		return int64(n), err
	}
}

func ParseUint(b []byte) (uint64, error) {
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, ErrInvalidCharacter
		}
		d := c - '0'
		n = n*10 + uint64(d)
	}
	return n, nil
}
