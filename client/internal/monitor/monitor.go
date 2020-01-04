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

package monitor

import (
	"strconv"
	"strings"
	"time"
)

// Monitor notification ic currently in Redis 6 beta no push type but standard string.

// Notification represents the type for an out of bound monitor push notification send by Redis.
type Notification struct {
	Time time.Time
	DB   int64
	Addr string
	Cmd  []string
}

// dirty hack - check if string is monitor push notification
const minNotificationLen = 34

// IsNotification checks if string is a monitor push notification.
func IsNotification(s string) bool {
	if len(s) < minNotificationLen {
		return false
	}
	_, s, ok := parseInt64(s, ".")
	if !ok {
		return false
	}
	_, s, ok = parseInt64(s, " [")
	if !ok {
		return false
	}
	_, s, ok = parseInt64(s, " ")
	if !ok {
		return false
	}
	_, s, ok = parseString(s, "] ")
	if !ok {
		return false
	}
	_, ok = parseCmd(s)
	if !ok {
		return false
	}
	return true
}

func parseInt64(s, sep string) (int64, string, bool) {
	pos := strings.Index(s, sep)
	if pos == -1 {
		return 0, "", false
	}
	i, err := strconv.ParseInt(s[:pos], 10, 64)
	if err != nil {
		return 0, "", false
	}
	return i, s[pos+len(sep):], true
}

func parseString(s, sep string) (string, string, bool) {
	pos := strings.Index(s, sep)
	if pos == -1 {
		return "", "", false
	}
	return s[:pos], s[pos+len(sep):], true
}

const (
	pow10_8 = 100000000
	pow10_9 = 1000000000
)

func nanoSeconds(fraction int64) int64 {
	if fraction < pow10_8 {
		for fraction < pow10_8 {
			fraction *= 10
		}
	} else {
		for fraction > pow10_9 {
			fraction /= 10
		}
	}
	return fraction
}

const (
	quote = '"'
	esc   = '\\'
)

func parseQuotedString(s string) (string, string, bool) {
	if s[0] != quote {
		return "", "", false
	}
	b := make([]byte, len(s)-2)
	j, escaped := 0, false
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case esc:
			escaped = true
		case quote:
			if !escaped {
				return s[i+1:], string(b[:j]), true
			}
			fallthrough
		default:
			b[j] = s[i]
			j++
			escaped = false
		}
	}
	return "", "", false
}

func parseCmd(s string) ([]string, bool) {
	r := make([]string, 0, 2)
	var cmd string
	var ok bool
	for {
		s, cmd, ok = parseQuotedString(s)
		if !ok {
			return nil, false
		}
		r = append(r, cmd)
		if s == "" {
			return r, true
		}
		s = s[1:] // skip blank between commands
	}
}

// Parse parses Redis monitor push notification string.
func Parse(s string) Notification {
	n := Notification{}

	seconds, s, ok := parseInt64(s, ".")
	if !ok {
		return n
	}
	fraction, s, ok := parseInt64(s, " [")
	if !ok {
		return n
	}
	n.Time = time.Unix(seconds, nanoSeconds(fraction))

	n.DB, s, ok = parseInt64(s, " ")
	if !ok {
		return n
	}

	n.Addr, s, ok = parseString(s, "] ")
	if !ok {
		return n
	}

	n.Cmd, ok = parseCmd(s)
	if !ok {
		return n
	}

	return n
}
