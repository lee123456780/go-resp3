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
	"testing"
	"time"
)

var parseTest = []struct {
	s string
	n *Notification
}{
	{`1339518083.107412 [0 127.0.0.1:60866] "keys" "*"`, &Notification{time.Unix(1339518083, 107412000), 0, "127.0.0.1:60866", []string{"keys", "*"}}},
	{`1339518087.877697 [5 127.0.0.1:60866] "dbsize"`, &Notification{time.Unix(1339518087, 877697000), 5, "127.0.0.1:60866", []string{"dbsize"}}},
	{`1339518090.420270 [15 127.0.0.1:60866] "set" "\"x" "6"`, &Notification{time.Unix(1339518090, 420270000), 15, "127.0.0.1:60866", []string{"set", "\"x", "6"}}},
}

func TestParser(t *testing.T) {
	for i, test := range parseTest {
		n, ok := Parse([]byte(test.s))
		if !ok {
			t.Fatalf("cmd %d: invalid notification %s", i, test.s)
		}

		if n.Time.UnixNano() != test.n.Time.UnixNano() {
			t.Fatalf("%d: time %d - expected %d", i, n.Time.UnixNano(), test.n.Time.UnixNano())
		}
		if n.Db != test.n.Db {
			t.Fatalf("%d: database %d - expected %d", i, n.Db, test.n.Db)
		}
		if n.Addr != test.n.Addr {
			t.Fatalf("%d: address %s - expected %s", i, n.Addr, test.n.Addr)
		}
		if len(n.Cmds) != len(test.n.Cmds) {
			t.Fatalf("%d: cmd length %d - expected %d", i, len(n.Cmds), len(test.n.Cmds))
		}
		for j, cmd := range n.Cmds {
			if cmd != test.n.Cmds[j] {
				t.Fatalf("cmd %d/%d: %s - expected %s", i, j, cmd, test.n.Cmds[j])
			}
		}
	}
}
