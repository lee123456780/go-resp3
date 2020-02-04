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
	. "testing"
)

func TestParseInt(t *T) {
	var parseTest = []struct {
		s string
		n int64
	}{
		{"", 0}, {"1", 1}, {"+1", 1}, {"-1", -1}, {"99999", 99999},
	}

	for i, test := range parseTest {
		n, err := ParseInt([]byte(test.s))
		if err != nil {
			t.Fatal(err)
		}
		if n != test.n {
			t.Fatalf("%d: %d - %d", i, n, test.n)
		}
	}
}
