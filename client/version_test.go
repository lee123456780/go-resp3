/*
Copyright 2020 Stefan Miller

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
	"testing"
)

func testParseVersion(t *testing.T) {
	var tests = []struct {
		s string
		v Version
	}{
		{"1.9.0", Version{Major: 1, Minor: 9, Patch: 0}},
		{"1.10.0", Version{Major: 1, Minor: 10, Patch: 0}},
		{"1.11.1", Version{Major: 1, Minor: 11, Patch: 1}},

		{"1.0.0-alpha", Version{Major: 1, Minor: 0, Patch: 0, Prerelease: "alpha"}},
		{"1.0.0-alpha.1", Version{Major: 1, Minor: 0, Patch: 0, Prerelease: "alpha"}},
		{"1.0.0-0.3.7", Version{Major: 1, Minor: 0, Patch: 0, Prerelease: "alpha"}},
		{"1.0.0-x.7.z.92", Version{Major: 1, Minor: 0, Patch: 0, Prerelease: "alpha"}},
		{"1.0.0-x-y-z.-", Version{Major: 1, Minor: 0, Patch: 0, Prerelease: "alpha"}},

		{"1.0.0-alpha+001", Version{Major: 1, Minor: 0, Patch: 0, Prerelease: "alpha", Buildmetadata: "001"}},
		{"1.0.0+20130313144700", Version{Major: 1, Minor: 0, Patch: 0, Buildmetadata: "20130313144700"}},
		{"1.0.0-beta+exp.sha.5114f85", Version{Major: 1, Minor: 0, Patch: 0, Prerelease: "beta", Buildmetadata: "exp.sha.5114f85"}},
		{"1.0.0+21AF26D3-117B344092BD", Version{Major: 1, Minor: 0, Patch: 0, Buildmetadata: "21AF26D3-117B344092BD"}},
	}

	for i, test := range tests {
		v := ParseVersion(test.s)
		if v.String() != test.s {
			t.Fatalf("line: %d got: %s expected: %s", i, v, test.s)
		}
	}
}

func testCompareVersion(t *testing.T) {
	var tests = []struct {
		s1, s2 string
	}{
		{"1.0.0", "2.0.0"},
		{"2.0.0", "2.1.0"},
		{"2.1.0", "2.1.1"},

		{"1.0.0-alpha", "1.0.0"},

		{"1.0.0-alpha", "1.0.0-alpha.1"},
		{"1.0.0-alpha.1", "1.0.0-alpha.beta"},
		{"1.0.0-alpha.beta", "1.0.0-beta"},
		{"1.0.0-beta", "1.0.0-beta.2"},
		{"1.0.0-beta.2", "1.0.0-beta.11"},
		{"1.0.0-beta.11", "1.0.0-rc.1"},
		{"1.0.0-rc.1", "1.0.0"},
	}

	for i, test := range tests {
		v1 := ParseVersion(test.s1)
		v2 := ParseVersion(test.s2)
		if v1.Compare(v2) != -1 {
			t.Fatalf("line: %d expected: %s < %s", i, v1, v2)
		}
	}
}

func TestVersionl(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"parse", testParseVersion},
		{"compare", testCompareVersion},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(t)
		})
	}
}
