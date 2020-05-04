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

// A Slice represents the redis slice type.
type Slice []RedisValue

// Kind returns the type of a Slice.
func (s Slice) Kind() RedisKind { return _slice(s).Kind() }

// ToSlice returns a slice with values of type interface{}.
func (s Slice) ToSlice() ([]interface{}, error) { return _slice(s).ToSlice() }

// ToSlice2 returns a slice with values of type []interface{}. In case value conversion to []interface{} is not possible
// a ConversitionError is returned.
func (s Slice) ToSlice2() ([][]interface{}, error) { return _slice(s).ToSlice2() }

// ToSlice3 returns a slice with values of type [][]interface{}. In case value conversion to [][]interface{} is not possible
// a ConversitionError is returned.
func (s Slice) ToSlice3() ([][][]interface{}, error) { return _slice(s).ToSlice3() }

// ToTree returns a tree with nodes of type []interface{} and leaves of type interface{}. In case value conversion to []interface{} is not possible
// a ConversitionError is returned.
func (s Slice) ToTree() ([]interface{}, error) { return _slice(s).ToTree() }

// ToStringSlice returns a slice with values of type string. In case value conversion to string is not possible
// a ConversitionError is returned.
func (s Slice) ToStringSlice() ([]string, error) { return _slice(s).ToStringSlice() }

// ToStringMapSlice returns a slice with values of type map[string]interface{}. In case value conversion to map[string]interface{} is not possible
// a ConversitionError is returned.
func (s Slice) ToStringMapSlice() ([]map[string]interface{}, error) {
	return _slice(s).ToStringMapSlice()
}

// ToInt64Slice returns a slice with values of type int64. In case value conversion to string is not possible
// a ConversitionError is returned.
func (s Slice) ToInt64Slice() ([]int64, error) { return _slice(s).ToInt64Slice() }

// ToXrange returns a slice with values of type XItem. In case the conversion is not possible
// a ConversitionError is returned.
func (s Slice) ToXrange() ([]XItem, error) { return _slice(s).ToXrange() }
