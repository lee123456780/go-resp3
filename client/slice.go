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

// ToSlice returns a slice with values of type interface{}.
func (s Slice) ToSlice() ([]interface{}, error) {
	r := make([]interface{}, len(s))
	for i, item := range s {
		r[i] = item.Value
	}
	return r, nil
}

// ToSlice2 returns a slice with values of type []interface{}. In case value conversion to []interface{} is not possible
// a ConversitionError is returned.
func (s Slice) ToSlice2() ([][]interface{}, error) {
	r := make([][]interface{}, len(s))
	for i, item := range s {
		l, err := item.ToSlice()
		if err != nil {
			return nil, err
		}
		r[i] = l
	}
	return r, nil
}

// ToTree returns a tree with nodes of type []interface{} and leaves of type interface{}. In case value conversion to []interface{} is not possible
// a ConversitionError is returned.
func (s Slice) ToTree() ([]interface{}, error) {
	r := make([]interface{}, len(s))
	for i, item := range s {
		switch item.Kind {
		case RkMap, RkSet:
			return nil, newConversionError("tree", item)
		case RkSlice:
			var err error
			r[i], err = item.Value.(Slice).ToTree()
			if err != nil {
				return nil, err
			}
		default:
			r[i] = item.Value
		}
	}
	return r, nil
}

// ToStringSlice returns a slice with values of type string. In case value conversion to string is not possible
// a ConversitionError is returned.
func (s Slice) ToStringSlice() ([]string, error) {
	r := make([]string, len(s))
	for i, item := range s {
		val, err := item.ToString()
		if err != nil {
			return nil, err
		}
		r[i] = val
	}
	return r, nil
}

// ToStringMapSlice returns a slice with values of type map[string]interfcae{}. In case value conversion to map[string]interface{} is not possible
// a ConversitionError is returned.
func (s Slice) ToStringMapSlice() ([]map[string]interface{}, error) {
	r := make([]map[string]interface{}, len(s))
	for i, item := range s {
		val, err := item.ToStringMap()
		if err != nil {
			return nil, err
		}
		r[i] = val
	}
	return r, nil
}

// ToInt64Slice returns a slice with values of type int64. In case value conversion to string is not possible
// a ConversitionError is returned.
func (s Slice) ToInt64Slice() ([]int64, error) {
	r := make([]int64, len(s))
	for i, item := range s {
		val, err := item.ToInt64()
		if err != nil {
			return nil, err
		}
		r[i] = val
	}
	return r, nil
}

// ToXrange returns a slice with values of type IdMap. In case the conversion is not possible
// a ConversitionError is returned.
func (s Slice) ToXrange() ([]IDMap, error) {
	r := make([]IDMap, len(s))
	for i, item := range s {
		slice, err := item.Slice()
		if err != nil {
			return nil, err
		}
		if len(slice) != 2 {
			return nil, newConversionError("Xrange", item)
		}
		s, err := slice[0].ToString()
		if err != nil {
			return nil, err
		}
		m, err := slice[1].ToStringStringMap()
		if err != nil {
			return nil, err
		}
		r[i] = IDMap{s, m}
	}
	return r, nil
}
