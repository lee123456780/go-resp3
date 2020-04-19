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

// A MapItem represents the redis map type key and value.
type MapItem struct {
	Key   RedisValue
	Value RedisValue
}

// A Map represents the redis map type.
type Map []MapItem

// Kind returns the type of a Map.
func (m Map) Kind() RedisKind { return RkMap }

// ToStringMap returns a map with keys of type string. In case key conversion to string is not possible
// a ConvertionError is returned.
func (m Map) ToStringMap() (map[string]interface{}, error) {
	r := make(map[string]interface{}, len(m))
	for _, item := range m {
		key, err := item.Key.ToString()
		if err != nil {
			return nil, err
		}
		switch value := item.Value.(type) {
		case baseRedisType:
			r[key] = value._interface()
		default:
			r[key] = value
		}
	}
	return r, nil
}

// ToStringValueMap returns a map with keys of type stringand values of type RedisValue.
// In case key conversion to string is not possible a ConvertionError is returned.
func (m Map) ToStringValueMap() (map[string]RedisValue, error) {
	r := make(map[string]RedisValue, len(m))
	for _, item := range m {
		key, err := item.Key.ToString()
		if err != nil {
			return nil, err
		}
		r[key] = item.Value
	}
	return r, nil
}

// ToStringStringMap returns a map with keys and values of type string. In case key or value conversion to string is not possible
// a ConvertionError is returned.
func (m Map) ToStringStringMap() (map[string]string, error) {
	r := make(map[string]string, len(m))
	for _, item := range m {
		key, err := item.Key.ToString()
		if err != nil {
			return nil, err
		}
		value, err := item.Value.ToString()
		if err != nil {
			return nil, err
		}
		r[key] = value
	}
	return r, nil
}

// ToStringInt64Map returns a map with keys of type string and values of type int64. In case key or value conversion is not possible
// a ConvertionError is returned.
func (m Map) ToStringInt64Map() (map[string]int64, error) {
	r := make(map[string]int64, len(m))
	for _, item := range m {
		key, err := item.Key.ToString()
		if err != nil {
			return nil, err
		}
		value, err := item.Value.ToInt64()
		if err != nil {
			return nil, err
		}
		r[key] = value
	}
	return r, nil
}

// XItem represents the id and the entry list of streams.
type XItem struct {
	ID    string
	Items []string
}

// ToXread returns a map[string] with values of type XItem. In case the conversion is not possible
// a ConversitionError is returned.
func (m Map) ToXread() (map[string][]XItem, error) {
	r := make(map[string][]XItem, len(m))
	for _, item := range m {
		s, err := item.Key.ToString()
		if err != nil {
			return nil, err
		}
		xrange, err := item.Value.ToXrange()
		if err != nil {
			return nil, err
		}
		r[s] = xrange
	}
	return r, nil
}
