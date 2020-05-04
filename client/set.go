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

// A Set represents the redis set type.
type Set []RedisValue

// ToStringSet returns a map with keys of type string and boolean true values. In case key conversion to string is not possible
// a ConvertionError is returned.
func (s Set) ToStringSet() (map[string]bool, error) { return _set(s).ToStringSet() }
