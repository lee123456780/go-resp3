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

import (
	"strconv"
)

// A VerbatimString represents the redis verbatim string type.
type VerbatimString string

// FileFormat is returning the file format of a verbatim string.
func (s VerbatimString) FileFormat() string { return string(s[:3]) }

// String implements the Stringer interface.
func (s VerbatimString) String() string { return string(s[4:]) }

// Kind returns the type of a VerbatimString.
func (s VerbatimString) Kind() RedisKind { return RkVerbatimString }

// ToString implements the Converter Stringer interface.
func (s VerbatimString) ToString() (string, error) { return string(s[4:]), nil }

// ToInt64 implements the Converter ToInt64er interface.
func (s VerbatimString) ToInt64() (int64, error) { return strconv.ParseInt(string(s[4:]), 10, 64) }

// ToFloat64 implements the Converter ToFloat64er interface.
func (s VerbatimString) ToFloat64() (float64, error) { return strconv.ParseFloat(string(s[4:]), 64) }

// ToBool implements the Converter ToBooler interface.
func (s VerbatimString) ToBool() (bool, error) { return string(s[4:]) == ReplyOK, nil }
